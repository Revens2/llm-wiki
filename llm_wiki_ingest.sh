#!/usr/bin/env bash
# Ingestion LLM Wiki — ChatGPT-seul (bascule 2026-09-09, contrat wiki-extract-v4).
# AUCUN appel LLM local : ni Gemini, ni AGY. Le raisonnement est fait par ChatGPT
# via la file MCP (wiki_ingest_claim/read/submit, autorite serveur vault-mcp).
# Ce script ne garde que le DETERMINISTE : eligibilite, statuts, retry manuel,
# fusion du spool (passe 2, zero LLM), demande de publication.
# L'ancien chemin AGY (cmd_run) et l'extraction locale (cmd_extract) REFUSENT.
set -uo pipefail

# ---------------------------------------------------------------- configuration
# WIKI_DIR est parametrable : le vault reel est /srv/obsidian-vault (1946 fiches
# sources, index.md cure de 121 Ko). /srv/llm-wiki est un doublon reduit, conserve
# mais plus alimente.
WIKI_DIR="${WIKI_DIR:-/srv/obsidian-vault}"
# RAW_DIR n est PLUS derive ici : il l est apres le chargement de
# /etc/default/llm-wiki (voir plus bas). Derive a cet endroit, il etait fige
# avant que le fichier de reglages ne soit lu, donc impossible a surcharger --
# c est la cause de la rupture du 2026-08-22 : la source lue ne pouvait pas
# etre repointee sur le miroir sans toucher WIKI_DIR, qui est aussi la cible
# des ECRITURES.
WIKI_SUB="${WIKI_DIR}/wiki"
SOURCES_DIR="${WIKI_SUB}/sources"
ENTITIES_DIR="${WIKI_SUB}/entities"
CONCEPTS_DIR="${WIKI_SUB}/concepts"
INDEX_FILE="${WIKI_DIR}/index.md"
LOG_FILE="${WIKI_DIR}/log.md"
MANIFEST="${WIKI_DIR}/.ingested_manifest.jsonl"
MANIFEST_V1="${WIKI_DIR}/.ingested_manifest"
STAGING_ROOT="${WIKI_DIR}/.staging"
STATE_DIR="/var/lib/llm-wiki"
EXTRACT_SPOOL="${EXTRACT_SPOOL:-${STATE_DIR}/spool/extract}"
PUBLISH_REQUEST="${PUBLISH_REQUEST:-${STATE_DIR}/publish.request}"
SPOOL_DIR="${STATE_DIR}/notify-spool"
INGEST_DUE="${STATE_DIR}/ingest-due-at"
LINT_DUE="${STATE_DIR}/lint-due-at"
RESUME_COUNT="${STATE_DIR}/resume-count"
# Bascule ChatGPT-seul : binaire externe et modeles LLM distants RETIRES.

# /etc/default/llm-wiki est SOURCE, donc ses affectations ecrasent ce que
# l appelant a mis dans l environnement -- y compris un `Environment=` de
# drop-in systemd. Constate le 2026-09-06 : un drop-in posant RAW_DIR et
# MAX_NOTES_PER_RUN etait visible dans `systemctl show -p Environment` et
# pourtant sans aucun effet sur le run. Un reglage qu on ne peut pas surcharger
# rend impossible tout canary controle et tout banc d essai.
# On sauvegarde donc l environnement EXPLICITE avant de sourcer, et on le
# rejoue apres : le fichier fournit les defauts, l appelant garde le dernier mot.
if [ -r /etc/default/llm-wiki ]; then
    _env_explicite="$(export -p)"
    . /etc/default/llm-wiki
    eval "$_env_explicite"
    unset _env_explicite
fi
# Le fichier est SOURCE, pas exporte : sans cette ligne, aucun reglage
# ci-dessus n atteint llm_wiki_extract.py / llm_wiki_merge.py, qui les
# lisent par os.environ.
# Bascule ChatGPT-seul : plus d'export lie a un LLM (mode de soumission,
# quota). Seuls les reglages deterministes sont propages.
export MAX_ATTEMPTS WIKI_DIR RAW_DIR CHUNK_MIN_TOKENS \
       CHUNK_MIN_TOKENS_FLOOR CHUNK_TARGET_RATIO CHUNK_OVERLAP_RATIO \
       MAX_TPM_REJECTS BYTES_PER_TOKEN INDEX_PAGE_SIZE INDEX_PAGINATE_OVER \
       EXTRACT_SPOOL 2>/dev/null || true

# --- Source LUE par l ingestion. Distincte de WIKI_DIR, qui est la cible ECRITE.
# Defaut retro-compatible : ${WIKI_DIR}/raw. En production elle vaut
# /srv/vault-mirror/raw (adr/0012, invariant 7), qui est la seule copie tenue
# a jour ET fidele aux suppressions faites sur Drive.
# Ne JAMAIS pointer WIKI_DIR sur le miroir : il est la cible d un rclone sync
# destructif, les fiches produites y seraient effacees au passage suivant.
RAW_DIR="${RAW_DIR:-${WIKI_DIR}/raw}"
MAX_NOTES_PER_RUN="${MAX_NOTES_PER_RUN:-25}"
INTER_NOTE_SLEEP="${INTER_NOTE_SLEEP:-5}"
# Jalons historiques de reprise quota (plus ecrits depuis la bascule ChatGPT-seul ;
# lus par --status tant que les fichiers existent, supprimes au deploiement).
QUOTA_SAFETY_MARGIN="${QUOTA_SAFETY_MARGIN:-3}"
MAX_CHAIN_RESUMES="${MAX_CHAIN_RESUMES:-8}"
RESUME_DELAY_SECONDS="${RESUME_DELAY_SECONDS:-18180}"
LINT_DELAY_SECONDS="${LINT_DELAY_SECONDS:-21600}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"
# index.md du vault reel est cure a la main (resumes d'une ligne, 121 Ko) : le
# regenerer en liste de liens nue le detruirait. C'est l'agent qui le met a jour,
# conformement a HERMES.md. Mettre a 1 uniquement pour un wiki dont l'index est
# integralement derive du contenu (cas de /srv/llm-wiki).
REGEN_INDEX="${REGEN_INDEX:-0}"
# Garde-fou : une promotion est refusee si index.md perd plus de N % de ses lignes.
INDEX_MAX_SHRINK_PCT="${INDEX_MAX_SHRINK_PCT:-5}"

RUN_ID="$(date +%s)-$$"

log() { printf '[llm-wiki] %s\n' "$*"; }
err() { printf '[llm-wiki] %s\n' "$*" >&2; }

# ---------------------------------------------------------------- notifications
# llmingest n'a AUCUN acces a send_telegram.sh (0700 root). Il depose une demande
# dans le spool ; le poller, qui tourne en root, la draine et l'envoie.
notify() {
    local emoji="$1" text="$2" persist="${3:-0}"
    [ -d "$SPOOL_DIR" ] || return 0
    local tmp dst
    tmp="${SPOOL_DIR}/.tmp.${RUN_ID}.${RANDOM}"
    dst="${SPOOL_DIR}/$(date +%s)-${RUN_ID}-${RANDOM}.json"
    jq -nc --arg e "$emoji" --arg t "$text" --argjson p "$persist" \
       '{emoji:$e, text:$t, persist:$p}' > "$tmp" 2>/dev/null || return 0
    mv -f "$tmp" "$dst" 2>/dev/null || rm -f "$tmp"
}

# ---------------------------------------------------------------- jalons
write_atomic() {
    local target="$1" value="$2" tmp
    tmp="${target}.tmp.$$"
    printf '%s\n' "$value" > "$tmp" && mv -f "$tmp" "$target"
}
resume_count_get() { [ -f "$RESUME_COUNT" ] && cat "$RESUME_COUNT" 2>/dev/null || echo 0; }

# ---------------------------------------------------------------- index
# index.md est reecrit EN PLACE, pas par tmp+rename : sous ProtectSystem=strict,
# seul le fichier index.md est en ReadWritePaths, pas le repertoire qui le
# contient - creer un ".tmp" a cote echoue en EROFS. Le temporaire vit donc dans
# .staging, et le contenu est verse dans le fichier existant.
# L'atomicite est perdue ; c'est sans consequence : l'index est integralement
# deterministe et regenere a chaque fin de run.
regen_index() {
    [ "$REGEN_INDEX" = "1" ] || return 0
    local tmp="${STAGING_ROOT}/.index.$$"
    mkdir -p "$STAGING_ROOT" 2>/dev/null || true
    {
        echo "# LLM Wiki Index"
        echo ""
        echo "## Sources"
        for s in "${SOURCES_DIR}"/*.md; do [ -f "$s" ] || continue; echo "- [[$(basename "$s" .md)]]"; done
        echo ""
        echo "## Entities"
        for e in "${ENTITIES_DIR}"/*.md; do [ -f "$e" ] || continue; echo "- [[$(basename "$e" .md)]]"; done
        echo ""
        echo "## Concepts"
        for c in "${CONCEPTS_DIR}"/*.md; do [ -f "$c" ] || continue; echo "- [[$(basename "$c" .md)]]"; done
    } > "$tmp" && cat "$tmp" > "$INDEX_FILE"
    rm -f "$tmp"
}

# ---------------------------------------------------------------- manifeste v2
# (Detection quota AGY/REST supprimee a la bascule ChatGPT-seul : aucun quota
# LLM externe ne gouverne plus l'ingestion.)
manifest_append() {
    # 1 path  2 sha  3 size  4 mtime  5 status  6 reason  7 attempts  8 duration  9 produced(json)
    local line
    line=$(jq -nc --arg p "$1" --arg s "$2" --argjson sz "$3" --argjson mt "$4" \
                  --arg st "$5" --arg rs "$6" --argjson at "$7" --argjson du "$8" \
                   --argjson pr "$9" --arg md "chatgpt" --arg ia "$(date -u +%FT%TZ)" \
        '{schema:2,path:$p,sha256:$s,size:$sz,mtime:$mt,ingested_at:$ia,status:$st,reason:(if $rs=="" then null else $rs end),attempts:$at,model:$md,duration_s:$du,produced:$pr}')
    printf '%s\n' "$line" >> "$MANIFEST"
    sync -d "$(dirname "$MANIFEST")" 2>/dev/null || true
}

# Etat courant = derniere ligne par chemin.
manifest_state() {
    [ -s "$MANIFEST" ] || { echo '{}'; return; }
    jq -sc 'map(select(type=="object")) | group_by(.path) | map({key:.[0].path, value:(sort_by(.ingested_at)|last)}) | from_entries' "$MANIFEST" 2>/dev/null || echo '{}'
}

sha_of() { sha256sum -- "$1" 2>/dev/null | cut -d" " -f1; }

# Liste des fichiers eligibles, un par ligne.

# --- Decouverte des sources eligibles. Point unique de verite : list_eligible et
# cmd_status DOIVENT passer par ici, sinon le status ment sur le backlog reel.
# INGEST_EXCLUDE_DIRS : chemins absolus de repertoires elagues avant tout examen.
# Defaut : raw/assets/ConvIA (transcripts bruts). Decision d architecture :
# le raw ConvIA reste dans le Vault et reste indexe par le RAG, mais il ne doit
# plus etre transforme en fiches wiki/sources. Le prefixe est teste en egalite
# exacte : raw/assets/ConvIA-Analysis n est PAS elague et reste ingere.
INGEST_EXCLUDE_DIRS="${INGEST_EXCLUDE_DIRS-${RAW_DIR}/assets/ConvIA}"

raw_find() {
    local sep="${1:--print0}"
    local args=() d
    for d in $INGEST_EXCLUDE_DIRS; do
        args+=( -path "$d" -prune -o )
    done
    find "$RAW_DIR" \
        "${args[@]}" \
        \( -type d -name assets -not -path "*/raw/assets" -prune \) -o \
        -type f \( -name '*.md' -o -name '*.txt' -o -name '*.docx' \) "$sep" 2>/dev/null
}

# Un fichier absent du manifeste est eligible sans calcul de hash : c'est le cas
# de l'ecrasante majorite du backlog. Le hash n'est calcule que pour les fichiers
# deja connus, ou il sert a detecter une modification du source.
# Argument optionnel : nombre maximum de lignes a produire. Sans lui, la boucle
# appelante qui s'arrete au plafond laisse le producteur ecrire dans un tube
# ferme, ce qui inonde le journal de "broken pipe".
list_eligible() {
    local limit="${1:-0}" emitted=0
    local tsv f st att old_sha sha
    tsv=$(mktemp)
    manifest_state | jq -r 'to_entries[]|"\(.key)\t\(.value.status)\t\(.value.attempts // 0)\t\(.value.sha256 // "")"' > "$tsv"
    raw_find -print0 \
    | while IFS= read -r -d '' f; do
        local rec emit=0
        rec=$(grep -m1 -F "$(printf '%s\t' "$f")" "$tsv" 2>/dev/null || true)
        if [ -z "$rec" ]; then
            emit=1
        else
            IFS=$'\t' read -r _ st att old_sha <<< "$rec"
            sha=$(sha_of "$f")
            if [ "$sha" != "$old_sha" ]; then
                emit=1
            else
                case "$st" in
                    ok|skipped) emit=0 ;;
                    failed) [ "${att:-0}" -lt "$MAX_ATTEMPTS" ] && emit=1 ;;
                    *) emit=1 ;;
                esac
            fi
        fi
        if [ "$emit" = "1" ]; then
            printf '%s\n' "$f"
            emitted=$((emitted+1))
            [ "$limit" -gt 0 ] && [ "$emitted" -ge "$limit" ] && break
        fi
    done
    rm -f "$tsv"
}
count_remaining() { list_eligible | grep -c . || true; }

attempts_for() {
    local p="$1" s="$2"
    [ -s "$MANIFEST" ] || { echo 0; return; }
    jq -s --arg p "$p" --arg s "$s" \
      '[.[] | select(.path==$p and .sha256==$s)] | (sort_by(.ingested_at)|last|.attempts) // 0' "$MANIFEST" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------- trap EXIT
# (Jalons de reprise quota supprimes a la bascule ChatGPT-seul : aucun worker
# LLM local ne s'auto-replanifie plus. La cadence est portee par l'unique tache
# horaire ChatGPT, qui reconstruit son etat via MCP a chaque run.)

# ---------------------------------------------------------------- validation
# Valide le DELTA du staging : uniquement les fiches ajoutees ou modifiees par
# ce run. Valider tout le staging ferait echouer chaque run sur les defauts du
# corpus historique, que le linter est justement la pour signaler.
# Un echec empeche toute promotion.
validate_stage() {
    local stage="$1" produced_out="$2"
    : > "$produced_out"
    local ok=1 f rel body fm k ftype dtype required real
    while IFS= read -r f; do
        rel="${f#"$stage"/}"
        # --- delta uniquement
        real="${WIKI_DIR}/${rel}"
        if [ -f "$real" ] && cmp -s "$f" "$real"; then continue; fi
        case "$rel" in
            wiki/sources/*|wiki/entities/*|wiki/concepts/*) : ;;
            *) err "validation: fichier hors perimetre: $rel"; ok=0; continue ;;
        esac
        # Le nom de fichier EST la cible du wikilink : Obsidian resout [[MXR 17]]
        # vers "MXR 17.md". Majuscules et espaces sont donc legitimes - 913 fiches
        # du corpus en portent. On ne controle que ce qui rendrait le fichier ou le
        # lien inutilisable.
        base=$(basename "$rel")
        if printf '%s' "$base" | grep -qE '[/:*?"<>|]' || [ "$base" = ".md" ]; then
            err "validation: nom de fichier inutilisable: $rel"; ok=0; continue
        fi
        if ! head -1 "$f" | grep -qx -- '---'; then
            err "validation: frontmatter absent: $rel"; ok=0; continue
        fi
        fm=$(awk 'NR>1 && /^---[[:space:]]*$/{exit} NR>1{print}' "$f")
        # Jeu de cles releve sur le corpus reel (1946 sources, 283 entites) :
        #   sources  -> title, type, tags, last_updated, links  (source_count frequent)
        #   entites  -> tags obligatoire ; title/type rares, donc non exiges
        # Aucune trace de original_file / created_at / summary dans ce vault.
        dtype=$(basename "$(dirname "$rel")")
        if [ "$dtype" = "sources" ]; then
            required="title type tags last_updated"
        else
            required="tags"
        fi
        for k in $required; do
            printf '%s\n' "$fm" | grep -qE "^${k}:" || { err "validation: cle '$k' absente: $rel"; ok=0; }
        done
        ftype=$(printf '%s\n' "$fm" | sed -nE 's/^type:[[:space:]]*"?([A-Za-z_]+)"?.*/\1/p' | head -1)
        # type est quasi absent des entites/concepts du corpus reel : on ne le
        # controle que s'il est present, et on l'exige seulement sur les sources.
        case "$dtype" in
            sources)  [ "$ftype" = "source" ]  || { err "validation: type='$ftype' incoherent avec sources: $rel"; ok=0; } ;;
            entities) [ -z "$ftype" ] || [ "$ftype" = "entity" ]  || { err "validation: type='$ftype' incoherent avec entities: $rel"; ok=0; } ;;
            concepts) [ -z "$ftype" ] || [ "$ftype" = "concept" ] || { err "validation: type='$ftype' incoherent avec concepts: $rel"; ok=0; } ;;
        esac
        body=$(awk 'f{print} /^---[[:space:]]*$/{n++; if(n==2) f=1}' "$f")
        if [ "$(printf '%s' "$body" | wc -c)" -lt 200 ]; then
            err "validation: corps < 200 o (fiche degradee ?): $rel"; ok=0; continue
        fi
        printf '%s\n' "$body" | grep -qE '^#' || { err "validation: aucun titre Markdown: $rel"; ok=0; }
        printf '%s\n' "$rel" >> "$produced_out"
    done < <(find "$stage/wiki" -type f -name '*.md' 2>/dev/null)
    [ "$ok" = "1" ]
}

# ---------------------------------------------------------------- prompt
# (Prompt AGY supprime a la bascule ChatGPT-seul : aucun LLM n'est pilote
# depuis ce script. Le prompt d'extraction vit dans llm_wiki_extract.py
# SYSTEM_PREFIX, consomme par ChatGPT via la file MCP.)

# ---------------------------------------------------------------- sous-commandes
cmd_migrate() {
    log "Migration du manifeste v1 -> v2"
    [ -f "$MANIFEST_V1" ] || { log "aucun manifeste v1"; return 0; }
    touch "$MANIFEST"
    local bak np p n=0 miss=0
    bak="${MANIFEST_V1}.v1.bak.$(date +%s)"
    cp -a "$MANIFEST_V1" "$bak"
    log "sauvegarde : $bak"
    while IFS= read -r p; do
        [ -n "$p" ] || continue
        np="${p/#\/home\/juliann\/llm-wiki//srv/llm-wiki}"
        if [ -f "$np" ]; then
            manifest_append "$np" "$(sha_of "$np")" "$(stat -c %s "$np")" "$(stat -c %Y "$np")" "ok" "migrated_v1" 1 0 '[]'
            n=$((n+1))
        else
            manifest_append "$np" "" 0 0 "skipped" "source_missing_at_migration" 1 0 '[]'
            miss=$((miss+1))
        fi
    done < <(sort -u "$MANIFEST_V1")
    log "migres : ${n} ok, ${miss} sources disparues. Ancien fichier conserve."
}

cmd_status() {
    local total elig st ok failed skipped migrated remaining
    total=$(find "$RAW_DIR" -type f 2>/dev/null | wc -l)
    elig=$(raw_find -print | wc -l)
    st=$(manifest_state)
    ok=$(printf '%s' "$st" | jq '[.[]|select(.status=="ok")]|length')
    failed=$(printf '%s' "$st" | jq '[.[]|select(.status=="failed")]|length')
    skipped=$(printf '%s' "$st" | jq '[.[]|select(.status=="skipped")]|length')
    migrated=$(printf '%s' "$st" | jq '[.[]|select(.reason=="migrated_v1")]|length')
    remaining=$(count_remaining)
    printf 'LLM Wiki - etat d ingestion            %s\n\n' "$(date -u '+%F %H:%M UTC')"
    printf 'Source  raw/          %6s fichiers eligibles (%s au total)\n' "$elig" "$total"
    printf 'Traites ok            %6s   dont %s migres du format v1\n' "$ok" "$migrated"
    printf 'Echecs  failed        %6s   retentables (attempts < %s)\n' "$failed" "$MAX_ATTEMPTS"
    printf 'Abandon skipped       %6s   voir --list-failed\n' "$skipped"
    printf 'Restant a traiter     %6s\n\n' "$remaining"
    if [ -f "$INGEST_DUE" ]; then
        printf 'Reprise planifiee     %s UTC (chaine %s/%s)\n' \
          "$(date -u -d "@$(cat "$INGEST_DUE")" '+%F %H:%M')" "$(resume_count_get)" "$MAX_CHAIN_RESUMES"
    else
        printf 'Reprise planifiee     aucune\n'
    fi
    if [ -f "$LINT_DUE" ]; then
        printf 'Lint planifie         %s UTC\n' "$(date -u -d "@$(cat "$LINT_DUE")" '+%F %H:%M')"
    else
        printf 'Lint planifie         aucun\n'
    fi
    printf '\nMotifs d echec  '
    printf '%s' "$st" | jq -r '[.[]|select(.status!="ok")|.reason//"?"]|group_by(.)|map("\(.[0]) \(length)")|join(" - ")'
}

cmd_list_failed() {
    manifest_state | jq -r 'to_entries[]|select(.value.status!="ok")|"\(.value.status)\t\(.value.attempts)\t\(.value.reason//"?")\t\(.key)"'
}

cmd_retry() {
    local target="${1:-}" p
    [ -n "$target" ] || { err "usage: ingest.sh --retry <chemin|motif>"; return 2; }
    while IFS= read -r p; do
        [ -n "$p" ] || continue
        if [ -f "$p" ]; then
            manifest_append "$p" "$(sha_of "$p")" "$(stat -c %s "$p")" "$(stat -c %Y "$p")" "failed" "manual_retry" 0 0 '[]'
            log "remis en file : $p"
        fi
    done < <(manifest_state | jq -r --arg t "$target" 'to_entries[]|select(.value.status!="ok")|select(.key==$t or .value.reason==$t)|.key')
    log "termine"
}

# ---------------------------------------------------------------- run principal
# Chemin historique AGY : RETIRE a la bascule ChatGPT-seul.
# --dry-run (listage des eligibles) reste disponible ; tout run reel refuse :
# l'extraction est produite par ChatGPT via la file MCP.
cmd_run() {
    local dry="$1" cap n file
    if [ "$dry" != "1" ]; then
        err "chemin AGY retire (bascule ChatGPT-seul) : produire l'extraction via"
        err "la file MCP wiki_ingest_claim -> wiki_ingest_read -> wiki_ingest_submit,"
        err "puis drainer avec wiki_ingest_merge_pending (ou $0 --merge)."
        return 2
    fi
    cap="$MAX_NOTES_PER_RUN"; n=0
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        if [ "$n" -ge "$cap" ]; then log "plafond atteint (${cap})"; break; fi
        printf '[DRY-RUN] %s\n' "$file"
        n=$((n+1))
    done < <(list_eligible "$cap")
    log "listage termine : ${n} eligible(s)"
}

# ---------------------------------------------------------------- passe 1
# PASSE 1 (extraction) : RETIREE a la bascule ChatGPT-seul.
# L'extraction est produite par ChatGPT via la file MCP (claim/read/submit),
# spoolisee par le serveur apres validation. Ce script ne contacte plus aucun LLM.
cmd_extract() {
    err "extraction locale retiree (bascule ChatGPT-seul, contrat wiki-extract-v4)"
    err "produire l'extraction via la file MCP : wiki_ingest_claim -> wiki_ingest_read -> wiki_ingest_submit"
    return 2
}

# ---------------------------------------------------------------- passe 2
# PASSE 2 (lot 3) : fusion du spool d extraction dans le wiki. Aucun appel
# modele, donc aucun quota consomme : elle DOIT tourner meme quand la passe 1
# s est arretee sur un 429 (sortie 75), sinon le travail LLM deja paye reste au
# spool et le wiki n avance jamais.
cmd_merge() {
    local rc=0
    log "passe 2 (fusion) : spool=${EXTRACT_SPOOL} vault=${WIKI_DIR}"
    python3 /usr/local/bin/llm_wiki_merge.py \
        --spool "$EXTRACT_SPOOL" \
        --vault "$WIKI_DIR" \
        --manifest "$MANIFEST" || rc=$?
    return $rc
}

# ------------------------------------------------------------ pipeline nominal
# CHEMIN NOMINAL (bascule ChatGPT-seul) : passe 2 (fusion, zero LLM) puis
# demande de publication. La passe 1 (extraction LLM) n'existe plus ici :
# elle est produite par ChatGPT via la file MCP et spoolisee par le serveur.
cmd_pipeline() {
    local rc_m=0
    cmd_merge || rc_m=$?
    if [ "$rc_m" -ne 0 ]; then
        err "passe 2 en echec (rc=${rc_m})"
    fi
    publish_request
    return "$rc_m"
}

# Demande de publication du wiki vers Drive. Depose un marqueur consomme par
# llm-wiki-publish.path (User=juliann, seul compte porteur du remote gdrive:).
# Meme separation que llm-wiki-ingest-request : le moteur n a pas le remote, et
# le publieur n a pas le wiki en ecriture.
publish_request() {
    if : > "$PUBLISH_REQUEST" 2>/dev/null; then
        log "publication demandee (${PUBLISH_REQUEST})"
    else
        err "marqueur de publication non ecrit : ${PUBLISH_REQUEST}"
    fi
}

# ---------------------------------------------------------------- dispatch
case "${1:-}" in
    --status)           cmd_status ;;
    --list-failed)      cmd_list_failed ;;
    --retry)            cmd_retry "${2:-}" ;;
    --migrate-manifest) cmd_migrate ;;
    --extract)          cmd_extract "${2:-}" ;;
    --merge)            cmd_merge ;;
    --publish)          publish_request ;;
    --dry-run)          cmd_run 1 ;;
    # Point d entree du service : fusion du spool valide (zero LLM) puis demande
    # de publication. Aucun LLM n'est contacte. L'extraction est produite par
    # l'unique tache horaire ChatGPT via la file MCP.
    "")                 cmd_pipeline "" ;;
    *) err "usage: $0 [--extract [n]|--merge|--publish|--dry-run|--status|--list-failed|--retry <cible>|--migrate-manifest]"; exit 2 ;;
esac
