#!/bin/bash
# Poller unique du pipeline llm-wiki. Tourne en root, toutes les 2 minutes.
#
# Trois responsabilites, dans cet ordre :
#   1. drainer le spool de notifications depose par llmingest (qui n'a pas acces
#      a send_telegram.sh, 0700 root) ;
#   2. branche RESUME - prioritaire et EXCLUSIVE ;
#   3. branche LINT - atteinte seulement si aucun jalon de reprise n'existe.
#
# L'exclusion mutuelle des deux jalons est une propriete du flot de controle :
# l'etape 2 sort dans TOUS ses cas de figure, y compris "jalon present mais pas
# encore du". Tant qu'une reprise est planifiee, le lint n'est jamais evalue.
set -uo pipefail

STATE_DIR="/var/lib/llm-wiki"
SPOOL_DIR="${STATE_DIR}/notify-spool"
INGEST_DUE="${STATE_DIR}/ingest-due-at"
LINT_DUE="${STATE_DIR}/lint-due-at"
HEARTBEAT="${STATE_DIR}/.poll-heartbeat"
LOCK="${STATE_DIR}/.poll.lock"

MAX_DRAIN=20
MAX_SPOOL_FILES=200
MAX_SPOOL_BYTES=65536

exec 9>"$LOCK" || exit 0
flock -n 9 || exit 0          # jamais deux pollers simultanes

touch "$HEARTBEAT"

redact() {
    sed -E -e 's#(bot)?[0-9]{8,10}:AA[A-Za-z0-9_-]{30,}#<REDACTED_TG_TOKEN>#g' \
           -e 's#(Bearer|Authorization:)[[:space:]]*[A-Za-z0-9._~+/=-]{16,}#\1 <REDACTED>#gI' \
           -e 's#(ya29\.|AIza|AQ\.|sk-|ghp_|gho_|xox[baprs]-)[A-Za-z0-9._~+/=-]{10,}#<REDACTED_KEY>#g' \
           -e 's#(password|passwd|secret|token|api[_-]?key)([[:space:]]*[:=][[:space:]]*)[^[:space:],;]+#\1\2<REDACTED>#gI'
}

# ---------------------------------------------------------------- 1. spool
drain_spool() {
    [ -d "$SPOOL_DIR" ] || return 0
    local count total f size emoji text persist body
    total=$(find "$SPOOL_DIR" -maxdepth 1 -name '*.json' | wc -l)
    if [ "$total" -gt "$MAX_SPOOL_FILES" ]; then
        # protection contre un remplissage volontaire du disque
        find "$SPOOL_DIR" -maxdepth 1 -name '*.json' -printf '%T@ %p\n' \
          | sort -n | head -n $(( total - MAX_SPOOL_FILES )) | cut -d' ' -f2- | xargs -r rm -f
        logger -t llm-wiki-poll "spool purge : ${total} entrees > ${MAX_SPOOL_FILES}"
    fi
    count=0
    while IFS= read -r f; do
        [ "$count" -ge "$MAX_DRAIN" ] && break
        size=$(stat -c %s "$f" 2>/dev/null || echo 0)
        if [ "$size" -gt "$MAX_SPOOL_BYTES" ]; then rm -f "$f"; continue; fi
        if ! jq -e . "$f" >/dev/null 2>&1; then rm -f "$f"; continue; fi
        emoji=$(jq -r '.emoji // "INFO"' "$f")
        text=$(jq -r '.text // ""' "$f" | redact | head -c 3600)
        persist=$(jq -r '.persist // 0' "$f")
        attach=$(jq -r '.attach // ""' "$f")
        [ -z "$text" ] && { rm -f "$f"; continue; }
        case "$emoji" in
            OK)     emoji="✅" ;;
            QUOTA)  emoji="⏳" ;;
            STOP)   emoji="🛑" ;;
            RESUME) emoji="🔁" ;;
            LINT)   emoji="📋" ;;
            *)      emoji="📥" ;;
        esac
        body="${emoji} ${text}"
        TG_PERSIST="$persist" /usr/local/bin/send_telegram.sh "$body" >/dev/null 2>&1 || true
        # Piece jointe : uniquement un chemin sous /var/lib/llm-wiki-lint, jamais
        # un chemin arbitraire fourni par un compte confine.
        case "$attach" in
            /var/lib/llm-wiki-lint/report-*.md)
                [ -f "$attach" ] && /usr/local/bin/send_telegram_doc.sh "$attach" "Rapport de lint RAG" >/dev/null 2>&1 || true
                ;;
        esac
        rm -f "$f"
        count=$((count+1))
    done < <(find "$SPOOL_DIR" -maxdepth 1 -name '*.json' -printf '%T@ %p\n' | sort -n | cut -d' ' -f2-)
}
drain_spool

NOW=$(date +%s)

# ---------------------------------------------------------------- 2. RESUME
if [ -f "$INGEST_DUE" ]; then
    DUE=$(cat "$INGEST_DUE" 2>/dev/null || echo 0)
    case "$DUE" in ''|*[!0-9]*) DUE=0 ;; esac
    if [ "$NOW" -lt "$DUE" ]; then
        exit 0                                   # le lint n'est PAS evalue
    fi
    if systemctl is-active --quiet llm-wiki-ingest.service; then
        exit 0
    fi
    rm -f "$INGEST_DUE"                          # consommation AVANT lancement
    logger -t llm-wiki-poll "reprise d ingestion declenchee"
    systemctl start --no-block llm-wiki-ingest.service
    exit 0
fi

# ---------------------------------------------------------------- 3. LINT
[ -f "$LINT_DUE" ] || exit 0
DUE=$(cat "$LINT_DUE" 2>/dev/null || echo 0)
case "$DUE" in ''|*[!0-9]*) DUE=0 ;; esac
[ "$NOW" -lt "$DUE" ] && exit 0
systemctl is-active --quiet llm-wiki-ingest.service && exit 0   # ceinture
rm -f "$LINT_DUE"
logger -t llm-wiki-poll "lint RAG declenche"
systemctl start --no-block llm-wiki-lint.service
exit 0
