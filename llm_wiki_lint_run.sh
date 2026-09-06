#!/bin/bash
# Execution du lint RAG : empreinte avant/apres, rapport, notification Telegram.
#
# Couche 3 de la garantie d'immuabilite. Couche 1 : ReadOnlyPaths (noyau, EROFS).
# Couche 2 : llmlint hors du groupe llmwiki. Ici on DETECTE une divergence, on ne
# l'empeche pas - c'est un filet, pas la barriere.
set -uo pipefail

WIKI_DIR="/srv/obsidian-vault"
REPORT_DIR="/var/lib/llm-wiki-lint"
SPOOL_DIR="/var/lib/llm-wiki/notify-spool"
LINT_PY="/usr/local/bin/llm_wiki_lint.py"
KEEP_REPORTS=12

fingerprint() {
    find "${WIKI_DIR}/wiki" "${WIKI_DIR}/index.md" -type f -printf '%p %s %T@\n' 2>/dev/null \
      | sort | sha256sum | cut -d' ' -f1
}

mkdir -p "$REPORT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REPORT="${REPORT_DIR}/report-${STAMP}.md"
JSONR="${REPORT_DIR}/report-${STAMP}.json"

H1=$(fingerprint)
python3 "$LINT_PY" > "$REPORT" 2>"${REPORT}.err"; RC=$?
python3 "$LINT_PY" --json > "$JSONR" 2>/dev/null || true
H2=$(fingerprint)

IMMUABLE="oui"
if [ "$H1" != "$H2" ]; then
    IMMUABLE="NON"
    {
        echo ""
        echo "## ALERTE CRITIQUE - immuabilite violee"
        echo ""
        echo "L'empreinte du RAG a change pendant le lint."
        echo "- avant : \`${H1}\`"
        echo "- apres : \`${H2}\`"
        echo ""
        echo "Le lint est cense etre en lecture seule. Investiguer immediatement :"
        echo "une autre ecriture concurrente, ou un durcissement inoperant."
    } >> "$REPORT"
fi

# rotation
ls -1t "${REPORT_DIR}"/report-*.md 2>/dev/null | tail -n +$((KEEP_REPORTS+1)) | xargs -r rm -f
ls -1t "${REPORT_DIR}"/report-*.json 2>/dev/null | tail -n +$((KEEP_REPORTS+1)) | xargs -r rm -f

# --- synthese pour la notification
CRIT=$(grep -c '^| R' "$REPORT" 2>/dev/null || echo 0)
NB() { grep -oP "^- $1 : \K[0-9]+" "$REPORT" 2>/dev/null | head -1 || echo 0; }
C=$(NB CRITIQUE); M=$(NB MAJEUR); N=$(NB MINEUR); I=$(NB INFO)
PAGES=$(grep -oP '^- fiches auditees : \K[0-9]+' "$REPORT" 2>/dev/null | head -1 || echo "?")

SUMMARY="Audit RAG llm-wiki - ${PAGES} fiches
CRITIQUE ${C:-0} - MAJEUR ${M:-0} - MINEUR ${N:-0} - INFO ${I:-0}
Immuabilite du RAG : ${IMMUABLE}
Rapport complet en piece jointe."

# Le service tourne en llmlint : il n'a pas acces a send_telegram.sh (0700 root).
# On depose dans le spool ; le poller root envoie. La piece jointe, elle, est
# envoyee par le poller via un marqueur 'attach'.
if [ -d "$SPOOL_DIR" ] && [ -w "$SPOOL_DIR" ]; then
    TMP="${SPOOL_DIR}/.tmp.lint.$$"
    DST="${SPOOL_DIR}/$(date +%s)-lint-$$.json"
    jq -nc --arg t "$SUMMARY" --arg a "$REPORT" \
       '{emoji:"LINT", text:$t, persist:0, attach:$a}' > "$TMP" 2>/dev/null && mv -f "$TMP" "$DST"
fi

echo "rapport : $REPORT (rc=$RC, immuable=$IMMUABLE)"
# Un RAG incoherent n'est pas une panne d'unite : on ne fait pas echouer le service.
exit 0
