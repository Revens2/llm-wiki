#!/bin/bash
# Cycle hebdomadaire : ne demarre l'ingestion que si aucune reprise n'est deja
# planifiee plus tard. Sans ce garde-fou, le timer du dimanche tombant avant la
# fin d'un quota hebdomadaire relancerait un run voue a heurter le mur, avec la
# notification trompeuse qui va avec.
set -uo pipefail
DUE=/var/lib/llm-wiki/ingest-due-at
NOW=$(date +%s)
if [ -f "$DUE" ]; then
    V=$(cat "$DUE" 2>/dev/null || echo 0)
    case "$V" in ''|*[!0-9]*) V=0 ;; esac
    if [ "$V" -gt "$NOW" ]; then
        logger -t llm-wiki-weekly "reprise deja planifiee le $(date -u -d "@$V" '+%F %H:%M') UTC : run hebdomadaire ignore"
        exit 0
    fi
fi
echo 0 > /var/lib/llm-wiki/resume-count
exec /bin/systemctl start --no-block llm-wiki-ingest.service
