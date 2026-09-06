#!/bin/bash
# NEUTRALISE le 2026-08-22 - ne synchronise plus rien. Conserve pour tracabilite.
#
# Ce script faisait `rclone copy "gdrive:Obsidian Vault/raw/assets" /srv/llm-wiki/raw`.
# Trois defauts, dont deux silencieux :
#   1. il alimentait /srv/llm-wiki/raw, que PERSONNE ne lisait : l ingestion lit
#      RAW_DIR, qui pointait sur /srv/obsidian-vault/raw. Le cycle hebdomadaire
#      synchronisait donc un arbre et en ingerait un autre ;
#   2. `copy` ne supprime jamais : une note effacee sur Drive restait la
#      indefiniment et serait re-ingeree comme si elle existait encore ;
#   3. il ne prenait que le sous-arbre raw/assets, aplati a la racine.
#
# Remplace par vault-mirror-sync.timer (toutes les 30 min, `rclone sync` avec
# --backup-dir), qui produit /srv/vault-mirror - source de verite unique du RAG
# (adr/0012, invariant 7). L ingestion lit desormais /srv/vault-mirror/raw.
#
# Le fichier n est pas supprime : le retirer effacerait la trace de la rupture.
set -uo pipefail
echo "llm_wiki_sync.sh est neutralise depuis le 2026-08-22."
echo "La source du RAG est /srv/vault-mirror, tenue a jour par vault-mirror-sync.timer."
echo "Voir adr/0012 et reference/services.md."
exit 0
