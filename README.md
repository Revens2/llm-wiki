# llm-wiki

Chaîne d'ingestion et de contrôle d'un **wiki RAG** alimenté par LLM : un vault de
notes brut (`RAW_DIR`) est transformé en wiki structuré (sources / entités / concepts,
`index.md`) via l'API Gemini, avec une ingestion **résiliente au quota** (429 /
RESOURCE_EXHAUSTED), incrémentale et traçable, et un **lint en lecture seule** du RAG.

> Dépôt : outils de la chaîne uniquement (scripts de service). Ni le vault source ni
> le wiki généré n'y figurent — ce sont des données.

## Scripts

| Script | Rôle |
|---|---|
| `llm_wiki_poll.sh` | Poller (root, ~2 min) : draine le spool de notifications, gère la branche reprise (exclusive) puis lint |
| `llm_wiki_weekly.sh` | Cycle hebdomadaire : ne démarre que si aucune reprise n'est planifiée |
| `llm_wiki_ingest.sh` | Ingestion incrémentale résiliente au quota (manifest, reprises, notifications) |
| `llm_submit.py` | Adaptateur de soumission LLM (stdlib seule) : pacing AIMD, retries, mode interactive (Gemini `generateContent`), historique `agy` |
| `llm_wiki_extract.py` | Passe d'extraction JSON structurée, chunking piloté par le TPM |
| `llm_wiki_merge.py` | Assemblage des extractions dans le wiki |
| `llm_wiki_repair.py`, `llm_wiki_make_stubs.py`, `llm_wiki_fix_frontmatter.py`, `llm_wiki_fix_verify.py` | Réparation / cohérence des fichiers du wiki |
| `llm_wiki_lint.py` + `llm_wiki_lint_run.sh` | Audit RAG **lecture seule** (règles R1-R24), rapport Markdown/JSON |
| `llm_wiki_sync.sh` | **Neutralisé** — conservé pour traçabilité (remplacé par la synchro du miroir du vault) |

## Configuration

Fichier d'environnement (ex. `/etc/default/llm-wiki`) — noms dans `.env.example`,
aucune valeur réelle versionnée. La clé API Gemini passe par `GEMINI_API_KEY`.

## Principes de conception

- Un **quota n'incrémente jamais `attempts`** : le statut quota est structurellement
  distinct de l'erreur HTTP (cf. `llm_submit.py`).
- Le **chunking est piloté par le TPM**, pas par la fenêtre de contexte ; le seuil
  s'abaisse seul sur rejet TPM et ne remonte jamais sans mesure.
- Garantie d'immuabilité du RAG en 3 couches (namespace lecture seule, utilisateur
  dédié, lint détecteur de divergence).
