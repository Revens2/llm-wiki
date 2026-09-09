# llm-wiki

Chaîne d'ingestion et de contrôle d'un **wiki RAG** : un vault de
notes brut (`RAW_DIR`) est transformé en wiki structuré (sources / entités / concepts,
`index.md`). Depuis la bascule **ChatGPT-seul** (2026-09-09, contrat `wiki-extract-v4`),
**ChatGPT est le seul LLM actif** : il produit les extractions via la file MCP
(`wiki_ingest_claim` → `wiki_ingest_read` → `wiki_ingest_submit`, autorité serveur
dans vault-mcp `wiki_jobs.py`), et ce dépôt ne garde que le **déterministe**
(éligibilité, chunking à tokenizer local, validation, spool, fusion, manifeste,
lint). **Aucun appel LLM local** (ancien adaptateur distant et chemin externe
supprimés ; `llm_wiki_ingest.sh` ne fait plus que lister, fusionner et publier).

> Dépôt : outils de la chaîne uniquement (scripts de service). Ni le vault source ni
> le wiki généré n'y figurent — ce sont des données.

## Scripts

| Script | Rôle |
|---|---|
| `llm_wiki_poll.sh` | Poller (root, ~2 min) : draine le spool de notifications, puis lint |
| `llm_wiki_weekly.sh` | Cycle hebdomadaire : fusion du spool si présent |
| `llm_wiki_ingest.sh` | Éligibilité, statuts, retry manuel, **fusion du spool (zéro LLM)**, demande de publication. L'ancien run LLM et l'extraction locale **refusent** (file MCP à la place) |
| `llm_wiki_extract.py` | Contrat d'extraction (schéma + validation), chunking déterministe à tokenizer local, spool, manifeste. **Aucun appel LLM** (`--dry-run` et `--print-schema` seuls en local) |
| `llm_wiki_merge.py` | Assemblage des extractions dans le wiki (zéro LLM, inchangé) |
| `llm_wiki_repair.py`, `llm_wiki_make_stubs.py`, `llm_wiki_fix_frontmatter.py`, `llm_wiki_fix_verify.py` | Réparation / cohérence des fichiers du wiki |
| `llm_wiki_lint.py` + `llm_wiki_lint_run.sh` | Audit RAG **lecture seule** (règles R1-R24), rapport Markdown/JSON |
| `llm_wiki_sync.sh` | **Neutralisé** — conservé pour traçabilité (remplacé par la synchro du miroir du vault) |

## Configuration

Fichier d'environnement (ex. `/etc/default/llm-wiki`) — noms dans `.env.example`,
aucune valeur réelle versionnée. Depuis la bascule, **aucune clé LLM n'est
requise côté service** : les secrets distants historiques ont été retirés de la
configuration (ne jamais les réintroduire pour l'ingestion).

## Principes de conception

- **ChatGPT raisonne, le serveur garantit** : file persistante, leases + fencing,
  validation serveur obligatoire, idempotence, CAS source, quarantaine bornée.
- Le **chunking est déterministe** (tokenizer local, jamais de service LLM) ;
  le seuil ne remonte jamais sans mesure.
- Garantie d'immuabilité du RAG en 3 couches (namespace lecture seule, utilisateur
  dédié, lint détecteur de divergence).
