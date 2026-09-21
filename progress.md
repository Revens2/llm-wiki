# progress — llm-wiki

## Revue convia-exclusion (commit 665bc75, parent 8fbc34f) — 2026-09-09

### Périmètre
8 fichiers, +478 / -1610 (git diff 8fbc34f..665bc75 --stat).
- Supprimé : llm_submit.py (1013 lignes, adaptateur Gemini/AGY : pacing AIMD, retries, interactive/batch/agy).
- Réécrit : llm_wiki_extract.py (+169/-256, dé-LLMisé : tokenizer local, _one_call/extract_one en stub refuse hors dry-run, manifeste v4 additif contract_version+tokenizer, exclusion ConvIA bilatérale), llm_wiki_ingest.sh (+52/-300, merge-only : cmd_run dry-run seul + refuse sinon, cmd_extract refuse, quota/AGY/prompt supprimés, pipeline=merge+publish, model manifest dur chatgpt).
- Config/docs : .env.example (clés LLM retirées), README (bascule ChatGPT-seul v4), .claudeignore.
- Tests nouveaux : tests/test_extract_contract.py (158 lignes, 11 tests), tests/test_no_llm_guard.py (52 lignes, 3 tests). Vérifié : 14 passed, zéro réseau.
Base : main.

### Blast radius
- code-review-graph : build 15 fichiers / 153 noeuds / 2408 arêtes ; update --base main --brief : 8 fichiers, 38 fonctions, 0 flux, 23 test gaps, risque 0.85.
- impact --depth 2 : 0 noeud (même artefact --files comma-join qu'en vault-mcp). Repli grep ciblé sur 665bc75 (AGY/agy/GEMINI/gemini/generateContent/countTokens/SUBMIT_MODE/LLM_MODEL/LLM_EFFORT/CAVEMAN/EXTRACT_MODEL/SAFETY/generativelanguage) + lecture extract 840-1084, ingest.sh intégral 462 lignes, merge spool-walk.
- Flux : extraction LLM locale supprimée (seuls --dry-run plan chunking et --print-schema restent) ; nominal = file MCP vault-mcp (claim/read/submit) -> spool sharded sha[:2]/sha.idx.json (identique des deux côtés) -> merge.py os.walk récursif (compatible, additive chunk_hash/contract_version ignorés) -> manifeste. ingest.sh --dry-run/--status/--merge/--publish conservés.

### Risques
- **Bloquant** — aucun. Aucun appel LLM actif résiduel : grep 665bc75 ne remonte que commentaires de retrait (ingest.sh:3,7,141,299,375,381), LEGACY_EXTRACT_MODEL lecture seule (extract:47), et les garde-fous eux-mêmes. llm_submit.py absent (test garde). .env.example sans GEMINI_API_KEY/AGY_BIN/SUBMIT_MODE/LLM_MODEL. ingest.sh n'exporte plus que le déterministe (58-61). main refuse sans dry-run (exit 2, extract:1044-1051), cmd_run/cmd_extract refusent (ingest.sh:378-404).
- **Majeur**
  - Divergence fallback tokenizer sous même étiquette (extract:564 BYTES_PER_TOKEN=2.44 vs vault-mcp wiki_jobs.py:65 BYTES_PER_TOKEN_FALLBACK=4.0, même TOKENIZER local-cl100k-or-bytes4). Sans tiktoken installé, même document mesuré ~60% différemment (100 Ko -> 41k vs 25k tokens) -> plan --dry-run et file réelle divergent sur le seuillage 50k/8k. Pire : docstring serveur (surexposition) fausse pour du FR (vrai ratio corpus 2.44) : bytes/4 SOUS-estime, un gros doc peut passer sous le seuil et rester mono-chunk. Fix : unifier (2.44 documenté ou max des deux) et tester sans tiktoken des deux côtés.
- **Mineur**
  - raw_find multi-exclude cassé (ingest.sh:173-182) : for d in $INGEST_EXCLUDE_DIRS split sur espaces, pas colons, alors que la var est documentée colon-separated (défaut mono-chemin OK, custom multi KO -> prune jamais). Fix : IFS=: read -ra.
  - eligible() sans exclusion + main --file/--files-from sans filtre (extract:838-858, 1030-1040) : list_files filtre ConvIA (888-907) mais --file explicite ne passe que par eligible (jamais _is_excluded) -> --dry-run sur raw/assets/ConvIA/x.md autorisé. Impact nul en écriture (dry-run seul), mais incohérent avec raw_find. Fix : filtrer _is_excluded dans main après --files-from.
  - extract_one() à effets avant refus (extract:955-1005) : os.stat+manifest_state+read+gc_old_chunks/gc_stale_chunks (suppressions spool !) AVANT de rendre refused/llm_retired. CLI safe (main refuse avant appel), mais appel lib direct détruit du spool puis refuse. Fix : refuser en tête de fonction.
  - cmd_status total incohérent (ingest.sh:328 : find RAW_DIR sans exclusion vs elig via raw_find avec exclusion -> total gonflé de ConvIA). Cosmétique. Fix : compter via raw_find.
  - Garde no-LLM incomplète : FORBIDDEN 9 motifs (test_no_llm_guard.py:18-26) manque LLM_MODEL/LLM_EFFORT/EXTRACT_MODEL/CAVEMAN_LEVEL/MAX_OUTPUT_TOKENS/agy-grep. Actuellement verte (vérifié), mais un retour SUBMIT_MODE-like sous autre nom passerait. Élargir + inclure *.py sous-dossiers si ajoutés (glob racine seule).
  - Trois manifests coexistent sans collision (extract MANIFEST schema 3 + contract/tokenizer additifs, ingest.sh MANIFEST schema 2 model chatgpt dur ligne 148, wiki_jobs_manifest.jsonl schema 4 serveur) — OK, mais documenter la séparation (un opérateur peut confondre .ingested_manifest.jsonl et wiki_jobs_manifest.jsonl).
  - manifest_state ignore lignes invalides silencieusement (extract:821-824 continue) — bénin append-only.

### Tests à lancer
- Vérifié : python -m pytest tests/test_no_llm_guard.py tests/test_extract_contract.py -q -> 14 passed.
- Complet : python -m pytest -q ; shellcheck -S error llm_wiki_ingest.sh (gate CI) ; bash -n llm_wiki_ingest.sh.
- À ajouter : multi-INGEST_EXCLUDE_DIRS a:b ; --file ConvIA refusé/filtré ; fallback tokenizer sans tiktoken (monkeypatch import fail) égalité serveur ; manifest v4 relu par manifest_state ; merge.py sur enveloppe serveur (chunk_hash+contract_version).
- Garde contractuelle : grep FORBIDDEN en CI déjà ; ajouter LLM_MODEL/LLM_EFFORT/EXTRACT_MODEL aux motifs principaux.

### Verdict
Mergeable : zéro LLM actif, exclusion ConvIA bilatérale (ingest.sh raw_find + extract list_files), spool/merge compatibles, 14 verts. Corriger en suivi : unifier fallback tokenizer (seul majeur, dry-run uniquement), IFS exclude, filtre --file, refus en tête d'extract_one, durcir garde. Aucune régression RAG (ConvIA reste indexé, jamais en fiches).
