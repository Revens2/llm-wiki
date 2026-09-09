"""Garde-fou no-LLM : aucun appel Gemini/AGY dans le chemin production.

Echec si un identifiant d'appel LLM actif reapparait dans *.py / *.sh
(generateContent, mesure distante, cle distante, binaire externe, mode de
soumission, quota distant). Les simples mentions historiques en toutes
lettres dans les commentaires/docs (ex. "ancien chemin ... retire") ne
declenchent pas : seuls les identifiants machine comptent.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Identifiants machine d'appel LLM : leur presence = chemin actif.
FORBIDDEN = [
    "generateContent",
    "countTokens",
    "GEMINI_API_KEY",
    "AGY_BIN",
    "SUBMIT_MODE",
    "RESOURCE_EXHAUSTED",
    "EXTRACT_MODEL_CASCADE",
    "SAFETY_BLOCK_NONE",
    "generativelanguage.googleapis.com",
]

ALLOW_FILES = {"test_no_llm_guard.py"}  # ce fichier cite les motifs


def test_aucun_identifiant_llm_actif():
    hits = []
    for path in sorted(REPO.glob("*.py")) + sorted(REPO.glob("*.sh")):
        if path.name in ALLOW_FILES or path.name.startswith("test_"):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for pat in FORBIDDEN:
            if pat in text:
                hits.append(f"{path.name}:{pat}")
    assert hits == [], f"chemin LLM reactivi : {hits}"


def test_adaptateur_llm_supprime():
    assert not (REPO / "llm_submit.py").exists(), \
        "llm_submit.py doit rester supprime (bascule ChatGPT-seul)"


def test_pas_de_cle_llm_dans_env_example():
    text = (REPO / ".env.example").read_text(encoding="utf-8")
    for pat in ("GEMINI_API_KEY", "AGY_BIN", "SUBMIT_MODE", "LLM_MODEL"):
        assert pat not in text, pat
