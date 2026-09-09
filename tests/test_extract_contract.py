"""Contrat d'extraction preserve (v4) : validation, chunking, exclusion, CLI.

Aucun appel LLM : --dry-run et --print-schema seuls, tout le reste refuse.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import llm_wiki_extract as X  # noqa: E402


def _doc(**kw):
    body = ("Corps de fiche amplement suffisant pour la validation serveur. " * 6)
    d = {
        "language": "fr",
        "confidence": 0.7,
        "note": {
            "slug": "doc-test",
            "title": "Doc test",
            "tags": ["test", "wiki"],
            "doc_date": "",
            "summary": "Resume.",
            "sections": [{"heading": "Resume",
                          "markdown": body + " Voir {{E:ent-test}}."}],
            "warnings": [],
        },
        "entities": [
            {"slug": "ent-test", "name": "Ent Test", "kind": "entity",
             "subtype": "systeme", "aliases": [], "tags": ["test"],
             "definition": "Def.", "evidence": "preuve",
             "salience": "primary"},
        ],
        "relations": [],
        "issues": [],
    }
    d.update(kw)
    return d


def test_contract_version_v4():
    assert X.CONTRACT_VERSION == "wiki-extract-v4"
    assert X.TOKENIZER


def test_validate_accepte_valide():
    ok, errs, warns, doc = X.validate(_doc())
    assert ok, errs
    assert doc["note"]["doc_date"] is None


def test_validate_rejette_tags_vides():
    d = _doc()
    d["note"]["tags"] = []
    ok, errs, _, _ = X.validate(d)
    assert not ok and errs


def test_validate_rejette_corps_court():
    d = _doc()
    d["note"]["sections"] = [{"heading": "Resume", "markdown": "trop court"}]
    ok, errs, _, _ = X.validate(d)
    assert not ok
    assert any("200" in e for e in errs)


def test_validate_goutte_relation_pendante():
    d = _doc()
    d["note"]["sections"][0]["markdown"] = "Sans jeton. " * 30
    d["relations"] = [{"from": "ent-test", "to": "fantome", "type": "x",
                       "evidence": "e", "confidence": 2.5}]
    ok, _, warns, doc = X.validate(d)
    assert ok
    assert doc["relations"] == []
    assert any("pendante" in w for w in warns)


def test_chunking_deterministe():
    text = "# Titre\n\nParagraphe. " * 500
    a = X.split_document(text, 800, 64)
    b = X.split_document(text, 800, 64)
    assert a == b and len(a) > 1
    assert a[0][0] == 0 and a[-1][1] == len(text)


def test_tokenizer_local_sans_reseau(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    n, how = X.estimate_tokens_local("bonjour le monde")
    assert n > 0 and how in ("tiktoken-cl100k", "bytes-fallback")
    # Bloquer tout acces reseau : la mesure ne doit pas en avoir besoin.
    import socket

    def interdit(*a, **k):
        raise AssertionError("aucun reseau pendant la mesure")

    monkeypatch.setattr(socket, "create_connection", interdit)
    X.estimate_tokens_local("encore un texte de mesure")


def test_list_files_honore_exclusion(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    conv = raw / "assets" / "ConvIA" / "x"
    ana = raw / "assets" / "ConvIA-Analysis" / "y"
    conv.mkdir(parents=True)
    ana.mkdir(parents=True)
    (conv / "brut.md").write_text("# brut", encoding="utf-8")
    (ana / "analyse.md").write_text("# analyse", encoding="utf-8")
    (raw / "note.md").write_text("# note", encoding="utf-8")
    monkeypatch.setenv("RAW_DIR", str(raw))
    import importlib

    importlib.reload(X)
    try:
        fichiers = X.list_files(str(raw))
    finally:
        monkeypatch.undo()
        importlib.reload(X)
    noms = [Path(f).name for f in fichiers]
    assert "brut.md" not in noms  # raw ConvIA exclu
    assert "analyse.md" in noms and "note.md" in noms


def test_cli_print_schema():
    proc = subprocess.run([sys.executable, str(REPO / "llm_wiki_extract.py"),
                           "--print-schema"], capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 0
    schema = json.loads(proc.stdout)
    assert schema["required"] == ["language", "confidence", "note",
                                  "entities", "relations", "issues"]


def test_cli_extraction_refusee_sans_dry_run(tmp_path):
    f = tmp_path / "doc.md"
    f.write_text("# Doc\n\nContenu. " * 50, encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(REPO / "llm_wiki_extract.py"), "--file", str(f),
         "--manifest", "none"], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2
    assert "RETIRE" in proc.stderr or "retire" in proc.stderr


def test_cli_dry_run_sans_llm(tmp_path):
    f = tmp_path / "doc.md"
    f.write_text("# Doc\n\nContenu. " * 50, encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(REPO / "llm_wiki_extract.py"), "--file", str(f),
         "--manifest", "none", "--dry-run"], capture_output=True, text=True,
        timeout=60)
    assert proc.returncode == 0
    out = json.loads(proc.stdout)
    assert out["ok"] == 1 and out["contract_version"] == "wiki-extract-v4"


def test_cli_fichier_exclu_refuse(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    conv = raw / "assets" / "ConvIA" / "x"
    conv.mkdir(parents=True)
    f = conv / "brut.md"
    f.write_text("# Brut\n\nContenu. " * 50, encoding="utf-8")
    monkeypatch.setenv("RAW_DIR", str(raw))
    proc = subprocess.run(
        [sys.executable, str(REPO / "llm_wiki_extract.py"), "--file", str(f),
         "--manifest", "none", "--dry-run"], capture_output=True, text=True,
        timeout=60)
    assert proc.returncode == 0
    out = json.loads(proc.stdout)
    assert out["ok"] == 0
