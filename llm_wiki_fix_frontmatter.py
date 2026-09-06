#!/usr/bin/env python3
"""Reparation DETERMINISTE du frontmatter du RAG llm-wiki.

Ce script n'invente aucun contenu. Il ne touche qu'a deux choses :
  1. le bloc frontmatter, en AJOUTANT des cles manquantes a la fin du bloc
     (l'ordre des cles existantes est preserve, aucune valeur existante n'est
     modifiee) ;
  2. une ligne de titre `# <title>` inseree en tete du corps, uniquement si le
     corps ne contient aucun titre Markdown (regle R14).

Ce qu'il REFUSE de faire, par construction :
  - inventer des `tags` ;
  - remplir un corps trop court (< 200 caracteres) ;
  - fabriquer un frontmatter a une fiche qui n'en a pas (releve de R8) ;
  - toucher aux pages d'amorcage `stub: true` ;
  - reecrire, reordonner ou reformuler le corps existant ;
  - toucher a index.md ou log.md.

Usage :
    llm_wiki_fix_frontmatter.py --dry      # rapport, aucune ecriture
    llm_wiki_fix_frontmatter.py --apply    # ecriture atomique par fichier
    llm_wiki_fix_frontmatter.py --dry --json
"""
import argparse
import json
import os
import re
import sys
import tempfile
from collections import Counter
from datetime import date, datetime

WIKI_DIR = "/srv/obsidian-vault"
SUBDIRS = ("sources", "entities", "concepts")
TYPE_OF_DIR = {"sources": "source", "entities": "entity", "concepts": "concept"}
# Cles obligatoires par R9 : les fiches derivees (entities/concepts) n'exigent
# que `tags`, que ce script n'ecrit jamais. Le perimetre de reparation du
# frontmatter se limite donc a wiki/sources/.
FIXABLE_KEYS = ("title", "type", "last_updated")
# Ordre de preference pour deduire last_updated. `ingested` figure dans la liste
# parce que 27 fiches du corpus le portent et qu'une date d'ingestion reelle
# vaut mieux qu'un mtime. Le mtime est le dernier recours : c'est une
# APPROXIMATION, pas une donnee d'origine.
DATE_KEYS = ("last_updated", "date_added", "date", "created_at", "ingested")
DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
HEADING_RE = re.compile(r"^#", re.M)
H1_RE = re.compile(r"^#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$", re.M)


def split_frontmatter(text):
    """Meme decoupage que le linter : YAML plat, cles de premier niveau.
    Retourne (fm, ordre_des_cles, index_ligne_fin, corps, erreur)."""
    if not text.startswith("---"):
        return None, [], None, text, "frontmatter absent"
    lines = text.split("\n")
    end = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end = i
            break
    if end is None:
        return None, [], None, text, "frontmatter non ferme"
    fm, order, last_key = {}, [], None
    for raw in lines[1:end]:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if raw.startswith((" ", "\t")) or raw.lstrip().startswith("-"):
            if last_key is not None:
                item = raw.strip().lstrip("-").strip().strip("\"'")
                if item:
                    prev = fm.get(last_key, "")
                    fm[last_key] = (prev + ", " + item) if prev else item
            continue
        if ":" not in raw:
            return None, [], None, "\n".join(lines[end + 1:]), \
                "ligne de frontmatter non parsable: %r" % raw[:60]
        key, _, val = raw.partition(":")
        last_key = key.strip()
        if last_key not in fm:
            order.append(last_key)
        fm[last_key] = val.strip().strip("\"'")
    return fm, order, end, "\n".join(lines[end + 1:]), None


def yaml_quote(value):
    """Scalaire double-quote, guillemets internes echappes."""
    return '"%s"' % value.replace("\\", "\\\\").replace('"', '\\"')


def valid_date(value):
    """Retourne AAAA-MM-JJ si la valeur est une date ISO valide et non future."""
    if not value:
        return None
    m = DATE_RE.match(value.strip())
    if not m:
        return None
    try:
        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None
    if d > date.today():
        return None
    return m.group(1)


def derive_title(fm, body, stem):
    """Titre : d'abord le frontmatter, puis le premier titre Markdown du corps,
    sinon le nom de fichier. Aucune invention."""
    t = (fm.get("title") or "").strip()
    if t:
        return t, "frontmatter"
    m = H1_RE.search(body)
    if m and m.group(1).strip():
        return m.group(1).strip(), "titre-h1"
    return stem, "nom-de-fichier"


def derive_last_updated(fm, path):
    for k in DATE_KEYS:
        v = valid_date(fm.get(k))
        if v:
            return v, k
    ts = datetime.fromtimestamp(os.path.getmtime(path)).date()
    if ts > date.today():           # garde-fou : jamais de date future
        ts = date.today()
    return ts.isoformat(), "mtime"


def read_preserving_newlines(path):
    """Lit le fichier SANS traduction de fin de ligne et retourne (texte_LF, style).

    41 fiches du vault sont en CRLF. Une lecture en mode texte ordinaire les
    traduit en LF et la reecriture les convertit en silence : le corps est alors
    modifie, ce qui est interdit. On memorise donc le style pour le restituer.
    """
    with open(path, "r", encoding="utf-8", errors="strict", newline="") as fh:
        raw = fh.read()
    if "\r\n" in raw:
        return raw.replace("\r\n", "\n"), "\r\n"
    if "\r" in raw:
        return raw, "\r"          # CR seul : non gere, la fiche sera ignoree
    return raw, "\n"


def plan_file(sub, name, path):
    """Retourne un dict decrivant les modifications a appliquer, ou None."""
    text, newline = read_preserving_newlines(path)
    if newline == "\r":
        return {"rel": "wiki/%s/%s" % (sub, name), "skip": "CR-seul"}
    fm, order, end, body, err = split_frontmatter(text)
    if err:
        return {"rel": "wiki/%s/%s" % (sub, name), "skip": "R8:" + err}
    if str(fm.get("stub", "")).strip().lower() == "true":
        return {"rel": "wiki/%s/%s" % (sub, name), "skip": "stub"}

    stem = name[:-3] if name.endswith(".md") else name
    lines = text.split("\n")
    added, sources = [], {}

    if sub == "sources":
        if not fm.get("type"):
            added.append(("type", TYPE_OF_DIR[sub]))       # jamais quote
            sources["type"] = "dossier"
        if not fm.get("title"):
            t, origin = derive_title(fm, body, stem)
            added.append(("title", yaml_quote(t)))
            sources["title"] = origin
        if not fm.get("last_updated"):
            v, origin = derive_last_updated(fm, path)
            added.append(("last_updated", v))
            sources["last_updated"] = origin

    # R14 : aucun titre Markdown dans un corps assez long -> inserer le titre.
    insert_title = None
    if len(body.strip()) >= 200 and not HEADING_RE.search(body):
        t, origin = derive_title(fm, body, stem)
        insert_title = t
        sources["h1"] = origin

    if not added and insert_title is None:
        return None

    new_lines = lines[:end] + ["%s: %s" % (k, v) for k, v in added] + lines[end:end + 1]
    rest = lines[end + 1:]
    if insert_title is not None:
        head = ["# " + insert_title]
        if not (rest and rest[0].strip() == ""):
            head.append("")
        rest = head + rest
    new_text = "\n".join(new_lines + rest)

    _, _, _, new_body, _ = split_frontmatter(new_text)
    return {
        "rel": "wiki/%s/%s" % (sub, name),
        "path": path,
        "added": [k for k, _ in added],
        "added_pairs": ["%s: %s" % (k, v) for k, v in added],
        "insert_title": insert_title,
        "sources": sources,
        "body_len_before": len(body),
        "body_len_after": len(new_body),
        "newline": "CRLF" if newline == "\r\n" else "LF",
        "new_text": new_text if newline == "\n" else new_text.replace("\n", newline),
    }


def atomic_write(path, text):
    """Ecriture atomique preservant proprietaire, groupe et mode."""
    st = os.stat(path)
    d = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".fixfm-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        os.chmod(tmp, st.st_mode & 0o7777)
        try:
            os.chown(tmp, st.st_uid, st.st_gid)
        except PermissionError:
            pass
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry", action="store_true", help="rapport seul, aucune ecriture")
    g.add_argument("--apply", action="store_true", help="applique les corrections")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    plans, skips = [], Counter()
    skip_list = []
    for sub in SUBDIRS:
        d = os.path.join(WIKI_DIR, "wiki", sub)
        if not os.path.isdir(d):
            continue
        for name in sorted(os.listdir(d)):
            p = os.path.join(d, name)
            if not os.path.isfile(p) or not name.endswith(".md"):
                continue
            try:
                pl = plan_file(sub, name, p)
            except Exception as exc:              # fichier illisible : R8
                skips["illisible"] += 1
                skip_list.append("wiki/%s/%s : %s" % (sub, name, exc))
                continue
            if pl is None:
                continue
            if pl.get("skip"):
                skips[pl["skip"].split(":")[0]] += 1
                if pl["skip"].startswith("R8"):
                    skip_list.append("%s : %s" % (pl["rel"], pl["skip"]))
                continue
            plans.append(pl)

    by_key = Counter()
    by_origin = Counter()
    for pl in plans:
        for k in pl["added"]:
            by_key[k] += 1
        if pl["insert_title"] is not None:
            by_key["#titre-corps"] += 1
        for k, v in pl["sources"].items():
            by_origin["%s<-%s" % (k, v)] += 1
        by_key["(fin-de-ligne %s preservee)" % pl["newline"]] += 1

    written = 0
    if args.apply:
        for pl in plans:
            atomic_write(pl["path"], pl["new_text"])
            written += 1

    if args.json:
        print(json.dumps({
            "mode": "apply" if args.apply else "dry",
            "fichiers_concernes": len(plans),
            "ecrits": written,
            "par_correction": dict(by_key),
            "par_origine": dict(by_origin),
            "ignores": dict(skips),
            "ignores_detail": skip_list,
            "fichiers": [{"rel": p["rel"], "added": p["added_pairs"],
                          "titre_insere": p["insert_title"],
                          "corps_avant": p["body_len_before"],
                          "corps_apres": p["body_len_after"]} for p in plans],
        }, ensure_ascii=False, indent=1))
        return 0

    print("mode          : %s" % ("APPLY" if args.apply else "DRY"))
    print("fiches a corriger : %d   (ecrites : %d)" % (len(plans), written))
    print("\ncorrections par cle :")
    for k, v in sorted(by_key.items()):
        print("  %-14s %d" % (k, v))
    print("\norigine des valeurs :")
    for k, v in sorted(by_origin.items()):
        print("  %-26s %d" % (k, v))
    print("\nignores :")
    for k, v in sorted(skips.items()):
        print("  %-14s %d" % (k, v))
    for s in skip_list:
        print("    ! %s" % s)
    delta = [p for p in plans
             if p["insert_title"] is None and p["body_len_before"] != p["body_len_after"]]
    print("\ncorps modifies hors insertion de titre : %d (doit valoir 0)" % len(delta))
    return 0


if __name__ == "__main__":
    sys.exit(main())
