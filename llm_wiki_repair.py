#!/usr/bin/env python3
"""Reparations ciblees du vault. Deterministe, aucun contenu invente.

A. `hermes-agent-masterclass-guide-complet.md` : fiche tronquee a l'ingestion du
   2026-07-06 (267 octets, frontmatter coupe en plein `links:`, marqueur
   `...[truncated]` ecrit tel quel). On ferme le YAML et on signale la perte.
B. Deux fiches en 640 llmingest:llmwiki, illisibles par `llmlint` : passage en 664.
C. Wikilinks malformes contenant de la syntaxe shell (`[[! -r "$X"]]`) : convertis
   en code inline. Ce ne sont pas des liens, ils n'ont jamais eu de cible.
D. `[[shadcn/ui]]` : le `/` est interdit dans un nom de fichier. Lien transforme en
   `[[shadcn-ui|shadcn/ui]]`, avec creation de la fiche cible.
E. `index.md` renvoie `[[NotebookLM]]` alors que la fiche s'appelle `notebooklm.md`
   et que les 4 liens du corpus pointent vers `notebooklm`. On aligne l'index.
"""
import os
import re
import sys
from datetime import date

V = "/srv/obsidian-vault"
DRY = "--dry" in sys.argv
report = []


def w(path, text):
    if DRY:
        return
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)


def r(path):
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        return fh.read()


# ------------------------------------------------------------------ A
p = os.path.join(V, "wiki/sources/hermes-agent-masterclass-guide-complet.md")
t = r(p)
if not t.rstrip().endswith("---") and "[truncated]" in t:
    lines = [l for l in t.split("\n") if l.strip()]
    lines = [l for l in lines if not l.startswith("links:")]
    lines.append('links: ["https://x.com/akshay_pachaar/status/205456"]')
    lines.append("---")
    lines.append("")
    lines.append("# Hermes Agent Masterclass — Guide Complet Architecture et Configuration")
    lines.append("")
    lines.append("> [!warning] Fiche tronquée à l'ingestion")
    lines.append("> Cette fiche a été écrite de façon incomplète le 2026-07-06 : le fichier")
    lines.append("> s'arrêtait au milieu du champ `links:`, avec le marqueur `...[truncated]`")
    lines.append("> écrit littéralement, et sans corps. Le contenu d'origine est perdu.")
    lines.append("> La source est une URL externe et non un document de `raw/` : la fiche")
    lines.append("> devra être ré-ingérée depuis le lien ci-dessus.")
    lines.append("")
    w(p, "\n".join(lines) + "\n")
    report.append("A. hermes-agent-masterclass : frontmatter ferme, perte signalee")

# ------------------------------------------------------------------ B
for name in ("tiktok_7648294886143184160.md", "twitter_2049468163113300226.md"):
    fp = os.path.join(V, "wiki/sources", name)
    # `llmlint` n'est ni proprietaire ni dans `llmwiki` : seul le bit « autres »
    # lui donne la lecture. 640 ne suffit pas, il faut 664.
    if os.path.exists(fp) and (os.stat(fp).st_mode & 0o004) == 0:
        if not DRY:
            os.chmod(fp, 0o664)
        report.append("B. %s : 640 -> 664" % name)

# ------------------------------------------------------------------ C
SHELLISH = re.compile(r'^\s*(?:[-!$:]|\d+\s|\w+\s+-(?:eq|ne|lt|gt|z|n|r|s|f|d)\b)'
                      r'|["\']|&&|\|\||-eq|-ne|\$\{|\$\(')
LINK = re.compile(r"\[\[([^\]\[|\n]{1,120})(\|[^\]\n]*)?\]\]")

# Garde-fou : ne jamais convertir un lien qui atteint une fiche reelle. Des pages
# legitimes commencent par un tiret (`-existing-code-.md`) et seraient sinon
# prises pour de la syntaxe shell.
EXISTING = set()
for _sub in ("sources", "entities", "concepts"):
    for _f in os.listdir(os.path.join(V, "wiki", _sub)):
        if _f.endswith(".md"):
            EXISTING.add(_f[:-3])

n_shell = 0
for sub in ("sources", "entities", "concepts"):
    d = os.path.join(V, "wiki", sub)
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".md"):
            continue
        fp = os.path.join(d, fn)
        txt = r(fp)
        cnt = [0]

        def rep(m):
            target = m.group(1)
            if target == "shadcn/ui":
                cnt[0] += 1
                return "[[shadcn-ui|shadcn/ui]]"
            if target in EXISTING or target.strip() in EXISTING:
                return m.group(0)           # lien valide : on n'y touche pas
            if "/" in target and " " not in target:
                return m.group(0)
            if SHELLISH.search(target) and not re.match(r"^[A-Za-zÀ-ÿ0-9]", target):
                cnt[0] += 1
                return "`%s`" % target
            return m.group(0)

        new = LINK.sub(rep, txt)
        if cnt[0]:
            w(fp, new)
            n_shell += cnt[0]
if n_shell:
    report.append("C/D. %d wikilinks malformes convertis (code inline ou alias)" % n_shell)

# ------------------------------------------------------------------ D (fiche cible)
sp = os.path.join(V, "wiki/entities/shadcn-ui.md")
if not os.path.exists(sp):
    body = [
        "---",
        'title: "shadcn/ui"',
        "type: entity",
        "tags: [stub, a-enrichir, frontend]",
        "stub: true",
        "date_added: %s" % date.today().isoformat(),
        "last_updated: %s" % date.today().isoformat(),
        "---",
        "",
        "# shadcn/ui",
        "",
        "> [!info] Page d'amorçage",
        "> Créée le %s parce que le wiki y renvoyait sans qu'elle existe." % date.today().isoformat(),
        "> Le nom d'origine contient une barre oblique, interdite dans un nom de",
        "> fichier : les liens l'atteignent via `[[shadcn-ui|shadcn/ui]]`.",
        "",
        "## Mentions dans le wiki",
        "",
        "- [[alignement-resolution-conflits-branches-git-cyna]]",
        "- [[recap-frontend-cyna-react]]",
        "",
    ]
    w(sp, "\n".join(body))
    if not DRY:
        os.chmod(sp, 0o664)
    report.append("D. wiki/entities/shadcn-ui.md cree")

# ------------------------------------------------------------------ E
ip = os.path.join(V, "index.md")
it = r(ip)
if "[[NotebookLM]]" in it:
    w(ip, it.replace("[[NotebookLM]]", "[[notebooklm]]"))
    report.append("E. index.md : [[NotebookLM]] -> [[notebooklm]]")

print("SIMULATION" if DRY else "APPLIQUE")
for line in report:
    print("  " + line)
if not report:
    print("  rien a faire")
