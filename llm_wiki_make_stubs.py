#!/usr/bin/env python3
"""Cree une page d'amorcage pour chaque cible de wikilink sans page.

Aucun appel LLM : le contenu est extrait du vault lui-meme (fiches citantes et
phrase de la mention). Rien n'est invente. Chaque page porte `stub: true` pour
rester identifiable et supprimable en bloc.

Le nom de fichier doit correspondre EXACTEMENT au texte du lien : c'est ainsi
qu'Obsidian resout [[Fail2Ban]] vers Fail2Ban.md. On ne slugifie pas.
"""
import os
import re
import sys
import collections
from datetime import date

V = "/srv/obsidian-vault"
DRY = "--dry" in sys.argv
LINK = re.compile(r"\[\[([^\]\[|\n`]{1,120})(\|[^\]\n]*)?\]\]")
ILLEGAL = re.compile(r'[/\\:*?"<>|]')

existing = {}
for sub in ("sources", "entities", "concepts"):
    for f in os.listdir(os.path.join(V, "wiki", sub)):
        if f.endswith(".md"):
            existing[f[:-3]] = sub

# --- recolte des mentions : cible -> [(fiche citante, phrase)]
mentions = collections.defaultdict(list)
for sub in ("sources", "entities", "concepts"):
    d = os.path.join(V, "wiki", sub)
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".md"):
            continue
        txt = open(os.path.join(d, fn), encoding="utf-8", errors="replace").read()
        for m in LINK.finditer(txt):
            t = m.group(1).strip()
            if not t or t in existing:
                continue
            start = txt.rfind("\n", 0, m.start()) + 1
            end = txt.find("\n", m.end())
            line = txt[start:end if end > 0 else len(txt)].strip()
            line = re.sub(r"^[-*>#\s]+", "", line)
            mentions[t].append((sub + "/" + fn[:-3], line[:400]))


def classify(name):
    """entities pour un nom propre, un acronyme, un produit ou une adresse ;
    concepts pour une notion. Heuristique assumee : `stub: true` permet de
    recategoriser en bloc si le classement deplait."""
    if re.search(r"\d", name) or "." in name:
        return "entities"
    words = name.split()
    if len(words) <= 3 and any(w[:1].isupper() for w in words):
        return "entities"
    if name.isupper():
        return "entities"
    return "concepts"


created = collections.Counter()
skipped = []
for target, refs in sorted(mentions.items()):
    if ILLEGAL.search(target):
        skipped.append((target, "caractere interdit dans un nom de fichier"))
        continue
    sub = classify(target)
    path = os.path.join(V, "wiki", sub, target + ".md")
    if os.path.exists(path):
        continue

    seen, uniq = set(), []
    for src, line in refs:
        if src in seen:
            continue
        seen.add(src)
        uniq.append((src, line))

    body = []
    body.append("---")
    body.append('title: "%s"' % target.replace('"', "'"))
    body.append("type: %s" % ("entity" if sub == "entities" else "concept"))
    body.append("tags: [stub, a-enrichir]")
    body.append("stub: true")
    body.append("date_added: %s" % date.today().isoformat())
    body.append("last_updated: %s" % date.today().isoformat())
    body.append("---")
    body.append("")
    body.append("# %s" % target)
    body.append("")
    body.append("> [!info] Page d'amorçage")
    body.append("> Cette page a été créée automatiquement le %s parce que %d fiche(s) y"
                % (date.today().isoformat(), len(uniq)))
    body.append("> renvoient sans qu'elle existe. Son contenu se limite aux mentions")
    body.append("> relevées dans le vault : rien n'a été rédigé ni inventé.")
    body.append("> Elle sera enrichie lors d'une prochaine ingestion.")
    body.append("")
    body.append("## Mentions dans le wiki")
    body.append("")
    for src, line in uniq:
        name = src.split("/", 1)[1]
        body.append("- [[%s]]" % name)
        if line:
            body.append("  > %s" % line)
    body.append("")
    text = "\n".join(body) + "\n"

    if not DRY:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.chmod(path, 0o664)
    created[sub] += 1

print("%s : %d pages (entities %d, concepts %d)"
      % ("SIMULATION" if DRY else "CREE", sum(created.values()),
         created["entities"], created["concepts"]))
if skipped:
    print("ignorees (%d) :" % len(skipped))
    for t, why in skipped[:10]:
        print("   %-50s %s" % (t[:50], why))
