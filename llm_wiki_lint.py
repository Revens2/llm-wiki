#!/usr/bin/env python3
"""Audit du RAG llm-wiki - STRICTEMENT EN LECTURE SEULE.

Ce script n'ouvre aucun fichier du RAG en ecriture, ne renomme rien, ne supprime
rien. La garantie ne repose pas sur cette promesse : le service qui l'execute
remonte /srv/llm-wiki en lecture seule dans son namespace (ReadOnlyPaths), et
toute tentative d'ecriture echoue en EROFS au niveau du noyau.

Regles R1 a R24 - voir /srv/docs/plan.md section 10.
Sortie : rapport Markdown sur stdout, ou JSON avec --json.
"""

import argparse
import difflib
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime

WIKI_DIR = "/srv/obsidian-vault"
SUBDIRS = ("sources", "entities", "concepts")
# `source-summary` est une variante etablie : 27 fiches du corpus l'emploient.
TYPE_OF_DIR = {"sources": "source", "entities": "entity", "concepts": "concept"}
TYPE_ALIASES = {"source": {"source", "source-summary"},
                "entity": {"entity"}, "concept": {"concept"}}
# Cles relevees sur le corpus reel (1946 sources, 283 entites) : les sources portent
# title/type/tags/last_updated/links ; les entites, tags seul. Aucune trace de
# original_file / created_at / summary dans ce vault.
REQUIRED_KEYS = ("title", "type", "tags", "last_updated")
REQUIRED_KEYS_DERIVED = ("tags",)
# Convention reelle : slug kebab-case. Aucune des 1946 fiches n'est au format
# YYYY-MM-DD-slug prescrit par .hermes/commands/ingest.md : on suit le corpus.
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*\.md$")
# Un wikilink ne contient ni saut de ligne, ni crochet, ni backtick, et reste court.
# Sans ces bornes, la regex agrafe les sequences d'echappement ANSI presentes dans
# les fiches (`^[[I`, `^[[O`) et fabrique des dizaines de faux liens casses.
WIKILINK_RE = re.compile(r"\[\[([^\]\[|\n`]{1,120})(?:\|[^\]\n]*)?\]\]")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
# Corps minimal attendu, par sous-dossier. Calibre sur le corpus : le minimum
# observe cote sources vaut exactement 200, contre 88 cote entities. Un seuil
# unique condamnait 57 definitions courtes parfaitement legitimes.
MIN_BODY = {"sources": 200, "entities": 120, "concepts": 120}
# Redirection de fusion : pointeur volontaire, pas une fiche tronquee.
REDIRECT_RE = re.compile(r"page fusionn|^Voir \[\[.{1,80}\]\]", re.M)
HEARTBEAT = "/var/lib/llm-wiki/.poll-heartbeat"
HEARTBEAT_MAX_AGE = 15 * 60

INJECTION_PATTERNS = [
    (r"ignore\s+(the\s+)?(previous|above|prior)", "consigne d'ignorer les instructions"),
    (r"disregard\s+(the\s+)?(previous|above|all)", "consigne de faire abstraction"),
    # "system prompt" seul est ecarte : ce vault documente des agents IA, le terme
    # y est banal. Ne restent que les formulations imperatives.
    (r"(ignore|oublie|remplace)[^.\n]{0,20}(ton|le|the)\s+system\s+prompt", "detournement du prompt systeme"),
    (r"tu\s+dois\s+ex[eé]cuter", "injonction d'execution"),
    (r"run\s+the\s+following", "injonction d'execution"),
    (r"curl\s+[^\s]+\s*\|\s*(ba)?sh", "pipe curl vers shell"),
    (r"rm\s+-rf\s+/", "commande destructrice"),
    # `<script>` seul est ecarte : ce vault documente du developpement web.
    # Ne restent que les formes exfiltrantes.
    (r"<(?:script|iframe)[^>]{0,200}src\s*=\s*[\"\']?(?:http://|//)?\d{1,3}(?:\.\d{1,3}){3}",
     "balise pointant vers une IP nue"),
    (r"<(?:script|iframe)[^>]{0,200}src\s*=\s*[\"\']?javascript:", "URL javascript:"),
    (r"\bjavascript:\s*(?:eval|fetch|document\.cookie)", "javascript: actif"),
]

SEVERITIES = ("CRITIQUE", "MAJEUR", "MINEUR", "INFO")


class Finding:
    __slots__ = ("severity", "rule", "location", "observed", "fix")

    def __init__(self, severity, rule, location, observed, fix):
        self.severity = severity
        self.rule = rule
        self.location = location
        self.observed = observed
        self.fix = fix

    def as_dict(self):
        return {
            "severity": self.severity,
            "rule": self.rule,
            "location": self.location,
            "observed": self.observed,
            "fix": self.fix,
        }


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except OSError:
        return None


def split_frontmatter(text):
    """Retourne (dict_frontmatter, corps, erreur). Parseur YAML minimal : le
    frontmatter du RAG est plat (cle: valeur), inutile d'importer PyYAML."""
    if not text.startswith("---"):
        return None, text, "frontmatter absent"
    lines = text.split("\n")
    end = None
    for i in range(1, len(lines)):
        st = lines[i].strip()
        if st == "---":
            end = i
            break
        # fermeture collee en fin de ligne (`cle = [...] ---`), vue dans le corpus
        if st.endswith(" ---"):
            lines[i] = lines[i].rsplit("---", 1)[0].rstrip()
            lines.insert(i + 1, "---")
            end = i + 1
            break
    if end is None:
        return None, text, "frontmatter non ferme"
    fm = {}
    last_key = None
    for raw in lines[1:end]:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        # element d'une liste en bloc ("  - valeur") : rattache a la cle precedente
        if raw.startswith((" ", "\t")) or raw.lstrip().startswith("-"):
            if last_key is not None:
                item = raw.strip().lstrip("-").strip().strip("\"'")
                if item:
                    prev = fm.get(last_key, "")
                    fm[last_key] = (prev + ", " + item) if prev else item
            continue
        if ":" not in raw and "=" not in raw:
            return None, "\n".join(lines[end + 1:]), "ligne de frontmatter non parsable: %r" % raw[:60]
        sep = ":" if ":" in raw else "="
        key, _, val = raw.partition(sep)
        last_key = key.strip()
        fm[last_key] = val.strip().strip("\"'")
    return fm, "\n".join(lines[end + 1:]), None


def slugify(t):
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    return re.sub(r"-+", "-", re.sub(r"[^a-zA-Z0-9]+", "-", t).lower()).strip("-")


CODE_FENCE_RE = re.compile(r"^[ \t]*(?:```|~~~).*?^[ \t]*(?:```|~~~)[ \t]*$",
                           re.S | re.M)
CODE_INLINE_RE = re.compile(r"`[^`\n]*`")
INDENTED_RE = re.compile(r"(?m)^(?: {4}|\t).*$")


def strip_code(text):
    """Retire blocs clotures, code indente et code inline.

    Sans cela, `[[ -z "$TOKEN" ]]` passe pour un wikilink casse et un
    `<script>` cite dans un tutoriel passe pour une injection. On remplace par
    des sauts de ligne pour conserver la numerotation des lignes.
    """
    def blank(m):
        return "\n" * m.group(0).count("\n")
    text = CODE_FENCE_RE.sub(blank, text)
    text = INDENTED_RE.sub("", text)
    return CODE_INLINE_RE.sub("", text)


def looks_french(body):
    """Heuristique volontairement grossiere : on cherche des mots outils francais
    frequents. Un faux positif est en MINEUR, jamais bloquant."""
    sample = body.lower()
    hits = sum(1 for w in (" le ", " la ", " les ", " des ", " une ", " est ", " qui ",
                           " dans ", " pour ", " avec ", " sur ", " par ") if w in sample)
    return hits >= 3


def collect_pages():
    pages = {}
    for sub in SUBDIRS:
        d = os.path.join(WIKI_DIR, "wiki", sub)
        if not os.path.isdir(d):
            continue
        for name in sorted(os.listdir(d)):
            p = os.path.join(d, name)
            if os.path.isfile(p):
                pages[(sub, name)] = p
    return pages


def load_manifest():
    path = os.path.join(WIKI_DIR, ".ingested_manifest.jsonl")
    state, produced = {}, set()
    if not os.path.isfile(path):
        return state, produced
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            state[rec.get("path", "")] = rec
            for f in rec.get("produced") or []:
                produced.add(f)
    return state, produced


def parse_index():
    """Retourne {section: [noms]} depuis index.md."""
    path = os.path.join(WIKI_DIR, "index.md")
    text = read_text(path)
    sections = defaultdict(list)
    if text is None:
        return sections
    current = None
    for line in text.split("\n"):
        if line.startswith("## "):
            current = line[3:].strip().lower()
        else:
            m = WIKILINK_RE.search(line)
            if m and current:
                sections[current].append(m.group(1).strip())
    return sections


def run_lint():
    findings = []
    add = lambda *a: findings.append(Finding(*a))

    pages = collect_pages()
    manifest_state, manifest_produced = load_manifest()
    index = parse_index()
    index_all = {n for names in index.values() for n in names}

    stems = {}          # nom sans extension -> [(sub, name)]
    prose = {}          # corps hors blocs de code
    meta = {}           # (sub, name) -> (fm, body)
    # Pages d'amorcage : frontmatter `stub: true`. Creees automatiquement parce
    # qu'une fiche y renvoie sans qu'elles existent. Exemptees de R14 (corps
    # court), R6 (citee par personne) et R17 (langue), sans objet pour une
    # amorce, et signalees par R24 pour rester visibles en INFO.
    stub_keys = set()
    for key, path in pages.items():
        sub, name = key
        stems.setdefault(name[:-3] if name.endswith(".md") else name, []).append(key)
        text = read_text(path)
        if text is None:
            add("CRITIQUE", "R8", "wiki/%s/%s" % (sub, name), "fichier illisible",
                "verifier les droits et l'encodage")
            continue
        fm, body, ferr = split_frontmatter(text)
        meta[key] = (fm, body)
        prose[key] = strip_code(body)
        rel = "wiki/%s/%s" % (sub, name)

        # R7 - nom de fichier. Deux cas distincts : un tiret initial est cosmetique
        # et concerne 650 fiches heritees ; une majuscule, un espace ou un accent
        # cassent les wikilinks, c'est actionnable.
        if not NAME_RE.match(name):
            if re.match(r"^-[a-z0-9._-]*\.md$", name):
                add("INFO", "R7", rel, "nom commencant par un tiret",
                    "cosmetique : le slug d'origine debutait par un caractere non alphanumerique")
            else:
                add("INFO", "R7", rel, "nom hors motif kebab-case (majuscule, espace ou accent)",
                    "ne PAS renommer sans reecrire les liens : Obsidian resout [[X]] vers X.md")

        # R8 - frontmatter present et parsable
        if ferr:
            add("CRITIQUE", "R8", rel, ferr,
                "ajouter un frontmatter delimite par --- avec les cles obligatoires")
            continue

        is_stub = str(fm.get("stub", "")).strip().strip('"').lower() == "true"
        if is_stub:
            stub_keys.add(key)
            add("INFO", "R24", rel, "page d'amorcage en attente d'enrichissement",
                "enrichir la fiche puis retirer stub: true et le tag a-enrichir")
        # R9 - cles obligatoires. Le jeu depend du type : original_file n'a de sens
        # que pour une fiche source, qui derive d'un document de raw/. Les entites
        # et concepts sont deduits de plusieurs sources, ils n'en ont pas.
        required = REQUIRED_KEYS if sub == "sources" else REQUIRED_KEYS_DERIVED
        for k in required:
            if k not in fm or not fm[k]:
                add("MAJEUR", "R9", rel, "cle '%s' absente ou vide" % k,
                    "renseigner '%s' dans le frontmatter" % k)
        if sub != "sources" and not fm.get("title"):
            add("INFO", "R9", rel, "cle 'title' absente (recommandee)",
                "ajouter un title explicite au frontmatter")

        # R10 - cles en anglais (detection des cles francaises courantes)
        for k in fm:
            if k in ("titre", "resume", "resume_", "type_", "cree_le", "etiquettes", "fichier_source"):
                add("MINEUR", "R10", rel, "cle de frontmatter en francais : '%s'" % k,
                    "utiliser la cle anglaise correspondante")

        # R11 - type coherent avec le dossier
        expected = TYPE_OF_DIR[sub]
        if fm.get("type") and fm["type"] not in TYPE_ALIASES[expected]:
            add("MAJEUR", "R11", rel, "type='%s' alors que la fiche est dans %s/" % (fm["type"], sub),
                "corriger type en '%s' ou deplacer la fiche" % expected)

        # R12 - dates valides et non futures
        for k in ("last_updated", "date_added", "created_at"):
            v = fm.get(k)
            if not v:
                continue
            if not DATE_RE.match(v):
                add("MAJEUR", "R12", rel, "%s='%s' n'est pas au format AAAA-MM-JJ" % (k, v),
                    "reecrire la date au format ISO")
                continue
            try:
                d = datetime.strptime(v[:10], "%Y-%m-%d").date()
                if d > date.today():
                    add("MAJEUR", "R12", rel, "%s='%s' est dans le futur" % (k, v),
                        "corriger la date")
            except ValueError:
                add("MAJEUR", "R12", rel, "%s='%s' n'est pas une date valide" % (k, v),
                    "corriger la date")

        # R13 - original_file coherent avec le manifeste
        orig = fm.get("original_file")
        if orig:
            known = any(os.path.basename(p) == os.path.basename(orig) for p in manifest_state)
            if not known:
                add("MINEUR", "R13", rel, "original_file='%s' inconnu du manifeste" % orig,
                    "verifier la provenance de la fiche")

        # R14 - fiche vide ou tronquee (sans objet pour une page d'amorcage)
        floor = MIN_BODY.get(sub, 200)
        if REDIRECT_RE.search(body[:300]):
            floor = 0
        if is_stub or not floor:
            pass
        elif len(body.strip()) < floor:
            add("MAJEUR", "R14", rel,
                "corps de %d caracteres (< %d attendus pour %s)"
                % (len(body.strip()), floor, sub),
                "regenerer la fiche : contenu insuffisant ou tronque")
        elif not re.search(r"^#", body, re.M):
            add("MAJEUR", "R14", rel, "aucun titre Markdown dans le corps",
                "ajouter au moins un titre de section")

        # R17 - contenu en francais
        if not is_stub and len(body.strip()) >= max(floor, 200) and not looks_french(body):
            add("MINEUR", "R17", rel, "le corps ne semble pas redige en francais",
                "reecrire le contenu en francais (regle 2 du prompt d'ingestion)")

        # R21 - motifs d'injection persistes dans une fiche
        # Le titre est derive du nom de fichier brut, souvent un fragment de
        # conversation. Le scanner ferait passer le SUJET de la fiche pour une
        # instruction adressee a l'agent.
        scan = prose[key]
        _title = (fm or {}).get("title", "").strip()
        if _title:
            scan = "\n".join(
                "" if (_title.lower() in ln.lower() or ln.strip().lstrip("# ").lower() == _title.lower())
                else ln
                for ln in scan.split("\n"))
        for pat, label in INJECTION_PATTERNS:
            m = re.search(pat, scan, re.I)
            if m:
                # Le corps commence apres le frontmatter : sans ce decalage,
                # la ligne annoncee ne correspond pas a celle du fichier.
                offset = text[:len(text) - len(body)].count("\n")
                line_no = scan[:m.start()].count("\n") + 1 + offset
                add("CRITIQUE", "R21", "%s:%d" % (rel, line_no),
                    "motif d'injection detecte (%s) : %r" % (label, m.group(0)[:60]),
                    "verifier le document source ; ne pas exposer cette fiche au RAG avant revue")

    # R1 - index -> disque
    stem_set = set(stems)
    for section, names in index.items():
        for n in names:
            if n not in stem_set:
                add("CRITIQUE", "R1", "index.md (## %s)" % section,
                    "'%s' reference dans l'index mais absent de wiki/" % n,
                    "supprimer l'entree de l'index ou restaurer la fiche")

    # R2 - orphelines : fiche sur disque absente de l'index
    for stem, keys in stems.items():
        if stem not in index_all:
            sub, name = keys[0]
            if (meta.get((sub, name), ({}, ""))[0] or {}).get("stub"):
                continue
            add("MAJEUR", "R2", "wiki/%s/%s" % (sub, name),
                "fiche absente de index.md (orpheline)",
                "relancer l'ingestion : index.md est regenere en fin de run")

    # R3/R4/R5/R6 - liens
    cited = set()
    for key, (fm, body) in meta.items():
        sub, name = key
        rel = "wiki/%s/%s" % (sub, name)
        links = WIKILINK_RE.findall(prose.get(key, body) or "")
        if not links:
            add("MINEUR", "R5", rel, "aucun lien sortant (cul-de-sac)",
                "relier la fiche a au moins une entite ou un concept")
        for target in links:
            t = target.strip()
            cited.add(t)
            if t not in stem_set:
                add("MAJEUR", "R3", rel, "lien casse : [[%s]]" % t,
                    "creer la fiche cible ou corriger le lien")
            elif len(stems.get(t, [])) > 1:
                dirs = ", ".join(s for s, _ in stems[t])
                add("MINEUR", "R4", rel, "lien ambigu : [[%s]] existe dans %s" % (t, dirs),
                    "desambiguiser le nom de fiche")
    for stem, keys in stems.items():
        if stem not in cited and not any(k in stub_keys for k in keys):
            sub, name = keys[0]
            add("INFO", "R6", "wiki/%s/%s" % (sub, name),
                "fiche citee par aucune autre",
                "verifier son rattachement au corpus")

    # R15 - doublons de contenu.
    # Comparer 2571 fiches deux a deux fait 3,3 M de comparaisons et prend ~5 min.
    # On regroupe d'abord par tranche de longueur : deux fiches de tailles tres
    # differentes ne peuvent pas etre des doublons, la comparaison est inutile.
    buckets = defaultdict(list)
    for k in meta:
        b = meta[k][1] or ""
        if len(b) >= 200:
            buckets[len(b) // 500].append((k, b))
    for bucket_id in list(buckets):
        items = buckets[bucket_id] + buckets.get(bucket_id + 1, [])
        if len(items) > 120:            # garde-fou : tranche trop peuplee
            items = items[:120]
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                (k1, b1), (k2, b2) = items[i], items[j]
                if k1 == k2 or abs(len(b1) - len(b2)) > max(len(b1), len(b2)) * 0.25:
                    continue
                if difflib.SequenceMatcher(None, b1[:3000], b2[:3000]).quick_ratio() <= 0.92:
                    continue
                ratio = difflib.SequenceMatcher(None, b1[:3000], b2[:3000]).ratio()
                if ratio > 0.92:
                    add("MINEUR", "R15", "wiki/%s/%s" % k1,
                        "contenu quasi identique a wiki/%s/%s (%.0f%%)" % (k2[0], k2[1], ratio * 100),
                        "fusionner les deux fiches ou differencier leur contenu")

    # R16 - comptages de l'index
    for sub in SUBDIRS:
        on_disk = sum(1 for (s, _) in pages if s == sub)
        in_index = len(index.get(sub, []))
        if on_disk != in_index:
            add("INFO", "R16", "index.md (## %s)" % sub,
                "%d fiches sur disque, %d dans l'index" % (on_disk, in_index),
                "regenerer index.md")

    # R18 - fiche produite hors pipeline.
    # N'a de sens que si le manifeste couvre deja l'essentiel du corpus. Le vault
    # reel a ete construit avant la mise en place du manifeste : signaler ses 2571
    # fiches comme "hors pipeline" serait exact mais inutile, et noierait le reste.
    if manifest_produced and len(manifest_produced) >= 0.2 * max(1, len(pages)):
        for (sub, name) in pages:
            rel = "wiki/%s/%s" % (sub, name)
            if rel not in manifest_produced:
                add("MAJEUR", "R18", rel,
                    "fiche absente des 'produced' du manifeste (produite hors pipeline)",
                    "verifier son origine : run interrompu avant promotion, ou ecriture manuelle")

    # R22 - terme mis en avant dans plusieurs fiches sans page dediee.
    # Demande explicitement par .hermes/commands/lint.md :
    # "concepts mentioned without their own page".
    mention_re = re.compile(r"\*\*([A-Z][\w .+-]{3,40})\*\*")
    mentions = Counter()
    for key, (fm, body) in meta.items():
        for m in set(mention_re.findall(prose.get(key, body) or "")):
            mentions[m.strip()] += 1
    for term, n in mentions.items():
        if n < 3 or slugify(term) in stem_set or term in stem_set:
            continue
        add("MINEUR", "R22", "wiki/",
            "'%s' mis en avant dans %d fiches sans page dediee" % (term, n),
            "creer wiki/concepts/%s.md ou wiki/entities/%s.md" % (slugify(term), slugify(term)))

    # R23 - reference croisee manquante : plusieurs fiches citent la meme page
    # tierce sans jamais se citer entre elles. "missing cross-references".
    inbound = defaultdict(set)
    for key, (fm, body) in meta.items():
        for t in set(WIKILINK_RE.findall(prose.get(key, body) or "")):
            inbound[t.strip()].add(key)
    for target, srcs_set in inbound.items():
        if len(srcs_set) < 4 or target not in stem_set:
            continue
        pair = sorted(srcs_set)[:2]
        a, b = pair[0], pair[1]
        ba = meta.get(a, (None, ""))[1] or ""
        bb = meta.get(b, (None, ""))[1] or ""
        na = a[1][:-3] if a[1].endswith(".md") else a[1]
        nb = b[1][:-3] if b[1].endswith(".md") else b[1]
        if nb not in ba and na not in bb:
            add("INFO", "R23", "wiki/%s/%s" % a,
                "partage [[%s]] avec wiki/%s/%s sans lien reciproque" % (target, b[0], b[1]),
                "ajouter une reference croisee entre les deux fiches")

    # R19 - staging orphelin
    staging = os.path.join(WIKI_DIR, ".staging")
    try:
        residual = [e for e in os.listdir(staging) if not e.startswith(".")]
    except OSError:
        residual = []
    if residual:
        add("MINEUR", "R19", ".staging/",
            "%d repertoire(s) de staging residuel(s) : %s" % (len(residual), ", ".join(residual[:3])),
            "un run a ete interrompu ; supprimer apres verification")

    # R20 - heartbeat du poller
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT)
        if age > HEARTBEAT_MAX_AGE:
            add("CRITIQUE", "R20", HEARTBEAT,
                "heartbeat vieux de %d min : la scrutation est peut-etre morte" % (age // 60),
                "systemctl status llm-wiki-poll.timer")
    except OSError:
        add("CRITIQUE", "R20", HEARTBEAT, "heartbeat absent : le poller n'a jamais tourne",
            "systemctl enable --now llm-wiki-poll.timer")

    return findings, pages, manifest_state


def ingest_summary(manifest_state):
    counts = defaultdict(int)
    for rec in manifest_state.values():
        counts[rec.get("status", "?")] += 1
    lines = ["- traites ok : %d" % counts["ok"],
             "- echecs : %d" % counts["failed"],
             "- abandonnes : %d" % counts["skipped"]]
    for f, label in (("/var/lib/llm-wiki/ingest-due-at", "reprise planifiee"),
                     ("/var/lib/llm-wiki/lint-due-at", "prochain lint")):
        try:
            with open(f) as fh:
                ts = int(fh.read().strip())
            lines.append("- %s : %s UTC" % (label, time.strftime("%F %H:%M", time.gmtime(ts))))
        except (OSError, ValueError):
            lines.append("- %s : aucune" % label)
    return lines


def render(findings, pages, manifest_state):
    by_sev = defaultdict(list)
    for f in findings:
        by_sev[f.severity].append(f)
    out = []
    out.append("# Audit RAG llm-wiki — %s UTC" % time.strftime("%F %H:%M"))
    out.append("")
    out.append("Lecture seule. Aucun fichier n'a ete modifie, renomme ni supprime.")
    out.append("")
    out.append("## Synthese")
    out.append("")
    out.append("- fiches auditees : %d" % len(pages))
    for s in SEVERITIES:
        out.append("- %s : %d" % (s, len(by_sev[s])))
    out.append("")
    out.append("### Etat de l'ingestion")
    out.append("")
    out.extend(ingest_summary(manifest_state))
    out.append("")
    for s in SEVERITIES:
        if not by_sev[s]:
            continue
        out.append("## %s (%d)" % (s, len(by_sev[s])))
        out.append("")
        out.append("| Regle | Emplacement | Constat | Correction proposee |")
        out.append("|---|---|---|---|")
        for f in sorted(by_sev[s], key=lambda x: (x.rule, x.location)):
            esc = lambda t: str(t).replace("|", "\\|").replace("\n", " ")
            out.append("| %s | `%s` | %s | %s |" % (f.rule, esc(f.location), esc(f.observed), esc(f.fix)))
        out.append("")
    out.append("---")
    out.append("")
    out.append("Aucune correction n'est appliquee automatiquement : toute modification")
    out.append("du RAG demande une confirmation explicite.")
    return "\n".join(out)


def main():
    global WIKI_DIR
    ap = argparse.ArgumentParser(description="Audit du RAG llm-wiki (lecture seule)")
    ap.add_argument("--json", action="store_true", help="sortie JSON")
    ap.add_argument("--wiki-dir", default=WIKI_DIR)
    args = ap.parse_args()
    WIKI_DIR = args.wiki_dir

    findings, pages, manifest_state = run_lint()
    if args.json:
        json.dump({"generated_at": time.strftime("%FT%TZ", time.gmtime()),
                   "pages": len(pages),
                   "findings": [f.as_dict() for f in findings]},
                  sys.stdout, ensure_ascii=False, indent=1)
        print()
    else:
        print(render(findings, pages, manifest_state))

    # Code de sortie : 0 sauf si un constat CRITIQUE existe (1). Le service
    # traite 1 comme un succes : un RAG incoherent n'est pas une panne d'unite.
    return 1 if any(f.severity == "CRITIQUE" for f in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
