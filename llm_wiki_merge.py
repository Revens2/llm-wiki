#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
llm_wiki_merge.py - Passe 2 du pipeline llm-wiki : FUSION. Aucun appel LLM.

Lot 4 du plan de refonte (plan-ingestion.md 2.1-2.8, 5, 7).

Invariants durs :
  * index.md n'est JAMAIS ecrit ni liste dans `produced` (assertion, pas seuil).
  * Un wikilink ne cree JAMAIS une page ({{E:slug}} non promu -> texte nu).
  * Dedup etage 1 seulement (slug exact / nom normalise / table d'alias).
    Jamais de fermeture transitive ; plafond de cluster CLUSTER_MAX.
  * Idempotent : rejouer la meme fusion ne promeut rien (rsync --checksum).
  * Zones protegees : seul l'interieur de <!-- llmwiki:auto:begin/end --> est reecrit.
"""
import argparse, copy, hashlib, json, os, re, shutil, subprocess, sys, tempfile, unicodedata
from datetime import datetime, timezone

MARK_BEGIN = "<!-- llmwiki:auto:begin -->"
MARK_END = "<!-- llmwiki:auto:end -->"
CLUSTER_MAX = 5                      # 2.3 : au-dela -> revue humaine, jamais de fusion
MIN_BODY = 200                       # validate_stage() exige >= 200 o de corps
REL_MIN_CONF = 0.6                   # 2.2 etape 3
ARTICLES = {"le", "la", "les", "l", "un", "une", "des", "du", "de", "the", "a", "an"}
FORBIDDEN = re.compile(r'[/\\:*?"<>|]')
TOKEN = re.compile(r"\{\{E:([^}]+)\}\}")


# --------------------------------------------------------------- normalisation
def canon(s):
    """NFKD, minuscules, ponctuation/tirets -> separateur unique, articles initiaux retires."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    w = s.split()
    while w and w[0] in ARTICLES:
        w.pop(0)
    return "-".join(w)


def safe_name(name):
    n = FORBIDDEN.sub("-", str(name)).strip().strip(".")
    n = re.sub(r"\s+", " ", n)
    return n or "sans-nom"


def yaml_list(items):
    return "[" + ", ".join('"%s"' % str(i).replace('"', "'") for i in items) + "]"


# --------------------------------------------------------------- frontmatter
FM_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.S)


def split_fm(text):
    m = FM_RE.match(text)
    if not m:
        return None, text
    return m.group(1), text[m.end():]


def fm_get(fm, key):
    if not fm:
        return None
    m = re.search(r"^%s:\s*(.*)$" % re.escape(key), fm, re.M)
    return m.group(1).strip() if m else None


def fm_list(fm, key):
    raw = fm_get(fm, key)
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        return [x.strip().strip("\"'") for x in raw[1:-1].split(",") if x.strip()]
    return [raw.strip("\"'")]


# --------------------------------------------------------------- index d'entites
class EntityIndex:
    """Reconstruit integralement depuis wiki/entities + wiki/concepts. C'est un cache."""

    def __init__(self):
        self.pages = {}     # slug_canon -> dict(name, rel, dirname, aliases, stub)
        self.keys = {}      # cle de rapprochement -> set(slug_canon)  (cluster, non transitif)
        self.collisions = {}

    def _key(self, k, slug):
        if not k or len(k) < 2:
            return
        self.keys.setdefault(k, set()).add(slug)

    def load(self, wiki_dir, alias_table):
        for d in ("entities", "concepts"):
            dd = os.path.join(wiki_dir, d)
            if not os.path.isdir(dd):
                continue
            for fn in sorted(os.listdir(dd)):
                if not fn.endswith(".md"):
                    continue
                name = fn[:-3]
                path = os.path.join(dd, fn)
                try:
                    txt = open(path, encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                fm, _ = split_fm(txt)
                # le slug ecrit en frontmatter fait foi : il fige l'identite meme
                # quand le nom de fichier se normalise autrement (caveman / Cave Man)
                slug = (fm_get(fm, "slug") or "").strip(chr(34) + chr(39)) or canon(name)
                if not slug:
                    continue
                aliases = fm_list(fm, "aliases")
                stub = (fm_get(fm, "stub") or "").lower() == "true"
                if slug not in self.pages:
                    self.pages[slug] = dict(name=name, rel="wiki/%s/%s" % (d, fn),
                                            dirname=d, aliases=aliases, stub=stub)
                self._key(slug, slug)
                self._key(canon(name), slug)
                for a in aliases:
                    self._key(canon(a), slug)
        # table d'alias versionnee a la main : arbitrage humain, prioritaire
        for a, s in alias_table.items():
            self._key(canon(a), canon(s))

    def resolve_key(self, key):
        """Etage 1 : rend un slug canonique, ou None. Jamais de fermeture transitive."""
        c = self.keys.get(key)
        if not c:
            return None
        if len(c) > CLUSTER_MAX:
            self.collisions[key] = sorted(c)
            return None                      # cluster trop large -> revue humaine
        if len(c) > 1:
            self.collisions[key] = sorted(c)
            return None                      # ambigu -> on tranche pour le doublon (2.3)
        return next(iter(c))

    def match(self, ent):
        """slug exact -> nom normalise -> alias. Retourne un slug_canon existant ou None."""
        cands = [canon(ent.get("slug")), canon(ent.get("name"))]
        cands += [canon(a) for a in (ent.get("aliases") or [])]
        for k in cands:
            if not k:
                continue
            r = self.resolve_key(k)
            if r and r in self.pages:
                return r
        return None

    def add_new(self, slug, name, dirname, aliases):
        self.pages[slug] = dict(name=name, rel="wiki/%s/%s.md" % (dirname, name),
                                dirname=dirname, aliases=aliases, stub=False)
        self._key(slug, slug)
        self._key(canon(name), slug)
        for a in aliases or []:
            self._key(canon(a), slug)


# --------------------------------------------------------------- etages 2 et 3
def dedup_stage2_candidates(pending, ent_index):
    """POINT D'ACCROCHE - etage 2 (embeddings sur les noms + clustering borne).
    Non implemente au lot 4 : ne produira que des propositions dans
    wiki/_review/collisions.md, jamais une fusion automatique.
    Contrainte a respecter : taille de cluster <= CLUSTER_MAX, JAMAIS de
    fermeture transitive."""
    return []


def dedup_stage3_llm(candidates):
    """POINT D'ACCROCHE - etage 3 (LLM). DEDUP_LLM=0 par defaut. Non implemente."""
    return []


# --------------------------------------------------------------- registre
def load_pending(path):
    if os.path.exists(path):
        try:
            return json.load(open(path, encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_pending(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, path)


def _norm_para(p):
    """Cle de comparaison d'un paragraphe : espaces normalises, casse ignoree.
    Sert UNIQUEMENT a detecter le doublon ; le texte rendu reste l'original."""
    return re.sub(r"\s+", " ", (p or "")).strip().lower()


def _dedup_paragraphs(md, seen):
    """Retire de `md` les paragraphes deja rencontres ailleurs dans le document.
    C'est le prix du recouvrement entre chunks, et il est prevu (plan 4.3.2)."""
    out = []
    for para in re.split(r"\n\s*\n", md or ""):
        k = _norm_para(para)
        if not k:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(para.strip())
    return "\n\n".join(out)


def reassemble(docs):
    """Plan 4.3 : reconstitue UN document a partir de ses chunks ordonnes.
    Pur, deterministe, testable sans reseau. `docs` est deja trie par index."""
    base = copy.deepcopy(docs[0])
    if len(docs) == 1:
        return base
    n = len(docs)
    note = base["note"]

    # 1. slug / title / doc_date : ceux du chunk 0 (le debut porte le titre).
    tag_count = {}
    for d in docs:
        for t in (d["note"].get("tags") or []):
            tag_count[t] = tag_count.get(t, 0) + 1
    note["tags"] = [t for t, _ in sorted(tag_count.items(),
                                         key=lambda kv: (-kv[1], kv[0]))][:8]
    if n <= 3:
        parts, seenp = [], set()
        for d in docs:
            s = (d["note"].get("summary") or "").strip()
            if s and _norm_para(s) not in seenp:
                seenp.add(_norm_para(s))
                parts.append(s)
        note["summary"] = " ".join(parts)
    else:
        note["summary"] = ((docs[0]["note"].get("summary") or "").strip()
                           + " (document en %d parties)" % n)

    # 2. sections : concatenees dans l'ordre, memes titres consecutifs fusionnes,
    #    paragraphes strictement identiques dedupliques.
    seen = set()
    merged = []
    _hidx = {}
    for d in docs:
        for s in (d["note"].get("sections") or []):
            body = _dedup_paragraphs(s.get("markdown") or "", seen)
            if not body:
                continue
            h = (s.get("heading") or "").strip()
            # ECART ASSUME au 4.3.2, qui ne fusionne que les sections
            # CONSECUTIVES de meme titre. Sur un document decoupe, chaque
            # chunk rend le meme jeu de titres canoniques : la regle
            # "consecutives" produisait une fiche a 5 "## Resume", dont un
            # "Resume" accentue. On regroupe donc par titre NORMALISE
            # (canon() ignore accents et casse), en conservant l ordre de
            # premiere apparition. Toujours deterministe, et une fiche =
            # une section par sujet.
            k = canon(h) or _norm_para(h)
            if k in _hidx:
                merged[_hidx[k]]["markdown"] += "\n\n" + body
            else:
                _hidx[k] = len(merged)
                merged.append({"heading": h, "markdown": body})
    note["sections"] = merged

    # note.warnings : concatenes, dedupliques sur le texte.
    wseen, ws = set(), []
    for d in docs:
        for w in (d["note"].get("warnings") or []):
            k = _norm_para(str(w.get("text", w)) if isinstance(w, dict) else str(w))
            if k and k not in wseen:
                wseen.add(k)
                ws.append(w)
    note["warnings"] = ws

    # 3. entites : union par slug. Definition = celle du chunk ou la salience est
    #    la plus forte ; a egalite, la plus longue. evidence suit la definition.
    ents = {}
    order = []
    for d in docs:
        for e in (d.get("entities") or []):
            k = e.get("slug") or e.get("name")
            if not k:
                continue
            if k not in ents:
                ents[k] = copy.deepcopy(e)
                order.append(k)
                continue
            cur = ents[k]
            rank = {"passing": 0, "secondary": 1, "primary": 2}
            new_r = (rank.get(e.get("salience"), 0), len(e.get("definition") or ""))
            cur_r = (rank.get(cur.get("salience"), 0), len(cur.get("definition") or ""))
            if new_r > cur_r:
                cur["definition"] = e.get("definition")
                cur["evidence"] = e.get("evidence")
                cur["salience"] = e.get("salience")
            for f in ("aliases", "tags"):
                seen_f = list(cur.get(f) or [])
                for v in (e.get(f) or []):
                    if v not in seen_f:
                        seen_f.append(v)
                cur[f] = seen_f
            if not cur.get("subtype"):
                cur["subtype"] = e.get("subtype")
    base["entities"] = [ents[k] for k in order]

    # 4. relations : union par (from, type, to), confidence = max.
    rels, rorder = {}, []
    for d in docs:
        for r in (d.get("relations") or []):
            k = (r.get("from"), r.get("type"), r.get("to"))
            if k not in rels:
                rels[k] = copy.deepcopy(r)
                rorder.append(k)
            elif (r.get("confidence") or 0) > (rels[k].get("confidence") or 0):
                rels[k]["confidence"] = r.get("confidence")
                rels[k]["evidence"] = r.get("evidence")
    base["relations"] = [rels[k] for k in rorder]

    # 5. issues : concatenees, dedupliquees sur le texte.
    iseen, iss = set(), []
    for d in docs:
        for it in (d.get("issues") or []):
            k = _norm_para(json.dumps(it, sort_keys=True, ensure_ascii=False)
                           if isinstance(it, dict) else str(it))
            if k and k not in iseen:
                iseen.add(k)
                iss.append(it)
    base["issues"] = iss

    base["source"]["chunk"] = {"index": 0, "total": n}
    base["extraction"]["chunks_reassembled"] = n
    return base


# --------------------------------------------------------------------------- #
# LOT 5 - sous-index pagines.
#
# Mesure du 2026-08-25 : wiki/_index/sources.md = 341 986 o pour 2 005 fiches
# (170 o/entree), et le corpus vise 2 687 fiches, soit ~458 Ko. Inouvrable dans
# Obsidian, et le probleme empire a chaque ingest.
#
# Pagination par periode ECARTEE, chiffres a l'appui : la distribution de
# last_updated est 2026-01:1 02:2 03:3 05:39 06:25 07:1882 08:53. La page
# "2026-07" ferait a elle seule 320 Ko - on n'aurait rien resolu.
# Bucket alphabetique ECARTE aussi : c=304, v=300, l=191 sur 2 005, soit des
# pages de 1 a 52 Ko, deja desequilibrees d'un facteur 100.
#
# Retenu : pages de taille FIXE dans l'ordre de tri deja utilise, plus un index
# racine reduit a un sommaire. Taille par page bornee par construction
# (250 entrees x ~170 o = ~42 Ko), sommaire ~2 Ko, aucune entree perdue,
# aucun resume tronque. Regeneration deterministe : meme entree = meme octet.
# --------------------------------------------------------------------------- #
INDEX_PAGE_SIZE = int(os.environ.get("INDEX_PAGE_SIZE", "250"))
INDEX_PAGINATE_OVER = int(os.environ.get("INDEX_PAGINATE_OVER", "600"))


def _index_pages(rows, page_size):
    return [rows[i:i + page_size] for i in range(0, len(rows), page_size)]


def write_category_index(index_dir, cat, rows):
    """Ecrit le sous-index d'une categorie. `rows` = [(nom_fiche, ligne)] deja
    trie. Renvoie la liste des chemins relatifs a `index_dir` reellement ecrits.
    Sous le seuil, le fichier unique historique est conserve tel quel."""
    head = ('---\ntitle: "Index %s"\ntype: index\ntags: ["index"]\n---\n'
            "# Index - %s\n\n")
    written = []
    if len(rows) <= INDEX_PAGINATE_OVER:
        p = os.path.join(index_dir, "%s.md" % cat)
        with open(p, "w", encoding="utf-8") as f:
            f.write(head % (cat, cat)
                    + "%d fiches.\n\n%s\n" % (len(rows),
                                              "\n".join(l for _, l in rows)))
        written.append("%s.md" % cat)
        return written

    pages = _index_pages(rows, INDEX_PAGE_SIZE)
    width = max(2, len(str(len(pages))))
    os.makedirs(os.path.join(index_dir, cat), exist_ok=True)
    summary = []
    for i, page in enumerate(pages, 1):
        name = ("p%0" + str(width) + "d") % i
        rel = "%s/%s.md" % (cat, name)
        with open(os.path.join(index_dir, rel), "w", encoding="utf-8") as f:
            f.write('---\ntitle: "Index %s - %s"\ntype: index\ntags: ["index"]\n'
                    "---\n# Index - %s (%s/%d)\n\n"
                    "Retour : [[%s|Index %s]]\n\n%d fiches, de `%s` a `%s`.\n\n%s\n"
                    % (cat, name, cat, name, len(pages), cat, cat, len(page),
                       page[0][0], page[-1][0],
                       "\n".join(l for _, l in page)))
        written.append(rel)
        summary.append("- [[%s/%s|%s]] - %d fiches, de `%s` a `%s`"
                       % (cat, name, name, len(page), page[0][0], page[-1][0]))
    p = os.path.join(index_dir, "%s.md" % cat)
    with open(p, "w", encoding="utf-8") as f:
        f.write(head % (cat, cat)
                + "%d fiches, %d pages de %d au plus.\n\n%s\n"
                % (len(rows), len(pages), INDEX_PAGE_SIZE, "\n".join(summary)))
    written.append("%s.md" % cat)
    return written


SAL_RANK = {"passing": 0, "secondary": 1, "primary": 2}


def promotion_met(rec, min_docs):
    return len(rec["docs"]) >= min_docs or rec.get("max_salience") == "primary"


# --------------------------------------------------------------- rendu
def resolve_tokens(md, slug2name, promoted):
    def rep(m):
        k = canon(m.group(1))
        if k in promoted and k in slug2name:
            return "[[%s]]" % slug2name[k]
        # entite connue non promue OU inconnue -> texte nu (cas normal, 2.4)
        return slug2name.get(k) or m.group(1).replace("-", " ")
    return TOKEN.sub(rep, md or "")


def auto_block(rec, mentions, relations, sources):
    out = [MARK_BEGIN, ""]
    defs = [d for d in rec.get("definitions", []) if d.get("text")]
    if defs:
        best = sorted(defs, key=lambda d: (-SAL_RANK.get(d.get("salience"), 0), d["doc"]))[0]
        out += ["## Definition", "", best["text"].strip(), ""]
    if mentions:
        out += ["## Mentions", ""]
        for doc, ev in mentions:
            out.append("- [[%s]]%s" % (doc, (" - " + ev.strip()) if ev else ""))
        out.append("")
    if relations:
        out += ["## Liens", ""]
        for t, target in relations:
            out.append("- %s : [[%s]]" % (t, target))
        out.append("")
    if sources:
        out += ["## Sources", ""]
        for s in sources:
            out.append("- `%s`" % s)
        out.append("")
    out.append(MARK_END)
    return "\n".join(out) + "\n"


def upsert_auto(existing_text, block):
    """N'ecrase JAMAIS une section redigee a la main : seul l'interieur des marqueurs bouge."""
    if MARK_BEGIN in existing_text and MARK_END in existing_text:
        pre = existing_text.split(MARK_BEGIN)[0]
        post = existing_text.split(MARK_END, 1)[1]
        return pre + block.rstrip("\n") + post
    if existing_text.endswith("\n\n"):
        sep = ""
    elif existing_text.endswith("\n"):
        sep = "\n"
    else:
        sep = "\n\n"
    return existing_text + sep + block


# --------------------------------------------------------------- fusion
def merge(args):
    vault = args.vault
    wiki_dir = os.path.join(vault, "wiki")
    today_fallback = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    alias_table = {}
    ap = os.path.join(wiki_dir, "_review", "aliases.yml")
    if os.path.exists(ap):
        for line in open(ap, encoding="utf-8"):
            line = line.split("#", 1)[0].strip()
            if ":" in line:
                a, s = line.split(":", 1)
                if a.strip() and s.strip():
                    alias_table[a.strip()] = s.strip()

    ENT = EntityIndex()
    ENT.load(wiki_dir, alias_table)

    files = []
    for root, _, fns in os.walk(args.spool):
        for fn in sorted(fns):
            if fn.endswith(".json"):
                files.append(os.path.join(root, fn))
    files.sort()

    # 2.2.a reassemblage : un groupe par sha256 ; incomplet -> on attend
    groups = {}
    for p in files:
        try:
            j = json.load(open(p, encoding="utf-8"))
        except Exception as e:
            print("SKIP json illisible %s (%s)" % (p, e), file=sys.stderr)
            continue
        groups.setdefault(j["source"]["sha256"], []).append((p, j))
    ready = []
    for sha, g in groups.items():
        total = g[0][1]["source"].get("chunk", {}).get("total", 1)
        if len(g) != total:
            print("ATTENTE chunks incomplets sha=%s (%d/%d)" % (sha[:12], len(g), total),
                  file=sys.stderr)
            continue
        g.sort(key=lambda t: t[1]["source"].get("chunk", {}).get("index", 0))
        # LOT 5 : reassemblage conforme au plan 4.3. L'ancienne concatenation
        # rendait N fois le meme paragraphe pour un document a recouvrement,
        # et N definitions concurrentes pour une meme entite.
        base = reassemble([j for _, j in g])
        if len(g) > 1:
            print("REASSEMBLE sha=%s %d chunks -> %d sections, %d entites, %d relations"
                  % (sha[:12], len(g), len(base["note"].get("sections") or []),
                     len(base.get("entities") or []),
                     len(base.get("relations") or [])), file=sys.stderr)
        ready.append((sha, [p for p, _ in g], base))

    # 2.5 ordonnancement : ConvIA d'abord, puis taille croissante, puis chemin
    def order(t):
        p = t[2]["source"]["path"]
        return (0 if "/ConvIA/" in p else 1, t[2]["source"].get("size", 0), p)
    ready.sort(key=order)
    if args.limit:
        ready = ready[:args.limit]

    os.makedirs(os.path.join(vault, ".staging"), exist_ok=True)
    stage = tempfile.mkdtemp(prefix="merge-", dir=os.path.join(vault, ".staging"))
    os.makedirs(os.path.join(stage, "wiki"), exist_ok=True)
    aux = os.path.join(stage, "aux")
    os.makedirs(os.path.join(aux, "_index"), exist_ok=True)

    pending = load_pending(args.pending)
    for rec in pending.values():
        rec["docs"] = list(rec.get("docs", []))

    doc_meta = {}
    touched = set()

    # ---- passe A : registre (toutes les entites de tous les documents du run)
    for sha, paths, d in ready:
        dslug = d["note"]["slug"]
        ddate = (d["note"].get("doc_date")
                 or d["extraction"].get("extracted_at", "")[:10] or today_fallback)
        doc_meta[dslug] = dict(sha=sha, paths=paths, d=d, date=ddate)
        for e in d.get("entities", []):
            key = ENT.match(e) or canon(e.get("slug")) or canon(e.get("name"))
            if not key:
                continue
            e["_key"] = key
            rec = pending.setdefault(key, dict(
                name=e.get("name") or key, kind=e.get("kind", "entity"),
                subtype=e.get("subtype"), aliases=[], tags=[], docs=[],
                max_salience="passing", definitions=[],
                first_seen=ddate, last_seen=ddate, promoted_at=None))
            rec["name"] = rec.get("name") or e.get("name")
            rec["aliases"] = sorted(set(rec.get("aliases") or []) | set(e.get("aliases") or []))
            rec["tags"] = sorted(set(rec.get("tags") or []) | set(e.get("tags") or []))
            if dslug not in rec["docs"]:
                rec["docs"].append(dslug)
            rec["docs"] = sorted(set(rec["docs"]))
            if SAL_RANK.get(e.get("salience"), 0) > SAL_RANK.get(rec["max_salience"], 0):
                rec["max_salience"] = e.get("salience")
            if e.get("definition") and not any(x["doc"] == dslug for x in rec["definitions"]):
                rec["definitions"].append(dict(doc=dslug, text=e["definition"],
                                               salience=e.get("salience")))
            rec["definitions"].sort(key=lambda x: x["doc"])
            rec["first_seen"] = min(rec["first_seen"], ddate)
            rec["last_seen"] = max(rec["last_seen"], ddate)

    keys_in_run = set()
    for meta in doc_meta.values():
        for e in meta["d"].get("entities", []):
            if e.get("_key"):
                keys_in_run.add(e["_key"])

    # ---- passe B : qui a une page (existante) / qui est promue ce run
    promoted, new_pages, held = set(), {}, []
    for key in sorted(keys_in_run):
        rec = pending[key]
        if key in ENT.pages:
            promoted.add(key)
            # coherence registre<->disque : une entite qui a une fiche est promue
            if not rec.get("promoted_at"):
                rec["promoted_at"] = rec["last_seen"]
            continue
        if promotion_met(rec, args.promote_min_docs):
            dirname = "concepts" if rec.get("kind") == "concept" else "entities"
            name = safe_name(rec.get("name") or key)
            promoted.add(key)
            new_pages[key] = dict(dirname=dirname, name=name)
            ENT.add_new(key, name, dirname, rec.get("aliases") or [])
        else:
            held.append(key)
    slug2name = {k: v["name"] for k, v in ENT.pages.items()}

    # ---- passe C : agregats par entite
    mentions, relations, sources_of = {}, {}, {}
    for dslug, meta in sorted(doc_meta.items()):
        d = meta["d"]
        src_rel = re.sub(r"^.*?/(raw/)", r"\1", d["source"]["path"])
        for e in d.get("entities", []):
            k = e.get("_key")
            if not k or k not in promoted:
                continue
            mentions.setdefault(k, []).append((dslug, (e.get("evidence") or "").strip()))
            sources_of.setdefault(k, set()).add(src_rel)
        for r in d.get("relations", []):
            try:
                conf = float(r.get("confidence") or 0)
            except (TypeError, ValueError):
                conf = 0.0
            if conf < REL_MIN_CONF:
                continue
            a, b = canon(r.get("from")), canon(r.get("to"))
            if a in promoted and b in promoted and a != b:
                relations.setdefault(a, set()).add((r.get("type") or "lie a", slug2name[b]))
    for k in mentions:
        mentions[k] = sorted(set(mentions[k]))

    # ---- passe D : ecriture du staging
    produced = []
    skipped_thin = []

    def stage_write(rel, content):
        p = os.path.join(stage, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)
        produced.append(rel)

    # D.1 fiches de provenance (2.8 : degradees, absentes d'index.md)
    for dslug, meta in sorted(doc_meta.items()):
        d = meta["d"]
        n = d["note"]
        src_rel = re.sub(r"^.*?/(raw/)", r"\1", d["source"]["path"])
        ents = sorted({slug2name[e["_key"]] for e in d.get("entities", [])
                       if e.get("_key") in promoted})
        has_resume = any(canon(s.get("heading")) in ("resume", "summary")
                         for s in n.get("sections", []))
        body = ["# %s" % n["title"], ""]
        if not has_resume:
            body += ["## Resume", "",
                     resolve_tokens(n.get("summary", "").strip(), slug2name, promoted), ""]
        for s in n.get("sections", []):
            body += ["## %s" % s["heading"], "",
                     resolve_tokens(s["markdown"].strip(), slug2name, promoted), ""]
        for w in n.get("warnings", []):
            txt = resolve_tokens(str(w.get("text", w)).strip(), slug2name, promoted)
            body += ["> [!warning] " + txt.replace("\n", "\n> "), ""]
        if ents:
            body += ["## Entites & Concepts Mentionnes", ""] + ["- [[%s]]" % e for e in ents] + [""]
        body += ["## Sources", "", "- `%s` (sha %s)" % (src_rel, meta["sha"][:8]), ""]
        fm = ["---", 'title: "%s"' % n["title"].replace('"', "'"), "type: source",
              "tags: %s" % yaml_list(n.get("tags") or ["imported"]),
              "source_count: 1", "last_updated: %s" % meta["date"],
              "links: %s" % yaml_list([src_rel]), "---", ""]
        stage_write("wiki/sources/%s.md" % safe_name(dslug), "\n".join(fm + body))
        touched.add("sources")

    # D.2 pages d'entites : creation (promues) ou upsert du bloc auto (existantes)
    for key in sorted(promoted):
        rec = pending.get(key)
        if not rec or key not in mentions:
            continue
        page = ENT.pages[key]
        blk = auto_block(rec, mentions[key], sorted(relations.get(key, set())),
                         sorted(sources_of.get(key, set())))
        rel = "wiki/%s/%s.md" % (page["dirname"], safe_name(page["name"]))
        real = os.path.join(vault, rel)
        if key in new_pages:
            fm = ["---", 'title: "%s"' % page["name"].replace('"', "'"),
                  "type: %s" % ("concept" if page["dirname"] == "concepts" else "entity"),
                  "slug: %s" % key,
                  "tags: %s" % yaml_list(rec.get("tags") or ["a-enrichir"])]
            if rec.get("aliases"):
                fm.append("aliases: %s" % yaml_list(rec["aliases"]))
            fm += ["last_updated: %s" % rec["last_seen"], "---", ""]
            content = "\n".join(fm) + "# %s\n\n" % page["name"] + blk
        elif os.path.exists(real):
            content = upsert_auto(open(real, encoding="utf-8", errors="replace").read(), blk)
        else:
            continue
        _, body = split_fm(content)
        if len(body.encode("utf-8")) < MIN_BODY:
            skipped_thin.append(key)
            if key in new_pages:
                del new_pages[key]
                promoted.discard(key)
            continue
        stage_write(rel, content)
        touched.add(page["dirname"])
        if key in new_pages:
            rec["promoted_at"] = rec["last_seen"]

    # D.3 log.md - append-only, idempotent par empreinte de document
    log_path = os.path.join(vault, "log.md")
    existing_log = ""
    if os.path.exists(log_path):
        existing_log = open(log_path, encoding="utf-8", errors="replace").read()
    new_entries = []
    for dslug, meta in sorted(doc_meta.items(), key=lambda t: (t[1]["date"], t[0])):
        tag = "sha %s" % meta["sha"][:8]
        if tag in existing_log:
            continue
        d = meta["d"]
        src_rel = re.sub(r"^.*?/(raw/)", r"\1", d["source"]["path"])
        prod = [r for r in produced if r == "wiki/sources/%s.md" % safe_name(dslug)]
        keys_here = {e.get("_key") for e in d.get("entities", []) if e.get("_key")}
        prom = len([k for k in keys_here if k in new_pages])
        att = len([k for k in keys_here if k not in promoted])
        fiches = [ENT.pages[k]["rel"] for k in sorted(keys_here)
                  if k in promoted and k in mentions]
        new_entries.append(
            "## [%s] ingest | %s\n- source : %s (%s)\n- fiches : %s\n"
            "- entites mises en attente : %d - promues : %d\n"
            % (meta["date"], d["note"]["title"], src_rel, tag,
               ", ".join(prod + fiches) or "aucune", att, prom))
    if new_entries:
        with open(os.path.join(aux, "log.md"), "w", encoding="utf-8") as f:
            f.write(existing_log.rstrip("\n") + "\n" + "\n".join(new_entries))

    # D.4 sous-index par categorie - regeneres deterministement (idempotents)
    def cat_entries(cat):
        seen = {}
        real_dir = os.path.join(wiki_dir, cat)
        if os.path.isdir(real_dir):
            for fn in os.listdir(real_dir):
                if fn.endswith(".md"):
                    seen[fn] = os.path.join(real_dir, fn)
        st_dir = os.path.join(stage, "wiki", cat)
        if os.path.isdir(st_dir):
            for fn in os.listdir(st_dir):
                if fn.endswith(".md"):
                    seen[fn] = os.path.join(st_dir, fn)
        out = []
        for fn in sorted(seen):
            try:
                t = open(seen[fn], encoding="utf-8", errors="replace").read()
            except OSError:
                continue
            fm, body = split_fm(t)
            lu = (fm_get(fm, "last_updated") or "").strip('"')
            line = ""
            for l in body.splitlines():
                l = l.strip()
                if l and not l.startswith(("#", ">", "-", "<!--", "|", "*")):
                    line = l[:160]
                    break
            out.append((fn[:-3],
                        "- [[%s]]%s%s" % (fn[:-3], (" - " + line) if line else "",
                                          (" *(%s)*" % lu) if lu else "")))
        return out

    index_written = []
    for cat in sorted(touched):
        index_written += write_category_index(os.path.join(aux, "_index"), cat,
                                              cat_entries(cat))

    # ---- validation : validate_stage() reutilise TEL QUEL depuis llm_wiki_ingest.sh
    prod_file = os.path.join(stage, ".produced")
    shim = os.path.join(stage, ".validate.sh")
    src = open("/usr/local/bin/llm_wiki_ingest.sh", encoding="utf-8", errors="replace").read()
    m = re.search(r"^validate_stage\(\) \{.*?^\}\n", src, re.S | re.M)
    if not m:
        print("ERREUR: validate_stage() introuvable dans llm_wiki_ingest.sh", file=sys.stderr)
        return 2
    with open(shim, "w", encoding="utf-8") as f:
        f.write("#!/usr/bin/env bash\nset -uo pipefail\nWIKI_DIR=%s\n" % wiki_dir)
        f.write('err(){ printf "%s\\n" "$*" >&2; }\n')
        f.write(m.group(0))
        f.write('\nvalidate_stage "$1" "$2"\n')
    rc = subprocess.run(["bash", shim, stage, prod_file], capture_output=True, text=True)
    val_out = (rc.stdout + rc.stderr).strip()
    validated = [l for l in open(prod_file, encoding="utf-8").read().splitlines() if l]

    # ---- assertions dures (5.1 / 2.8)
    hard = []
    for lst, label in ((produced, "produced"), (validated, "validate_stage")):
        for r in lst:
            if os.path.basename(r) == "index.md":
                hard.append("ASSERTION VIOLEE: %s contient index.md (%s)" % (label, r))
    for cand in (os.path.join(stage, "wiki", "index.md"), os.path.join(stage, "index.md"),
                 os.path.join(aux, "index.md")):
        if os.path.exists(cand):
            hard.append("ASSERTION VIOLEE: index.md present dans le staging (%s)" % cand)

    report = dict(
        stage=stage, docs=len(ready), fiches_staging=len(produced),
        validate_rc=rc.returncode, validate_out=val_out, validated=len(validated),
        entites_registre=len(pending), promues_ce_run=len(new_pages),
        versees_au_registre_sans_page=len(held),
        rejetees_corps_court=len(skipped_thin),
        collisions_etage1=len(ENT.collisions), log_entries=len(new_entries),
        sous_index=sorted(touched), assertions=hard)

    if args.dry_run:
        print("=== DRY-RUN : rien n'est ecrit dans le wiki ===")
        print(json.dumps(report, ensure_ascii=False, indent=1))
        print("--- fichiers QUI SERAIENT ecrits ---")
        for r in sorted(produced):
            print("  %s" % r)
        for f in sorted(index_written):
            print("  wiki/_index/%s" % f)
        if new_entries:
            print("  log.md (+%d entrees)" % len(new_entries))
        print("--- rsync --checksum --dry-run vers le wiki reel ---")
        d1 = subprocess.run(["rsync", "-rlp", "--chmod=F664,D2775", "--checksum", "--dry-run", "-i",
                             os.path.join(stage, "wiki") + "/", wiki_dir + "/"],
                            capture_output=True, text=True)
        print(d1.stdout.strip() or "  (aucun changement)")
        print("--- promues : %d ---" % len(new_pages))
        for k in sorted(new_pages):
            print("  %-45s -> %s" % (k, ENT.pages[k]["rel"]))
        print("--- versees au registre SANS page : %d ---" % len(held))
        for k in sorted(held):
            print("  %-45s docs=%d salience=%s" % (k, len(pending[k]["docs"]),
                                                   pending[k]["max_salience"]))
        if ENT.collisions:
            print("--- collisions etage 1 (non fusionnees, revue humaine) ---")
            for k, v in sorted(ENT.collisions.items()):
                print("  %s -> %s" % (k, v))
        shutil.rmtree(stage, ignore_errors=True)
        return 0 if (rc.returncode == 0 and not hard) else 1

    if hard or rc.returncode != 0:
        print(json.dumps(report, ensure_ascii=False, indent=1))
        print("ABANDON : aucune promotion.", file=sys.stderr)
        shutil.rmtree(stage, ignore_errors=True)
        return 1

    # ---- promotion (2.1 : promotion AVANT manifeste)
    idx_path = os.path.join(vault, "index.md")
    idx_before = hashlib.sha256(open(idx_path, "rb").read()).hexdigest()
    moved = []
    r1 = subprocess.run(["rsync", "-rlp", "--chmod=F664,D2775", "--checksum", "-i",
                         os.path.join(stage, "wiki") + "/", wiki_dir + "/"],
                        capture_output=True, text=True)
    moved += [l for l in r1.stdout.splitlines() if l.strip()]
    os.makedirs(os.path.join(wiki_dir, "_index"), exist_ok=True)
    r2 = subprocess.run(["rsync", "-rlp", "--chmod=F664,D2775", "--checksum", "-i",
                         os.path.join(aux, "_index") + "/",
                         os.path.join(wiki_dir, "_index") + "/"], capture_output=True, text=True)
    moved += [l for l in r2.stdout.splitlines() if l.strip()]
    for cat in sorted(touched):
        live = {os.path.basename(r) for r in index_written
                if r.startswith(cat + "/")}
        d = os.path.join(wiki_dir, "_index", cat)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d)):
            if fn.endswith(".md") and fn not in live:
                try:
                    os.unlink(os.path.join(d, fn))
                    moved.append("*deleting  _index/%s/%s" % (cat, fn))
                except OSError:
                    pass
    if new_entries:
        r3 = subprocess.run(["rsync", "-rlp", "--chmod=F664,D2775", "--checksum", "-i",
                             os.path.join(aux, "log.md"), log_path],
                            capture_output=True, text=True)
        moved += [l for l in r3.stdout.splitlines() if l.strip()]
    # aligner proprietaire/droits sur la convention du vault (juliann:llmwiki)
    targets = [os.path.join(vault, r) for r in produced]
    targets += [os.path.join(wiki_dir, "_index")]
    targets += [os.path.join(wiki_dir, "_index", f) for f in index_written]
    targets += [os.path.join(wiki_dir, "_index", c) for c in sorted(touched)
                if os.path.isdir(os.path.join(aux, "_index", c))]
    if new_entries:
        targets.append(log_path)
    for tp in targets:
        if os.path.exists(tp):
            try:
                shutil.chown(tp, user=args.owner, group=args.group)
            except (OSError, LookupError):
                pass
    subprocess.run(["sync"])
    idx_after = hashlib.sha256(open(idx_path, "rb").read()).hexdigest()
    if idx_before != idx_after:
        print("ASSERTION VIOLEE: index.md a change pendant la promotion !", file=sys.stderr)
        return 3

    save_pending(args.pending, pending)

    # ---- manifeste v3 (apres promotion)
    if args.manifest:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(args.manifest, "a", encoding="utf-8") as f:
            for dslug, meta in sorted(doc_meta.items()):
                d = meta["d"]
                own = [r for r in validated
                       if r == "wiki/sources/%s.md" % safe_name(dslug)]
                own += [ENT.pages[k]["rel"] for k in sorted(
                    {e.get("_key") for e in d.get("entities", []) if e.get("_key")})
                    if k in promoted and ENT.pages[k]["rel"] in validated]
                own = [r for r in sorted(set(own)) if os.path.basename(r) != "index.md"]
                total = d["source"].get("chunk", {}).get("total", 1)
                f.write(json.dumps(dict(
                    schema=3, path=d["source"]["path"], sha256=meta["sha"],
                    size=d["source"].get("size", 0), mtime=d["source"].get("mtime", 0),
                    ingested_at=stamp, phase="merge", status="merged", reason=None,
                    attempts=0, model=d["extraction"].get("model"), duration_s=0,
                    chunks=dict(total=total, done=total), produced=own),
                    ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    report["rsync_promus"] = len(moved)
    report["index_md_sha_before"] = idx_before[:32]
    report["index_md_sha_after"] = idx_after[:32]
    print(json.dumps(report, ensure_ascii=False, indent=1))
    print("--- rsync -i : ce qui a REELLEMENT ete promu (%d) ---" % len(moved))
    for l in moved:
        print("  " + l)
    shutil.rmtree(stage, ignore_errors=True)
    return 0


def main():
    p = argparse.ArgumentParser(description="Passe 2 llm-wiki : fusion sans LLM.")
    p.add_argument("--spool", required=True)
    p.add_argument("--vault", default="/srv/obsidian-vault")
    p.add_argument("--pending", default="/var/lib/llm-wiki/pending-entities.json")
    p.add_argument("--manifest", default=None)
    p.add_argument("--promote-min-docs", type=int,
                   default=int(os.environ.get("PROMOTE_MIN_DOCS", "2")))
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--owner", default="juliann")
    p.add_argument("--group", default="llmwiki")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    import fcntl
    fd = os.open("/var/lib/llm-wiki/.merge.lock", os.O_CREAT | os.O_RDWR, 0o664)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Une fusion est deja en cours (flock).", file=sys.stderr)
        return 75
    return merge(a)


if __name__ == "__main__":
    sys.exit(main())
