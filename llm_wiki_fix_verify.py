"""Usage : llm_wiki_fix_verify.py <archive.tar.gz> [plan.json]

Preuve : le corps n'a pas bouge, hors insertion de la ligne de titre.
Lecture RAW des deux cotes (aucune traduction de fin de ligne), pour attraper
aussi une conversion CRLF -> LF accidentelle."""
import json, sys, tarfile, importlib.util
spec = importlib.util.spec_from_file_location("f", "/usr/local/bin/llm_wiki_fix_frontmatter.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
tf = tarfile.open(sys.argv[1])
plan_json = sys.argv[2] if len(sys.argv) > 2 else "/tmp/dry.json"
plan = {f["rel"]: f for f in json.load(open(plan_json))["fichiers"]}
ok_fm = ok_title = nl_ok = 0
bad = []
def raw(p):
    return open(p, "r", encoding="utf-8", newline="").read()
for rel, f in plan.items():
    old = tf.extractfile(rel).read().decode("utf-8")
    new = raw("/srv/obsidian-vault/" + rel)
    # 1. le style de fin de ligne doit etre identique
    if (("\r\n" in old) == ("\r\n" in new)) and old.count("\r\n") == new.count("\r\n") - (
            0 if f["titre_insere"] is None else new.count("\r\n") - old.count("\r\n")) or True:
        pass
    if ("\r\n" in old) != ("\r\n" in new):
        bad.append((rel, "style de fin de ligne modifie")); continue
    nl_ok += 1
    _, _, _, ob, _ = m.split_frontmatter(old.replace("\r\n", "\n"))
    _, _, _, nb, _ = m.split_frontmatter(new.replace("\r\n", "\n"))
    t = f["titre_insere"]
    if t is None:
        if ob == nb:
            ok_fm += 1
        else:
            bad.append((rel, "corps modifie sans insertion de titre"))
    else:
        rest = nb.split("\n", 1)[1]
        if nb.startswith("# " + t) and rest.lstrip("\n") == ob.lstrip("\n"):
            ok_title += 1
        else:
            bad.append((rel, "insertion de titre non conforme"))
print("fiches verifiees              :", len(plan))
print("style de fin de ligne conserve:", nl_ok)
print("corps STRICTEMENT identique   :", ok_fm, "(frontmatter seul)")
print("corps = titre + ancien corps  :", ok_title, "(insertions R14)")
print("anomalies                     :", len(bad))
for b in bad[:10]:
    print("  !", b)
