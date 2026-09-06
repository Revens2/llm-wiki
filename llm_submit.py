#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""llm_submit.py — adaptateur de soumission LLM pour le pipeline llm-wiki (lot 2).

Python 3 stdlib UNIQUEMENT (urllib, json, time, random, os, argparse) : le service
tourne dans un bac a sable systemd strict, aucune dependance externe n'est installable.

Trois modes derriere une interface unique, choisis par SUBMIT_MODE :
  - interactive : POST generateContent (SEUL mode reellement implemente)
  - batch       : stub NotImplementedError (indisponible en palier gratuit,
                  batchGenerateContent -> 400 FAILED_PRECONDITION, verifie sur la vraie cle)
  - agy         : chemin historique /opt/agy/bin/agy, conserve pour rollback

REGLE ABSOLUE : un quota (429 / RESOURCE_EXHAUSTED) n'incremente JAMAIS `attempts`
cote appelant. Le statut "quota" est structurellement distinct de "http_error".
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

__version__ = "1.0.0"

API_ROOT = "https://generativelanguage.googleapis.com/v1beta/models"
AGY_BIN = os.environ.get("AGY_BIN", "/opt/agy/bin/agy")

STATE_DIR = os.environ.get("LLM_WIKI_STATE_DIR", "/var/lib/llm-wiki")
PACE_PATH = os.path.join(STATE_DIR, "pace.json")

DEFAULT_TIMEOUT = float(os.environ.get("LLM_SUBMIT_TIMEOUT", "180"))

# Garde-fous de stabilite de la boucle. Ce ne sont PAS des seuils de quota Google :
# aucun RPM/RPD n'est code en dur nulle part, le debit est decouvert a l'execution.
MIN_INTERVAL_MS = int(os.environ.get("MIN_INTERVAL_MS", "1500"))
MAX_INTERVAL_MS = int(os.environ.get("MAX_INTERVAL_MS", "300000"))
START_INTERVAL_MS = int(os.environ.get("START_INTERVAL_MS", "4000"))
AI_STEP_MS = int(os.environ.get("AIMD_STEP_MS", "250"))     # additive increase du debit
AI_STREAK = int(os.environ.get("AIMD_OK_STREAK", "10"))     # succes requis avant increase
MD_FACTOR = float(os.environ.get("AIMD_MD_FACTOR", "2.0"))  # multiplicative decrease
JITTER_RATIO = float(os.environ.get("PACE_JITTER_RATIO", "0.3"))
MAX_LOCAL_RETRIES = int(os.environ.get("MAX_LOCAL_RETRIES", "3"))
MAX_TRANSIENT_RETRIES = int(os.environ.get("MAX_TRANSIENT_RETRIES", "3"))
TRANSIENT_BASE_S = float(os.environ.get("TRANSIENT_BASE_S", "2"))
RESUME_DELAY_SECONDS = float(os.environ.get("RESUME_DELAY_SECONDS", "18180"))
QUOTA_SAFETY_MARGIN = int(os.environ.get("QUOTA_SAFETY_MARGIN", "3"))
DAILY_DELAY_THRESHOLD_S = 3600.0  # retryDelay > 1 h => on considere la limite journaliere

# --- lot 6 : un 429 "PerDay" n'est PAS une preuve d'epuisement journalier -------- #
# Mesure du 2026-08-25 : sur des quotaId contenant "PerDay", Google renvoie
# retryDelay "51s", puis 32s, puis 46s, et une sonde ~90 s plus tard passe.
# Appliquer un plancher de 5 h a ces 429 transformait une pause de 51 s en panne
# de 5 h. Regle retenue : on RESPECTE TOUJOURS le retryDelay annonce ; le plancher
# RESUME_DELAY_SECONDS ne s'applique qu'a un epuisement CONFIRME, c'est-a-dire
#   - N echecs PerDay consecutifs sur le meme modele malgre le respect du retryDelay, ou
#   - un retryDelay long (> DAILY_DELAY_THRESHOLD_S : Google dit lui-meme de revenir plus tard), ou
#   - un quotaValue atteint de facon avereee (compteur du jour >= quotaValue).
DAILY_CONFIRM_STRIKES = int(os.environ.get("DAILY_CONFIRM_STRIKES", "3"))
# Au-dela de cette attente, on ne bloque plus le run sur place : on rend la main
# a l'appelant (cascade de modeles) plutot que de dormir indefiniment.
DAILY_CONFIRM_MAX_WAIT_S = float(os.environ.get("DAILY_CONFIRM_MAX_WAIT_S", "300"))

# Le reset du quota RPD du palier gratuit se fait a minuit PACIFIQUE, pas UTC.
PACIFIC_TZ = os.environ.get("QUOTA_RESET_TZ", "America/Los_Angeles")

# Cascade par qualite decroissante. L'ordre vient de la mesure (comparatif-modeles.md),
# pas de l'intuition. Surchargeable par EXTRACT_MODEL_CASCADE (liste separee par des virgules).
# ORDRE MESURE le 2026-08-26 sur 10 documents communs (voir comparatif-modeles.md).
# Critere : relations et slugs PARTAGES par >= 2 documents (= graphe reel), pas volume.
#   1. 2.5-flash          43 relations (x2 le suivant), 5 slugs partages, cache 14,5 k
#   2. 3.5-flash          meilleure densite de graphe : 5 partages sur 31 slugs (16,1 %)
#   3. 3.6-flash          18 promouvables, 21 relations, 0 avertissement
#   4. 3.1-flash-lite     GA, quota abondant, 7,3 % de partage
#   5. 3.1-flash-lite-preview  checkpoint IDENTIQUE au precedent (sortie octet pour
#                              octet a temperature 0) : c'est un SECOND SEAU DE QUOTA
#                              a qualite egale, d'ou sa presence juste apres.
#   6. 3.5-flash-lite     14,3 % de partage mais seulement 13 relations : fin de cascade.
# EXCLUS deliberement :
#   - gemini-2.5-flash-lite : 1 slug partage sur 102 (1,0 %). Volume sans graphe,
#     confirme deux fois (lot 3 et lot 6). Ne jamais le remettre dans la cascade.
#   - gemini-3.7-flash : refuse thinkingLevel=MINIMAL (400). Le repli automatique
#     le rend utilisable, mais il n'a JAMAIS ete mesure : a evaluer avant inclusion.
#   - gemini-3-flash-preview : jamais mesure.
DEFAULT_CASCADE = [
    "gemini-2.5-flash",
    "gemini-3.5-flash",
    "gemini-3.6-flash",
    "gemini-3.1-flash-lite",
    "gemini-3.1-flash-lite-preview",
    "gemini-3.5-flash-lite",
]

_DURATION_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*s\s*$")


# --------------------------------------------------------------------------- #
# Resultat
# --------------------------------------------------------------------------- #
def _result(ok, status, text=None, http_code=None, retry_after_s=None,
            usage=None, raw_error=None, finish_reason=None, quota_scope=None,
            quota_id=None, quota_value=None):
    return {
        "ok": bool(ok),
        "text": text,
        "status": status,          # ok|quota|http_error|timeout|truncated|blocked|invalid
        "http_code": http_code,
        "retry_after_s": retry_after_s,
        "usage": usage or {},
        "raw_error": raw_error,
        "finish_reason": finish_reason,
        "quota_scope": quota_scope,  # None | "minute" | "daily"
        "quota_id": quota_id,        # ex. GenerateRequestsPerDayPerProjectPerModel-FreeTier
        "quota_value": quota_value,  # ex. 20 (int) : le plafond annonce par Google
    }


class QuotaExhausted(Exception):
    """Quota non absorbable sur place : l'appelant doit sortir en 75."""

    def __init__(self, reset_s, scope, result):
        super().__init__("quota exhausted (%s), reset in %.0fs" % (scope, reset_s))
        self.reset_s = reset_s
        self.scope = scope
        self.result = result


class SafetyMarginReached(Exception):
    """Arret propre volontaire : QUOTA_SAFETY_MARGIN atteint. PAS un incident."""


# --------------------------------------------------------------------------- #
# Parsing des erreurs Google
# --------------------------------------------------------------------------- #
def parse_duration(value):
    """'34s' / '1.5s' -> float. Tolere un nombre nu. None si format inattendu."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = _DURATION_RE.match(str(value))
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _iter_details(body):
    err = (body or {}).get("error") or {}
    det = err.get("details")
    return det if isinstance(det, list) else []


def retry_info_seconds(body):
    """Extrait error.details[@type=...RetryInfo].retryDelay ('34s') -> float|None."""
    for d in _iter_details(body):
        if not isinstance(d, dict):
            continue
        if d.get("@type") == "type.googleapis.com/google.rpc.RetryInfo":
            return parse_duration(d.get("retryDelay"))
    return None


def quota_violation(body):
    """Renvoie (quotaId, quotaValue) de la premiere violation exploitable, sinon (None, None).

    quotaValue est une CHAINE cote API ("20") : on la convertit en int quand c'est possible,
    car c'est elle qui permet de confirmer un epuisement journalier de facon averee."""
    for d in _iter_details(body):
        if not isinstance(d, dict):
            continue
        for v in d.get("violations") or []:
            if not isinstance(v, dict):
                continue
            qid = v.get("quotaId")
            if not qid:
                continue
            qval = v.get("quotaValue")
            try:
                qval = int(str(qval)) if qval is not None else None
            except (TypeError, ValueError):
                qval = None
            return str(qid), qval
    return None, None


def quota_scope(body, retry_after_s):
    """'daily' | 'minute'. quotaId prime ; sinon retryDelay > 1 h => journalier."""
    for d in _iter_details(body):
        if not isinstance(d, dict):
            continue
        for v in d.get("violations") or []:
            qid = str((v or {}).get("quotaId", ""))
            low = qid.lower()
            if "perday" in low:
                return "daily"
            if "perminute" in low or "perminutepermodel" in low:
                return "minute"
    if retry_after_s is not None and retry_after_s > DAILY_DELAY_THRESHOLD_S:
        return "daily"
    return "minute"


def _is_quota(http_code, body, raw_text):
    if http_code == 429:
        return True
    status = ((body or {}).get("error") or {}).get("status")
    if status == "RESOURCE_EXHAUSTED":
        return True
    return "RESOURCE_EXHAUSTED" in (raw_text or "")


# --------------------------------------------------------------------------- #
# Transport HTTP (injectable pour les tests hors ligne)
# --------------------------------------------------------------------------- #
def http_post(url, headers, payload_bytes, timeout):
    """-> (http_code, headers_dict, body_bytes). Leve TimeoutError sur expiration."""
    req = urllib.request.Request(url, data=payload_bytes, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as e:            # 4xx/5xx : corps exploitable
        body = b""
        try:
            body = e.read()
        except Exception:
            pass
        return e.code, dict(e.headers.items() if e.headers else {}), body
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
            raise TimeoutError(str(reason))
        raise


# --------------------------------------------------------------------------- #
# Analyse d'une reponse generateContent
# --------------------------------------------------------------------------- #
_BLOCKED = {"SAFETY", "RECITATION", "PROHIBITED_CONTENT", "BLOCKLIST", "SPII"}


def interpret(http_code, resp_headers, body_bytes):
    raw_text = ""
    if body_bytes:
        try:
            raw_text = body_bytes.decode("utf-8", "replace")
        except Exception:
            raw_text = repr(body_bytes)
    try:
        body = json.loads(raw_text) if raw_text.strip() else {}
    except Exception:
        body = None

    hdr_retry = None
    for k, v in (resp_headers or {}).items():
        if k.lower() == "retry-after":
            hdr_retry = parse_duration(v)
            if hdr_retry is None:
                try:
                    hdr_retry = float(v)
                except Exception:
                    hdr_retry = None

    # --- quota : jamais confondu avec une faute du document ---
    if _is_quota(http_code, body if isinstance(body, dict) else None, raw_text):
        delay = retry_info_seconds(body if isinstance(body, dict) else None)
        if delay is None:
            delay = hdr_retry  # Retry-After n'est pas garanti : appoint, pas source primaire
        scope = quota_scope(body if isinstance(body, dict) else None, delay)
        qid, qval = quota_violation(body if isinstance(body, dict) else None)
        return _result(False, "quota", http_code=http_code, retry_after_s=delay,
                       raw_error=raw_text[:2000], quota_scope=scope,
                       quota_id=qid, quota_value=qval)

    if http_code is not None and http_code >= 500:
        return _result(False, "http_error", http_code=http_code,
                       retry_after_s=hdr_retry, raw_error=raw_text[:2000])

    if http_code != 200:
        return _result(False, "http_error", http_code=http_code, raw_error=raw_text[:2000])

    if not isinstance(body, dict):
        return _result(False, "invalid", http_code=http_code,
                       raw_error="corps JSON illisible: " + raw_text[:500])

    usage = body.get("usageMetadata") or {}

    pf = body.get("promptFeedback") or {}
    if pf.get("blockReason"):
        return _result(False, "blocked", http_code=http_code, usage=usage,
                       finish_reason=str(pf.get("blockReason")),
                       raw_error="promptFeedback.blockReason=%s" % pf.get("blockReason"))

    cands = body.get("candidates") or []
    if not cands:
        return _result(False, "invalid", http_code=http_code, usage=usage,
                       raw_error="aucun candidate: " + raw_text[:500])

    c0 = cands[0] or {}
    finish = c0.get("finishReason")

    parts = ((c0.get("content") or {}).get("parts")) or []
    text = "".join(p.get("text", "") for p in parts if isinstance(p, dict))

    if finish == "MAX_TOKENS":
        # NE PAS tenter de parser un JSON coupe : on remonte le texte brut tel quel.
        return _result(False, "truncated", text=text or None, http_code=http_code,
                       usage=usage, finish_reason=finish,
                       raw_error="finishReason=MAX_TOKENS")
    if finish in _BLOCKED:
        return _result(False, "blocked", http_code=http_code, usage=usage,
                       finish_reason=finish, raw_error="finishReason=%s" % finish)
    if not text:
        return _result(False, "invalid", http_code=http_code, usage=usage,
                       finish_reason=finish, raw_error="reponse sans texte")

    return _result(True, "ok", text=text, http_code=http_code, usage=usage,
                   finish_reason=finish or "STOP")


# --------------------------------------------------------------------------- #
# submit() — interface unique
# --------------------------------------------------------------------------- #
def _api_key():
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "GEMINI_API_KEY absente de l'environnement "
            "(EnvironmentFile=/etc/llm-wiki/gemini.env, drop-in 70-gemini-key.conf)")
    return key


# Noms COMPLETS obligatoires : l'alias court (ex. "DANGEROUS_CONTENT") provoque
# un 400 silencieux. Corpus technique (admin systeme, reseau, securite) =>
# faux positifs garantis sans BLOCK_NONE.
SAFETY_BLOCK_NONE = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]


# Echelle de repli des niveaux de reflexion. MESURE le 2026-08-26 :
# gemini-3.7-flash REFUSE thinkingLevel=MINIMAL par un 400 INVALID_ARGUMENT
# ("Thinking level MINIMAL is not supported for this model"), alors que LOW
# passe (200). Sans repli, les 20 appels du quota journalier partent en 400 :
# une journee entiere de quota perdue sur un parametre. On degrade donc d'un
# cran automatiquement, une seule fois, plutot que de coder une table de
# modeles qui sera fausse au prochain modele publie.
THINKING_LADDER = ["minimal", "low", "medium"]
_THINKING_UNSUPPORTED = "thinking level"


def thinking_config(model):
    """Le parametre DIFFERE entre familles, ne pas les confondre :
    2.5 -> thinkingConfig.thinkingBudget=0 ; 3.x -> thinkingConfig.thinkingLevel="minimal".
    Renvoie None si la famille est inconnue (on n'envoie alors rien)."""
    m = (model or "").lower()
    if m.startswith("gemini-2.5"):
        return {"thinkingBudget": 0}
    if m.startswith("gemini-3"):
        return {"thinkingLevel": "minimal"}
    return None


def downgrade_thinking(thinking):
    """Niveau immediatement superieur, ou None s'il n'y en a plus."""
    if not isinstance(thinking, dict):
        return None
    lvl = str(thinking.get("thinkingLevel") or "").lower()
    if lvl not in THINKING_LADDER:
        return None
    i = THINKING_LADDER.index(lvl)
    if i + 1 >= len(THINKING_LADDER):
        return None
    return {"thinkingLevel": THINKING_LADDER[i + 1]}


def _thinking_rejected(res):
    """400 INVALID_ARGUMENT portant sur le niveau de reflexion, et lui seul."""
    return (res.get("status") == "http_error"
            and res.get("http_code") == 400
            and _THINKING_UNSUPPORTED in (res.get("raw_error") or "").lower())


def build_payload(prompt, response_schema=None, max_output_tokens=None,
                  temperature=None, safety_settings=None, thinking=None):
    gen = {}
    if response_schema is not None:
        gen["responseMimeType"] = "application/json"
        gen["responseSchema"] = response_schema
    if max_output_tokens:
        gen["maxOutputTokens"] = int(max_output_tokens)
    if temperature is not None:
        gen["temperature"] = float(temperature)
    if thinking:
        gen["thinkingConfig"] = thinking
    payload = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
    if gen:
        payload["generationConfig"] = gen
    if safety_settings:
        payload["safetySettings"] = safety_settings
    return payload


def _submit_interactive(prompt, model, response_schema, max_output_tokens,
                        timeout, transport, temperature,
                        safety_settings=None, thinking=None):
    if not model or model.endswith("-latest"):
        # Un alias mouvant casserait l'idempotence du spool : ID fige obligatoire.
        return _result(False, "invalid",
                       raw_error="modele invalide ou alias -latest interdit: %r" % model)
    url = "%s/%s:generateContent" % (API_ROOT, model)
    headers = {
        "Content-Type": "application/json",
        # La cle voyage en EN-TETE. Jamais dans l'URL : elle fuirait dans les logs.
        "x-goog-api-key": _api_key(),
        "User-Agent": "llm-wiki-submit/%s" % __version__,
    }
    data = json.dumps(build_payload(prompt, response_schema, max_output_tokens,
                                    temperature, safety_settings,
                                    thinking)).encode("utf-8")
    try:
        code, rhdr, rbody = (transport or http_post)(url, headers, data, timeout)
    except TimeoutError as e:
        return _result(False, "timeout", raw_error="timeout: %s" % e)
    except OSError as e:
        return _result(False, "http_error", raw_error="reseau: %s" % e)
    return interpret(code, rhdr, rbody)


class CountTokensError(RuntimeError):
    """countTokens indisponible. L'appelant DOIT alors se rabattre sur une
    estimation par octets, jamais partir du principe que le document est petit."""


def count_tokens(text, *, model, timeout=60.0, transport=None):
    """Mesure a priori du cout d'entree (plan §11, declencheur 1).

    `:countTokens` ne consomme NI le quota de tokens par minute NI le quota de
    requetes de `generateContent` : c'est ce qui permet de mesurer un document
    de 500 000 tokens que `generateContent` refuserait en bloc.
    Renvoie un entier. Leve CountTokensError si l'API ne repond pas exploitable.
    """
    if not model or model.endswith("-latest"):
        raise CountTokensError("modele invalide ou alias -latest interdit: %r" % model)
    url = "%s/%s:countTokens" % (API_ROOT, model)
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": _api_key(),
        "User-Agent": "llm-wiki-submit/%s" % __version__,
    }
    data = json.dumps(
        {"contents": [{"role": "user", "parts": [{"text": text}]}]}
    ).encode("utf-8")
    try:
        code, _hdr, body = (transport or http_post)(url, headers, data, timeout)
    except TimeoutError as e:
        raise CountTokensError("timeout: %s" % e)
    except OSError as e:
        raise CountTokensError("reseau: %s" % e)
    raw = (body or b"").decode("utf-8", "replace")
    if code != 200:
        raise CountTokensError("HTTP %s: %s" % (code, raw[:300]))
    try:
        n = int(json.loads(raw)["totalTokens"])
    except Exception as e:
        raise CountTokensError("reponse illisible (%s): %s" % (e, raw[:300]))
    if n <= 0:
        raise CountTokensError("totalTokens absurde: %r" % n)
    return n


def _submit_agy(prompt, model, timeout):
    """Chemin historique, conserve pour rollback (SUBMIT_MODE=agy)."""
    cmd = [AGY_BIN, "-m", model or os.environ.get("LLM_MODEL", ""), "-p", prompt]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as e:
        return _result(False, "invalid", raw_error="agy introuvable: %s" % e)
    except subprocess.TimeoutExpired:
        return _result(False, "timeout", raw_error="timeout agy")
    out = (p.stdout or "") + (p.stderr or "")
    if _is_quota(None, None, out) or re.search(r"\b429\b", out):
        return _result(False, "quota", raw_error=out[:2000], quota_scope="minute")
    if p.returncode != 0:
        return _result(False, "http_error", http_code=p.returncode, raw_error=out[:2000])
    return _result(True, "ok", text=p.stdout)


def submit(prompt, *, model, response_schema=None, max_output_tokens=None,
           timeout=DEFAULT_TIMEOUT, mode=None, transport=None, temperature=None,
           safety_settings=None, thinking=None):
    """Un appel, aucun pacing, aucun retry. Renvoie le dict de resultat structure.

    safety_settings / thinking : ignores en mode agy (chemin de rollback, l'ID de
    modele y est un alias AGY et le routeur gere ses propres reglages)."""
    mode = mode or os.environ.get("SUBMIT_MODE", "interactive")
    if mode == "interactive":
        res = _submit_interactive(prompt, model, response_schema, max_output_tokens,
                                  timeout, transport, temperature,
                                  safety_settings, thinking)
        # Repli du niveau de reflexion : un modele qui refuse MINIMAL ne doit pas
        # consommer tout son quota journalier en 400. Une seule tentative.
        if _thinking_rejected(res):
            nxt = downgrade_thinking(thinking)
            if nxt is not None:
                res2 = _submit_interactive(prompt, model, response_schema,
                                           max_output_tokens, timeout, transport,
                                           temperature, safety_settings, nxt)
                res2["thinking_downgraded_to"] = nxt.get("thinkingLevel")
                return res2
        return res
    if mode == "batch":
        raise NotImplementedError(
            "SUBMIT_MODE=batch : le Batch API n'est PAS disponible en palier gratuit "
            "(batchGenerateContent -> 400 FAILED_PRECONDITION, verifie sur la cle du "
            "projet, alors que generateContent -> 200 avec la meme cle). Stub assume : "
            "utiliser SUBMIT_MODE=interactive.")
    if mode == "agy":
        return _submit_agy(prompt, model, timeout)
    raise ValueError("SUBMIT_MODE inconnu: %r (interactive|batch|agy)" % mode)


# --------------------------------------------------------------------------- #
# pace.json — etat persistant, AIMD, jitter
# --------------------------------------------------------------------------- #
DEFAULT_PACE = {
    "interval_ms": START_INTERVAL_MS,
    "ok_streak": 0,
    "last_429_at": 0,
    "daily_exhausted_until": 0,
    "ewma_latency_ms": 0,
    "last_run_ok_count": 0,   # appels reussis avant le dernier 429 journalier
    # Seuil de chunking decouvert a l'execution (plan 11). 0 = jamais abaisse :
    # on utilise alors CHUNK_MIN_TOKENS de /etc/default/llm-wiki. JAMAIS remonte
    # automatiquement, seule une mesure explicite le releve.
    "chunk_min_tokens": 0,
    "updated_at": 0,
}

# Sous-etat PAR MODELE, persiste dans pace.json sous la cle "models" :
#   { "gemini-2.5-flash": {"day": "2026-08-26", "count": 12,
#                          "strikes": 0, "exhausted_until": 0} }
# "day" est la date PACIFIQUE : c'est le fuseau du reset RPD du palier gratuit,
# pas UTC. Un changement de jour pacifique remet count/strikes/exhausted_until a zero.
DEFAULT_MODEL_STATE = {"day": "", "count": 0, "strikes": 0, "exhausted_until": 0}


def _pacific_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(PACIFIC_TZ)
    except Exception:
        # Repli si tzdata manque : PST fixe. Approximation assumee et journalisee,
        # elle ne peut decaler le reset que d'une heure en periode d'ete.
        return datetime.timezone(datetime.timedelta(hours=-8))


def pacific_day(ts=None):
    """Date du reset RPD, 'YYYY-MM-DD' en fuseau pacifique."""
    t = time.time() if ts is None else ts
    return datetime.datetime.fromtimestamp(float(t), _pacific_tz()).strftime("%Y-%m-%d")


def next_pacific_midnight(ts=None):
    """Epoch du prochain minuit pacifique — borne haute naturelle d'un blocage RPD."""
    t = time.time() if ts is None else ts
    tz = _pacific_tz()
    now = datetime.datetime.fromtimestamp(float(t), tz)
    nxt = (now + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return nxt.timestamp()


def _clamp_interval(v):
    try:
        v = int(v)
    except Exception:
        v = START_INTERVAL_MS
    return max(MIN_INTERVAL_MS, min(MAX_INTERVAL_MS, v))


def load_pace(path=None):
    """Tolerant : fichier absent, illisible ou corrompu => defauts, jamais d'exception."""
    path = path or PACE_PATH
    st = dict(DEFAULT_PACE)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            for k in DEFAULT_PACE:
                if k in data and isinstance(data[k], (int, float)):
                    st[k] = data[k]
            models = data.get("models")
            if isinstance(models, dict):
                for name, ms in models.items():
                    if isinstance(ms, dict):
                        clean = dict(DEFAULT_MODEL_STATE)
                        for f, dv in DEFAULT_MODEL_STATE.items():
                            v = ms.get(f)
                            if isinstance(v, type(dv)) or (
                                    isinstance(dv, int) and isinstance(v, (int, float))):
                                clean[f] = v
                        st.setdefault("models", {})[str(name)] = clean
    except Exception:
        pass  # charabia, JSON tronque, droits : on repart sur les defauts
    st["interval_ms"] = _clamp_interval(st["interval_ms"])
    st.setdefault("models", {})
    return st


def save_pace(state, path=None):
    """Ecriture atomique : temporaire dans le meme repertoire + os.replace."""
    path = path or PACE_PATH
    state = dict(state)
    state["updated_at"] = int(time.time())
    d = os.path.dirname(path) or "."
    tmp = os.path.join(d, ".pace.json.tmp.%d" % os.getpid())
    try:
        os.makedirs(d, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        try:
            os.chmod(path, 0o664)
        except OSError:
            pass
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False
    return True


def jitter(seconds):
    """Jitter toujours, jamais de cadence nue : evite la resynchronisation des retries."""
    if seconds <= 0:
        return 0.0
    return random.uniform(0, JITTER_RATIO * seconds)


class Pacer:
    """Boucle de pacing adaptatif AIMD au-dessus de submit().

    QUOTA_SAFETY_MARGIN : on s'arrete proprement (SafetyMarginReached) a
    last_run_ok_count - QUOTA_SAFETY_MARGIN appels reussis, pour ne pas finir
    chaque run sur un 429. Si le run se termine sans quota, last_run_ok_count
    remonte de 10 % pour re-tester la borne au run suivant.
    """

    def __init__(self, path=None, sleeper=None, clock=None, margin=None):
        self.path = path or PACE_PATH
        self.state = load_pace(self.path)
        self.sleep = sleeper or time.sleep
        self.clock = clock or time.time
        self.margin = QUOTA_SAFETY_MARGIN if margin is None else margin
        self.ok_count = 0
        self.quota_hit = False
        self.quota_reset_s = 0.0

    # -- etat -------------------------------------------------------------- #
    def persist(self):
        return save_pace(self.state, self.path)

    def model_state(self, model, now=None):
        """Sous-etat du modele, remis a zero au passage de minuit PACIFIQUE."""
        now = self.clock() if now is None else now
        models = self.state.setdefault("models", {})
        key = str(model or "")
        st = models.get(key)
        day = pacific_day(now)
        if not isinstance(st, dict):
            st = dict(DEFAULT_MODEL_STATE)
        if st.get("day") != day:
            # Nouveau jour pacifique : le quota RPD est reparti a zero.
            st = dict(DEFAULT_MODEL_STATE)
            st["day"] = day
        models[key] = st
        return st

    def model_blocked_for(self, model, now=None):
        """Secondes restantes avant que `model` redevienne utilisable (0 si libre)."""
        now = self.clock() if now is None else now
        st = self.model_state(model, now)
        return max(0.0, float(st.get("exhausted_until") or 0) - now)

    def mark_model_exhausted(self, model, reset_s, now=None):
        """Arme le blocage du modele, borne au prochain minuit pacifique."""
        now = self.clock() if now is None else now
        st = self.model_state(model, now)
        until = now + max(0.0, float(reset_s))
        try:
            st["exhausted_until"] = int(min(until, next_pacific_midnight(now)))
        except Exception:
            st["exhausted_until"] = int(until)
        return st["exhausted_until"]

    def budget(self):
        """Nombre d'appels autorises ce run, ou None si aucune borne connue."""
        base = int(self.state.get("last_run_ok_count") or 0)
        if base <= 0:
            return None
        return max(1, base - self.margin)

    def _on_ok(self, model=None):
        self.ok_count += 1
        if model is not None:
            ms = self.model_state(model)
            ms["count"] = int(ms.get("count", 0)) + 1
            ms["strikes"] = 0     # un succes invalide toute suspicion d'epuisement
        self.state["ok_streak"] = int(self.state.get("ok_streak", 0)) + 1
        if self.state["ok_streak"] >= AI_STREAK:
            self.state["interval_ms"] = _clamp_interval(
                self.state["interval_ms"] - AI_STEP_MS)
            self.state["ok_streak"] = 0

    def _on_quota(self, res):
        self.state["ok_streak"] = 0
        self.state["last_429_at"] = int(self.clock())
        self.state["interval_ms"] = _clamp_interval(
            self.state["interval_ms"] * MD_FACTOR)

    def finish_run(self, quota=False):
        """A appeler en fin de run. Persiste pace.json AVANT tout exit 75."""
        if quota:
            self.state["last_run_ok_count"] = self.ok_count
        else:
            base = int(self.state.get("last_run_ok_count") or 0)
            if self.ok_count >= max(1, base - self.margin) or base == 0:
                # run propre : on remonte la borne de 10 % pour la re-tester
                # +10 %, avec un plancher de +1 : sur de petits entiers, round(n*1.1)
                # vaudrait n et la borne resterait bloquee a jamais.
                grown = max(base + 1, int(round(base * 1.1))) if base else self.ok_count
                self.state["last_run_ok_count"] = max(self.ok_count, grown)
        self.persist()

    # -- boucle ------------------------------------------------------------ #
    def run_one(self, prompt, **kw):
        b = self.budget()
        if b is not None and self.ok_count >= b:
            raise SafetyMarginReached(
                "budget atteint: %d appels (borne %d = %d observes - marge %d)"
                % (self.ok_count, b, int(self.state["last_run_ok_count"]), self.margin))

        # Gate PAR MODELE : un modele epuise ne condamne plus les autres (cascade).
        blocked = self.model_blocked_for(kw.get("model"))
        if blocked > 0:
            raise QuotaExhausted(blocked, "daily",
                                 _result(False, "quota", quota_scope="daily",
                                         raw_error="quota journalier du modele %r encore "
                                                   "actif (%ds)" % (kw.get("model"),
                                                                    int(blocked))))

        consecutive_429 = 0
        transient = 0
        while True:
            base_s = self.state["interval_ms"] / 1000.0
            self.sleep(base_s + jitter(base_s))

            t0 = self.clock()
            res = submit(prompt, **kw)
            lat = max(0.0, (self.clock() - t0)) * 1000.0

            st = res["status"]
            if st == "ok":
                prev = float(self.state.get("ewma_latency_ms") or 0)
                self.state["ewma_latency_ms"] = int(
                    lat if prev <= 0 else 0.7 * prev + 0.3 * lat)
                self._on_ok(kw.get("model"))
                self.persist()
                return res

            if st == "quota":
                # Un quota n'incremente JAMAIS attempts cote appelant.
                self._on_quota(res)
                self.persist()  # AVANT toute sortie, jamais dans le trap
                delay = res.get("retry_after_s") or 0.0
                if res.get("quota_scope") == "daily":
                    model = kw.get("model")
                    ms = self.model_state(model)
                    ms["strikes"] = int(ms.get("strikes", 0)) + 1
                    qval = res.get("quota_value")
                    # Trois voies de CONFIRMATION d'un epuisement journalier :
                    reached = (qval is not None
                               and int(ms.get("count", 0)) >= int(qval))
                    long_delay = bool(delay) and delay > DAILY_DELAY_THRESHOLD_S
                    strikes_out = ms["strikes"] >= DAILY_CONFIRM_STRIKES
                    confirmed = reached or long_delay or strikes_out

                    if not confirmed and delay and delay <= DAILY_CONFIRM_MAX_WAIT_S:
                        # retryDelay court sur un quotaId "PerDay" : c'est une
                        # information a SUIVRE, pas a ignorer. On dort ce qui est
                        # annonce et on rejoue le meme prompt. attempts INCHANGE.
                        self.persist()
                        w = max(float(delay), 1.0)
                        self.sleep(w + jitter(w))
                        continue

                    if confirmed:
                        reset = max(delay or 0.0, RESUME_DELAY_SECONDS)
                    else:
                        # Non confirme mais attente trop longue pour etre absorbee ici :
                        # on rend la main SANS plancher, la cascade prendra le relais.
                        reset = float(delay or 0.0)
                    self.mark_model_exhausted(model, reset)
                    self.state["daily_exhausted_until"] = int(self.clock() + reset)
                    self.finish_run(quota=True)
                    raise QuotaExhausted(reset, "daily", res)
                consecutive_429 += 1
                if consecutive_429 <= MAX_LOCAL_RETRIES:
                    w = max(delay, self.state["interval_ms"] / 1000.0)
                    self.sleep(w + jitter(w))
                    continue
                self.finish_run(quota=True)
                raise QuotaExhausted(delay, "minute", res)

            if st in ("timeout",) or (st == "http_error" and
                                      (res.get("http_code") or 0) >= 500) or \
                    (st == "http_error" and res.get("http_code") is None):
                transient += 1
                if transient > MAX_TRANSIENT_RETRIES:
                    return res      # l'appelant fera attempts+1
                w = TRANSIENT_BASE_S * (2 ** (transient - 1))
                self.sleep(w + jitter(w))
                continue

            return res  # invalid / blocked / truncated / 4xx : faute de la requete


# --------------------------------------------------------------------------- #
# Cascade de modeles — qualite decroissante, PAS un tourniquet
# --------------------------------------------------------------------------- #
class AllModelsExhausted(Exception):
    """Tous les modeles de la cascade sont epuises pour la journee : exit 75."""

    def __init__(self, reset_s, detail=None):
        super().__init__("cascade epuisee, reprise dans %ds" % int(reset_s))
        self.reset_s = reset_s
        self.detail = detail or {}


def cascade_order(spec=None):
    """Ordre de consommation, du meilleur au moins bon.

    On ne tourne PAS en rond : on consomme le meilleur modele disponible jusqu'a
    epuisement de son quota journalier, puis on descend d'un cran. Un tourniquet
    aveugle diluerait la qualite du graphe en donnant autant de documents a un
    modele faible qu'au meilleur."""
    spec = spec if spec is not None else os.environ.get("EXTRACT_MODEL_CASCADE", "")
    if isinstance(spec, (list, tuple)):
        items = list(spec)
    else:
        items = [x.strip() for x in str(spec).split(",")]
    items = [x for x in items if x]
    if not items:
        items = list(DEFAULT_CASCADE)
    out = []
    for m in items:
        if m.endswith("-latest"):
            # Alias mouvant : proscrit (casserait l'idempotence du spool).
            continue
        if m not in out:
            out.append(m)
    return out


class ModelCascade:
    """Selectionne le meilleur modele encore disponible, d'apres pace.json."""

    def __init__(self, pacer, order=None):
        self.pacer = pacer
        self.order = cascade_order(order)

    def available(self, now=None):
        now = self.pacer.clock() if now is None else now
        return [m for m in self.order if self.pacer.model_blocked_for(m, now) <= 0]

    def current(self, now=None):
        """Meilleur modele disponible, ou AllModelsExhausted."""
        now = self.pacer.clock() if now is None else now
        for m in self.order:
            if self.pacer.model_blocked_for(m, now) <= 0:
                return m
        waits = [self.pacer.model_blocked_for(m, now) for m in self.order]
        reset = min(waits) if waits else RESUME_DELAY_SECONDS
        raise AllModelsExhausted(reset, {
            "order": list(self.order),
            "blocked_until": {m: int(self.pacer.model_state(m, now).get(
                "exhausted_until") or 0) for m in self.order},
            "day": pacific_day(now),
        })

    def counts(self, now=None):
        now = self.pacer.clock() if now is None else now
        return {m: int(self.pacer.model_state(m, now).get("count", 0))
                for m in self.order}

    def run_one(self, prompt, **kw):
        """Un appel, en descendant la cascade a chaque epuisement journalier.

        Renvoie (result, model_utilise). Les quotas PAR MINUTE restent absorbes
        par le Pacer : ils ne font pas changer de modele."""
        while True:
            model = self.current()
            kw2 = dict(kw)
            kw2["model"] = model
            try:
                return self.pacer.run_one(prompt, **kw2), model
            except QuotaExhausted as e:
                if e.scope != "daily":
                    raise
                # Le modele vient d'etre marque epuise : on redescend d'un cran.
                if self.pacer.model_blocked_for(model) <= 0:
                    # Securite anti-boucle : si le marquage n'a pas pris, on force.
                    self.pacer.mark_model_exhausted(model, max(e.reset_s, 60.0))
                    self.pacer.persist()
                continue


# --------------------------------------------------------------------------- #
# CLI de test
# --------------------------------------------------------------------------- #
def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Client de soumission LLM (llm-wiki, lot 2).")
    ap.add_argument("--model", default=os.environ.get("EXTRACT_MODEL",
                                                      "gemini-2.5-flash-lite"))
    ap.add_argument("--prompt-file")
    ap.add_argument("--prompt")
    ap.add_argument("--schema-file")
    ap.add_argument("--max-output-tokens", type=int)
    ap.add_argument("--temperature", type=float)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    ap.add_argument("--mode", default=os.environ.get("SUBMIT_MODE", "interactive"))
    ap.add_argument("--pace-file", default=PACE_PATH)
    ap.add_argument("--no-pace", action="store_true",
                    help="appel direct, sans pacing ni pace.json")
    ap.add_argument("--json", action="store_true", help="resultat complet en JSON")
    ap.add_argument("--show-pace", action="store_true",
                    help="affiche pace.json et sort")
    args = ap.parse_args(argv)

    if args.show_pace:
        print(json.dumps(load_pace(args.pace_file), indent=2, sort_keys=True))
        return 0

    if args.prompt_file:
        with open(args.prompt_file, "r", encoding="utf-8") as fh:
            prompt = fh.read()
    elif args.prompt is not None:
        prompt = args.prompt
    else:
        prompt = sys.stdin.read()

    schema = None
    if args.schema_file:
        with open(args.schema_file, "r", encoding="utf-8") as fh:
            schema = json.load(fh)

    kw = dict(model=args.model, response_schema=schema,
              max_output_tokens=args.max_output_tokens,
              timeout=args.timeout, mode=args.mode, temperature=args.temperature)

    try:
        if args.no_pace:
            res = submit(prompt, **kw)
        else:
            p = Pacer(path=args.pace_file)
            res = p.run_one(prompt, **kw)
            p.finish_run(quota=False)
    except NotImplementedError as e:
        print("NotImplementedError: %s" % e, file=sys.stderr)
        return 3
    except SafetyMarginReached as e:
        print("arret propre (QUOTA_SAFETY_MARGIN): %s" % e, file=sys.stderr)
        return 0
    except QuotaExhausted as e:
        print("QUOTA_HIT=1", file=sys.stderr)
        print("QUOTA_RESET_S=%d" % int(e.reset_s), file=sys.stderr)
        return 75

    if args.json:
        print(json.dumps(res, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        if res["ok"]:
            print(res["text"])
        else:
            print("status=%s http=%s retry_after=%s err=%s"
                  % (res["status"], res["http_code"], res["retry_after_s"],
                     (res["raw_error"] or "")[:300]), file=sys.stderr)
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
