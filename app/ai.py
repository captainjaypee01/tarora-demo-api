from __future__ import annotations
"""
Minimal AI insights (Responses API only) with optional prompt template + debug.

Env:
  OPENAI_API_KEY=...                # required
  OPENAI_MODEL=gpt-5                # or gpt-5-mini (default below)
  OPENAI_PROMPT_ID=pmpt_xxx         # optional; if set we use `prompt={...}` path
  OPENAI_PROMPT_VERSION=2           # optional; used only when PROMPT_ID is set
  INSIGHTS_DOWNSAMPLE=150           # cap raw samples passed to LLM (default 150)
  INSIGHTS_INCLUDE_SAMPLES=0/1      # include raw samples in variables/text (default 0)
  INSIGHTS_CACHE_TTL=120            # seconds

Return:
  generate_insight(..., debug=False) -> str (Markdown)
  generate_insight(..., debug=True)  -> dict { summary, meta... }
"""

import json, os, time, math
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Literal, TypedDict

from sqlmodel import select

from .db import get_session
from .models import Telemetry

# Event model optional
try:
    from .models import Event  # type: ignore
    HAS_EVENT = True
except Exception:
    Event = None  # type: ignore
    HAS_EVENT = False

# OpenAI Responses API
try:
    from openai import OpenAI
    from openai import BadRequestError, RateLimitError, APITimeoutError, APIStatusError
except Exception:
    OpenAI = None  # type: ignore
    BadRequestError = RateLimitError = APITimeoutError = APIStatusError = Exception  # type: ignore

# ---------------- config / cache ----------------
INSIGHTS_DOWNSAMPLE = int(os.getenv("INSIGHTS_DOWNSAMPLE", "150"))
INSIGHTS_INCLUDE_SAMPLES = os.getenv("INSIGHTS_INCLUDE_SAMPLES", "0") == "1"
INSIGHTS_CACHE_TTL = int(os.getenv("INSIGHTS_CACHE_TTL", "120"))

_CACHE: Dict[str, tuple[float, str]] = {}
_CACHE_LOCK = Lock()

def _cache_get(k: str) -> str | None:
    now = time.time()
    with _CACHE_LOCK:
        it = _CACHE.get(k)
        if not it: return None
        exp, val = it
        if now <= exp: return val
        _CACHE.pop(k, None); return None

def _cache_set(k: str, v: str, ttl: int) -> None:
    with _CACHE_LOCK:
        _CACHE[k] = (time.time() + ttl, v)

# ---------------- utils ----------------
def _utcnow_iso() -> str: return datetime.now(timezone.utc).isoformat()
def _is_num(x: Any) -> bool: return isinstance(x, (int, float)) and not isinstance(x, bool) and not math.isnan(x)

def _downsample(points: List[dict], max_points: int) -> List[dict]:
    if len(points) <= max_points: return points
    step = max(1, len(points)//max_points)
    return points[::step]

def _numeric_stats(vals: List[float]) -> Dict[str, Any]:
    n = len(vals)
    if n == 0: return {"count": 0}
    mean = sum(vals)/n
    var = sum((v-mean)**2 for v in vals)/n
    std = var**0.5
    slope = 0.0
    if n >= 2:
        num = sum((i-(n-1)/2)*(v-mean) for i,v in enumerate(vals))
        den = sum((i-(n-1)/2)**2 for i in range(n)) or 1.0
        slope = num/den
    return {
        "count": n, "min": min(vals), "max": max(vals), "mean": mean, "std": std,
        "slope": slope, "spikes_gt_3sigma": sum(1 for v in vals if v > mean + 3*std),
    }

def _telemetry_stats(points: List[dict]) -> Dict[str, Any]:
    metrics: Dict[str, List[float]] = {}
    door_bools: List[bool] = []
    for p in points:
        data = p.get("data") or {}
        for k, v in data.items():
            if _is_num(v): metrics.setdefault(k, []).append(float(v))
            if k == "open" and isinstance(v, bool): door_bools.append(v)
    return {
        "metrics": {k: _numeric_stats(vs) for k, vs in metrics.items()},
        "door": {
            "has_door": len(door_bools) > 0,
            "open_count": sum(1 for b in door_bools if b is True),
            "closed_count": sum(1 for b in door_bools if b is False),
        },
    }

def _events_summary(events: List[dict]) -> Dict[str, Any]:
    by_level, by_event, last_alarm = {}, {}, None
    for e in events:
        lvl = (e.get("level") or "info").lower()
        name = e.get("event") or "event"
        by_level[lvl] = by_level.get(lvl, 0) + 1
        by_event[name] = by_event.get(name, 0) + 1
        if lvl in ("alarm", "error"): last_alarm = e.get("ts") or last_alarm
    return {"by_level": by_level, "by_event": by_event, "last_alarm_ts": last_alarm}

def _fallback_markdown(telem: list[dict], events: list[dict]) -> str:
    # short, professional fallback
    stats_t = _telemetry_stats(telem) if telem else {"metrics": {}, "door": {"has_door": False, "open_count": 0, "closed_count": 0}}
    stats_e = _events_summary(events) if events else {"by_level": {}, "by_event": {}, "last_alarm_ts": None}
    parts = [
        "## Summary",
        f"Window contains {len(telem)} telemetry points and {len(events)} event(s).",
        "",
        "## Trends & Anomalies",
    ]
    for k, s in sorted(stats_t["metrics"].items()):
        if s.get("count", 0) >= 3:
            trend = "rising" if s["slope"] > 0.01 else "falling" if s["slope"] < -0.01 else "flat"
            extra = f" ({s['spikes_gt_3sigma']} spike(s) > 3σ)" if s["spikes_gt_3sigma"] else ""
            parts.append(f"- **{k}**: min {s['min']:.2f}, max {s['max']:.2f}, mean {s['mean']:.2f}, trend {trend}{extra}.")
    parts += [
        "",
        "## Events & Faults",
        f"- Counts by level: {json.dumps(stats_e['by_level']) or '{}'}",
        f"- Most recent alarm: {stats_e['last_alarm_ts'] or '—'}",
        "",
        "## Door Activity (if any)",
        f"- open={stats_t['door'].get('open_count', 0)}, closed={stats_t['door'].get('closed_count', 0)}",
        "",
        "## Recommendations",
        "- Review thresholds if spikes are frequent; add debounce if noisy.",
        "- Validate sensor calibration if trends drift.",
        "- Correlate alarms with usage or environment.",
        "",
        "## TL;DR",
        "Stable unless noted; investigate alarms and consider threshold tuning.",
    ]
    return "\n".join(parts)

# ---------------- prompt construction ----------------
AIR_QUALITY_HINTS = {
    "co2_ppm": {"ok": "<=1000", "warn": "1000-1500", "bad": ">1500"},
    "pm2_5":   {"ok": "<=12", "warn": "12-35", "bad": ">35"},
    "pm10":    {"ok": "<=54", "warn": "55-154", "bad": ">=155"},
    "voc_index": {"ok": "<=2", "warn": "3-4", "bad": ">=5"},
    "humidity": {"ok": "30-60%"},
    "temp_c":   {"note": "comfort ~20-26°C"},
    "aqi":      {"note": "lower is better"},
}

def _build_inline_prompt(scope: str, telem: List[dict], events: List[dict], stats: Dict[str, Any]) -> str:
    body = {
        "generated_at": _utcnow_iso(),
        "scope": scope,
        "telemetry_count": len(telem),
        "events_count": len(events),
        "stats": stats,
        "reference_bands": AIR_QUALITY_HINTS,
        "sample_telemetry": _downsample(telem, INSIGHTS_DOWNSAMPLE) if INSIGHTS_INCLUDE_SAMPLES else [],
        "sample_events": (events[-INSIGHTS_DOWNSAMPLE:] if len(events) > INSIGHTS_DOWNSAMPLE else events) if INSIGHTS_INCLUDE_SAMPLES else [],
        "format": [
            "## Summary",
            "## Trends & Anomalies",
            "## Events & Faults",
            "## Door Activity (if any)",
            "## Health & Data Quality",
            "## Recommendations",
            "## TL;DR (1 line)",
        ],
        "style": "Short bullets, pragmatic. Avoid hedging and repetition.",
    }
    header = (
        "You are a senior IoT data analyst. Analyze telemetry + events for the scope below.\n"
        "Be concise and practical. Use the provided reference bands as rough hints."
    )
    return header + "\n\nDATA:\n" + json.dumps(body, ensure_ascii=False)

def _build_template_variables(scope: str, telem: List[dict], events: List[dict], stats: Dict[str, Any]) -> Dict[str, str]:
    # Variables are strings; keep them compact
    return {
        "generated_at": _utcnow_iso(),
        "scope": scope,
        "telemetry_count": str(len(telem)),
        "events_count": str(len(events)),
        "stats_json": json.dumps(stats, ensure_ascii=False),
        "reference_bands_json": json.dumps(AIR_QUALITY_HINTS, ensure_ascii=False),
        "sample_telemetry_json": json.dumps(_downsample(telem, INSIGHTS_DOWNSAMPLE) if INSIGHTS_INCLUDE_SAMPLES else [], ensure_ascii=False),
        "sample_events_json": json.dumps((events[-INSIGHTS_DOWNSAMPLE:] if len(events) > INSIGHTS_DOWNSAMPLE else events) if INSIGHTS_INCLUDE_SAMPLES else [], ensure_ascii=False),
    }

# ---------------- public entry ----------------
class InsightMeta(TypedDict, total=False):
    path: Literal["responses"]
    model: str
    used_prompt_template: bool
    include_samples: bool
    downsample: int
    telemetry_count: int
    events_count: int
    cache_key: str
    generated_at: str
    error: dict

def generate_insight(device_id: str | None, horizon_minutes: int, debug: bool = False):
    key = f"rx::v1::{device_id or 'ALL'}::{horizon_minutes}"
    cached = _cache_get(key)
    if cached and not debug:
        return cached

    since = datetime.utcnow() - timedelta(minutes=horizon_minutes)

    with get_session() as s:
        st = select(Telemetry).where(Telemetry.ts >= since)
        if device_id: st = st.where(Telemetry.device_id == device_id)
        telem_rows = s.exec(st.order_by(Telemetry.ts.asc()).limit(2000)).all()
        telem = [{"device_id": r.device_id, "ts": r.ts.isoformat(), "data": r.data} for r in telem_rows]

        events: List[dict] = []
        if HAS_EVENT and Event is not None:
            se = select(Event).where(Event.ts >= since)
            if device_id: se = se.where(Event.device_id == device_id)
            for r in s.exec(se.order_by(Event.ts.asc()).limit(2000)).all():
                events.append({
                    "device_id": r.device_id, "ts": r.ts.isoformat(),
                    "level": getattr(r, "level", "info"),
                    "event": getattr(r, "event", "event"),
                    "details": getattr(r, "details", {}) or {}
                })

    if not telem and not events:
        text = _fallback_markdown(telem, events)
        _cache_set(key, text, INSIGHTS_CACHE_TTL)
        return {"summary": text, "path": "responses", "model": os.getenv("OPENAI_MODEL","gpt-5-mini"),
                "used_prompt_template": False, "include_samples": INSIGHTS_INCLUDE_SAMPLES,
                "downsample": INSIGHTS_DOWNSAMPLE, "telemetry_count": 0, "events_count": 0,
                "cache_key": key, "generated_at": _utcnow_iso()} if debug else text

    stats_t = _telemetry_stats(telem)
    stats_e = _events_summary(events) if events else {"by_level": {}, "by_event": {}, "last_alarm_ts": None}
    stats = {"metrics": stats_t["metrics"], "door": stats_t["door"], "events": stats_e}

    scope = f"device `{device_id}`" if device_id else "all devices"

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    prompt_id = os.getenv("OPENAI_PROMPT_ID", "").strip()
    prompt_version = os.getenv("OPENAI_PROMPT_VERSION", "").strip()

    if not OpenAI or not api_key:
        text = _fallback_markdown(telem, events)
        return {"summary": text, "path": "responses", "model": model, "used_prompt_template": False,
                "include_samples": INSIGHTS_INCLUDE_SAMPLES, "downsample": INSIGHTS_DOWNSAMPLE,
                "telemetry_count": len(telem), "events_count": len(events),
                "cache_key": key, "generated_at": _utcnow_iso(),
                "error": {"type": "config", "message": "OPENAI_API_KEY missing"}} if debug else text

    # Build payload: template path (preferred if id provided) or inline input
    use_template = bool(prompt_id)
    if use_template:
        variables = _build_template_variables(scope, telem, events, stats)
        prompt_obj: Dict[str, Any] = {"id": prompt_id, "variables": variables}
        if prompt_version:
            prompt_obj["version"] = prompt_version
    else:
        inline_prompt = _build_inline_prompt(scope, telem, events, stats)

    client = OpenAI(api_key=api_key, max_retries=0)

    try:
        if use_template:
            resp = client.responses.create(model=model, prompt=prompt_obj)
        else:
            resp = client.responses.create(model=model, input=inline_prompt)

        text = getattr(resp, "output_text", None) or ""
        if not text:
            # robust parse
            try:
                parts: List[str] = []
                for b in getattr(resp, "output", []) or []:
                    for c in (b.get("content") or []):
                        if c.get("type") == "output_text":
                            parts.append(c.get("text") or "")
                text = "\n".join(parts).strip()
            except Exception:
                text = ""

        if not text:
            text = _fallback_markdown(telem, events)

        _cache_set(key, text, INSIGHTS_CACHE_TTL)

        if debug:
            return {
                "summary": text,
                "path": "responses",
                "model": model,
                "used_prompt_template": use_template,
                "include_samples": INSIGHTS_INCLUDE_SAMPLES,
                "downsample": INSIGHTS_DOWNSAMPLE,
                "telemetry_count": len(telem),
                "events_count": len(events),
                "cache_key": key,
                "generated_at": _utcnow_iso(),
            }
        return text

    except BadRequestError as e:
        # Surface exact server message to help with template/version/variables issues
        text = _fallback_markdown(telem, events)
        print(e)
        if debug:
            body = getattr(getattr(e, "response", None), "json", lambda: {})()
            return {
                "summary": text,
                "path": "responses",
                "model": model,
                "used_prompt_template": use_template,
                "include_samples": INSIGHTS_INCLUDE_SAMPLES,
                "downsample": INSIGHTS_DOWNSAMPLE,
                "telemetry_count": len(telem),
                "events_count": len(events),
                "cache_key": key,
                "generated_at": _utcnow_iso(),
                "error": {"type": "BadRequestError", "message": str(getattr(e, "message", e)), "body": body},
            }
        return text
    except (RateLimitError, APITimeoutError, APIStatusError, Exception):
        text = _fallback_markdown(telem, events)
        if debug:
            return {
                "summary": text, "path": "responses", "model": model, "used_prompt_template": use_template,
                "include_samples": INSIGHTS_INCLUDE_SAMPLES, "downsample": INSIGHTS_DOWNSAMPLE,
                "telemetry_count": len(telem), "events_count": len(events),
                "cache_key": key, "generated_at": _utcnow_iso(),
                "error": {"type": "RuntimeError", "message": "LLM call failed; using fallback"},
            }
        return text
