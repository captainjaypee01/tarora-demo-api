from datetime import datetime, timedelta
from sqlmodel import select
from .db import get_session
from .models import Telemetry
from .settings import settings

try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


def _rule_summary(points: list[dict]) -> str:
    if not points:
        return "No telemetry in the selected window."
    keys = set()
    for p in points:
        keys.update(p.get("data", {}).keys())
    msg = [f"Analyzed {len(points)} points across metrics: {', '.join(sorted(keys)) or 'n/a'}."]
    for k in sorted(keys):
        vals = [p.get("data", {}).get(k) for p in points if isinstance(p.get("data", {}).get(k), (int, float))]
        if len(vals) < 3:
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = var ** 0.5
        spikes = [v for v in vals if v > mean + 3 * std]
        if spikes:
            msg.append(f"Metric '{k}': {len(spikes)} spike(s) > 3σ; check thresholds/environment.")
    if len(msg) == 1:
        msg.append("No strong anomalies detected.")
    return " ".join(msg)


def generate_insight(device_id: str | None, horizon_minutes: int) -> str:
    since = datetime.utcnow() - timedelta(minutes=horizon_minutes)
    with get_session() as session:
        stmt = select(Telemetry).where(Telemetry.ts >= since)
        if device_id:
            stmt = stmt.where(Telemetry.device_id == device_id)
        stmt = stmt.order_by(Telemetry.ts.asc()).limit(500)
        rows = session.exec(stmt).all()
        points = [{"device_id": r.device_id, "ts": r.ts.isoformat(), "data": r.data} for r in rows]

    if settings.openai_api_key and OpenAI is not None:
        try:
            print(settings.openai_api_key)
            client = OpenAI(api_key=settings.openai_api_key)
            prompt = (
                "Summarize anomalies, trends, and practical checks within the following IoT telemetry JSON list. Be concise.\n\n"
                + str(points)[:20000]
            )
            resp = client.responses.create(
                model=settings.openai_model,
                reasoning={"effort": "high"},
                instructions="Talk like a Senior Data Analyst",
                input=prompt,
            )
            print(resp)
            print(f"[INFO] {resp}", flush=True)
            return resp.choices[0].message.content or _rule_summary(points)
        except Exception as e:
            print(e)
            return _rule_summary(points)
    return _rule_summary(points)