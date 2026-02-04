from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from transformer import run_transform

app = FastAPI()


# ─────────────────────────────────────────────────────────
# Endpoint actual: 1 plataforma
# ─────────────────────────────────────────────────────────
class TransformRequest(BaseModel):
    client_key: str
    platform: str
    gcs_path: str
    project_id: str


@app.post("/transform")
def transform(req: TransformRequest):
    try:
        rows = run_transform(
            gcs_path=req.gcs_path,
            client_key=req.client_key,
            platform=req.platform,
            project_id=req.project_id,
        )
        return {
            "status": "ok",
            "rows_upserted": rows,
            "client_key": req.client_key,
            "platform": req.platform,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────
# NUEVO: Transformar múltiples plataformas en 1 llamada
# ─────────────────────────────────────────────────────────

SOURCE_MAP = {
    "BCRA": "bcra",
    "Google Analytics": "ga",
    "GA4": "ga",
    "Google Merchant": "merchant",
    "Merchant": "merchant",
    "Meta Ads": "meta-ads",
    "Meta": "meta-ads",
    "Instagram": "ig",
    "IG": "ig",
    "Tienda Nube": "tn",
    "TiendaNube": "tn",
    "TN": "tn",
    "TikTok": "tiktok",
    "Tiktok": "tiktok",
    # "Google Ads": "google-ads"  # por ahora lo excluimos
}

EXCLUDE_DEFAULT = {"google ads", "google-ads", "gads"}


def _normalize_platform(p: str) -> Optional[str]:
    if not p:
        return None
    p = str(p).strip()

    # mapeo por nombre “humano”
    if p in SOURCE_MAP:
        return SOURCE_MAP[p]

    # si ya viene “normalizado”
    low = p.lower().strip()
    aliases = {
        "meta-ads": "meta-ads",
        "ga": "ga",
        "tn": "tn",
        "ig": "ig",
        "bcra": "bcra",
        "merchant": "merchant",
        "tiktok": "tiktok",
        "google ads": "google-ads",
        "google-ads": "google-ads",
    }
    return aliases.get(low)


class TransformAllRequest(BaseModel):
    client_key: str
    project_id: str
    # podés mandar cualquiera de las dos:
    syncedSources: Optional[List[str]] = None
    platforms: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    bucket: str = "loopi-data-dev"


@app.post("/transform_all")
def transform_all(req: TransformAllRequest):
    try:
        raw = req.platforms or req.syncedSources or []
        if not raw:
            return {
                "status": "ok",
                "client_key": req.client_key,
                "results": [],
                "note": "No platforms provided",
            }

        exclude = {x.lower().strip() for x in (req.exclude or [])}
        exclude |= EXCLUDE_DEFAULT

        # normalizar + filtrar + dedupe con orden
        wanted: List[str] = []
        seen = set()
        for item in raw:
            plat = _normalize_platform(item)
            if not plat:
                continue

            # excluir por nombre humano o por slug
            if item.lower().strip() in exclude or plat.lower() in exclude:
                continue

            if plat not in seen:
                wanted.append(plat)
                seen.add(plat)

        results: List[Dict[str, Any]] = []
        for plat in wanted:
            gcs_path = f"gs://{req.bucket}/{req.client_key}/{plat}/snapshot-latest.json"
            try:
                rows = run_transform(
                    gcs_path=gcs_path,
                    client_key=req.client_key,
                    platform=plat,
                    project_id=req.project_id,
                )
                results.append({"platform": plat, "rows_upserted": rows, "ok": True})
            except Exception as e:
                # seguimos con las demás
                results.append({"platform": plat, "ok": False, "error": str(e)})

        return {
            "status": "ok",
            "client_key": req.client_key,
            "project_id": req.project_id,
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
