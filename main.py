from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

from transformer import run_transform

app = FastAPI()

# ─────────────────────────────────────────────────────────
# Endpoint actual: 1 plataforma (se mantiene)
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
# NUEVO: /transform_all (múltiples plataformas)
# Ajustado a nombres reales en GCS
# ─────────────────────────────────────────────────────────

# Mapea lo que puede venir en syncedSources (humano) -> folder/slug en GCS
SOURCE_MAP = {
    # Finanzas
    "BCRA": "bcra",
    "bcra": "bcra",

    # Social / Ads
    "Meta Ads": "meta-ads",
    "Meta": "meta-ads",
    "meta-ads": "meta-ads",

    "Instagram": "instagram",
    "IG": "instagram",
    "ig": "instagram",
    "instagram": "instagram",

    "TikTok": "tiktok",
    "Tiktok": "tiktok",
    "tiktok": "tiktok",

    # Ecom / Merchant
    "Google Merchant": "merchant",
    "Merchant": "merchant",
    "merchant": "merchant",

    "Tienda Nube": "tiendanube",
    "TiendaNube": "tiendanube",
    "tiendanube": "tiendanube",

    # Google
    "Google Ads": "google-ads",
    "google-ads": "google-ads",

    "Google Business": "googleBusiness",
    "Google Business Profile": "googleBusiness",
    "GBP": "googleBusiness",
    "googlebusiness": "googleBusiness",
    "googleBusiness": "googleBusiness",

    # (Si en el futuro vuelve GA4 como folder "ga" u otro, se agrega acá)
    # "GA4": "ga",
    # "Google Analytics": "ga",
}

# Por defecto: si no querés transformar algunas
EXCLUDE_DEFAULT = set()  # ej: {"google-ads"} si quisieras apagarla


def _normalize_platform(p: str) -> Optional[str]:
    if not p:
        return None
    p = str(p).strip()

    # match directo (case-sensitive) y luego flexible
    if p in SOURCE_MAP:
        return SOURCE_MAP[p]

    low = p.lower().strip()
    # intentamos matchear por lower-case contra keys lower-case
    for k, v in SOURCE_MAP.items():
        if k.lower() == low:
            return v

    return None


class TransformAllRequest(BaseModel):
    client_key: str
    project_id: str
    # Podés mandar cualquiera de las dos:
    syncedSources: Optional[List[str]] = None
    platforms: Optional[List[str]] = None  # si ya vienen normalizadas / slugs
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
                "project_id": req.project_id,
                "results": [],
                "note": "No platforms provided",
            }

        exclude = {x.lower().strip() for x in (req.exclude or [])}
        exclude |= {x.lower() for x in EXCLUDE_DEFAULT}

        # normalizar + filtrar + dedupe con orden
        wanted: List[str] = []
        seen = set()

        for item in raw:
            plat = _normalize_platform(item) or _normalize_platform(str(item).lower())
            if not plat:
                continue

            # excluir por el nombre humano o por el slug final
            if str(item).lower().strip() in exclude or plat.lower() in exclude:
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
                    platform=plat,  # OJO: acá llega el slug/folder real
                    project_id=req.project_id,
                )
                results.append({"platform": plat, "rows_upserted": rows, "ok": True})
            except Exception as e:
                results.append({"platform": plat, "ok": False, "error": str(e)})

        return {
            "status": "ok",
            "client_key": req.client_key,
            "project_id": req.project_id,
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

