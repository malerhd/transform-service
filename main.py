# main.py
from __future__ import annotations

import os
import json
import base64
import re
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from transformer import run_transform

# Para "skipped" necesitamos chequear si existe el snapshot en GCS
from google.cloud import storage
from google.oauth2 import service_account

app = FastAPI()


# ─────────────────────────────────────────────────────────
# GCP creds (mismo criterio que transformer.py: SERVICE_ACCOUNT_B64 o ADC)
# ─────────────────────────────────────────────────────────
def _get_gcp_credentials():
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()
    if not raw:
        return None  # ADC

    try:
        if raw.lstrip().startswith("{"):
            info = json.loads(raw)
        else:
            s = re.sub(r"\s+", "", raw)
            s += "=" * (-len(s) % 4)
            info = json.loads(base64.b64decode(s).decode("utf-8"))
        return service_account.Credentials.from_service_account_info(info)
    except Exception as e:
        raise RuntimeError(f"SERVICE_ACCOUNT_B64 inválido (JSON/Base64): {e}")


def _parse_gcs_uri(gcs_path: str) -> tuple[str, str]:
    u = urlparse(gcs_path)
    return u.netloc, u.path.lstrip("/")


def gcs_exists(gcs_path: str) -> bool:
    """
    Devuelve True si el objeto existe en GCS.
    Esto permite marcar "skipped" cuando un cliente no tiene esa plataforma.
    """
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    creds = _get_gcp_credentials()
    sc = storage.Client(credentials=creds) if creds else storage.Client()
    return sc.bucket(bucket).blob(blob_path).exists(sc)


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
        # (Opcional) también podés marcar skipped acá si querés.
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
# NUEVO: /transform_all (múltiples plataformas) + "skipped"
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
}

# Por defecto: si no querés transformar algunas
EXCLUDE_DEFAULT = set()  # ej: {"google-ads"}


def _normalize_platform(p: str) -> Optional[str]:
    if not p:
        return None
    p = str(p).strip()

    # match directo
    if p in SOURCE_MAP:
        return SOURCE_MAP[p]

    # match por lower-case contra keys lower-case
    low = p.lower().strip()
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

        # Normalizar + filtrar + dedupe con orden
        wanted: List[str] = []
        seen = set()

        for item in raw:
            # Si viene ya como slug (p.ej. "googleBusiness"), esto igual lo resuelve
            plat = _normalize_platform(item) or _normalize_platform(str(item).lower()) or str(item).strip()
            if not plat:
                continue

            # Excluir por el nombre humano o por el slug final
            if str(item).lower().strip() in exclude or plat.lower() in exclude:
                continue

            if plat not in seen:
                wanted.append(plat)
                seen.add(plat)

        results: List[Dict[str, Any]] = []
        done_count = 0
        skipped_count = 0
        error_count = 0

        for plat in wanted:
            gcs_path = f"gs://{req.bucket}/{req.client_key}/{plat}/snapshot-latest.json"

            # Si no existe el snapshot, lo marca como "skipped"
            try:
                if not gcs_exists(gcs_path):
                    skipped_count += 1
                    results.append({
                        "platform": plat,
                        "ok": True,
                        "status": "skipped",
                        "rows_upserted": 0,
                        "gcs_path": gcs_path,
                        "reason": "snapshot_not_found",
                    })
                    continue
            except Exception as e:
                # Si falla el exists (credenciales, permisos, etc.), devuelve error explícito
                error_count += 1
                results.append({
                    "platform": plat,
                    "ok": False,
                    "status": "error",
                    "error": f"gcs_exists_failed: {e}",
                    "gcs_path": gcs_path,
                })
                continue

            # Existe snapshot => transforma
            try:
                rows = run_transform(
                    gcs_path=gcs_path,
                    client_key=req.client_key,
                    platform=plat,  # slug/folder real
                    project_id=req.project_id,
                )
                done_count += 1
                results.append({
                    "platform": plat,
                    "ok": True,
                    "status": "done",
                    "rows_upserted": rows,
                    "gcs_path": gcs_path,
                })
            except Exception as e:
                error_count += 1
                results.append({
                    "platform": plat,
                    "ok": False,
                    "status": "error",
                    "error": str(e),
                    "gcs_path": gcs_path,
                })

        return {
            "status": "ok",
            "client_key": req.client_key,
            "project_id": req.project_id,
            "summary": {
                "requested": len(raw),
                "normalized_unique": len(wanted),
                "done": done_count,
                "skipped": skipped_count,
                "errors": error_count,
            },
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


