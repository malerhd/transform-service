#main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Tuple

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
    """
    Mantiene compatibilidad con el flujo viejo:
    - vos le pasás el gcs_path explícito
    - y el platform que espera el transformer (meta-ads, ga, tn, ig, bcra, merchant, tiktok, etc.)
    """
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
            "gcs_path": req.gcs_path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────
# NUEVO: /transform_all (múltiples plataformas)
#
# IMPORTANTE:
# - Un "source" puede venir con nombres humanos (syncedSources)
# - En GCS el folder puede tener otro nombre (tiendanube, instagram, googleBusiness, ...)
# - En transformer.py la plataforma interna puede tener otro nombre (tn, ig, ...)
#
# Entonces: main.py traduce -> (folder_gcs, platform_transformer)
# ─────────────────────────────────────────────────────────

# 1) Entradas posibles (lo que puede venir en syncedSources o platforms)
# 2) Se traduce a:
#    - folder: carpeta real en GCS
#    - platform: string que entiende transformer.py
#
# OJO:
# - Para algunas plataformas folder == platform (ej: bcra, merchant, meta-ads, tiktok)
# - Para otras NO (ej: tiendanube folder -> platform tn; instagram folder -> platform ig)
SOURCE_MAP: Dict[str, Tuple[str, str]] = {
    # Finanzas
    "BCRA": ("bcra", "bcra"),
    "bcra": ("bcra", "bcra"),

    # Ads/Social
    "Meta Ads": ("meta-ads", "meta-ads"),
    "Meta": ("meta-ads", "meta-ads"),
    "meta-ads": ("meta-ads", "meta-ads"),

    "Instagram": ("instagram", "ig"),
    "IG": ("instagram", "ig"),
    "ig": ("instagram", "ig"),
    "instagram": ("instagram", "ig"),

    "TikTok": ("tiktok", "tiktok"),
    "Tiktok": ("tiktok", "tiktok"),
    "tiktok": ("tiktok", "tiktok"),

    # Merchant / Ecom
    "Google Merchant": ("merchant", "merchant"),
    "Merchant": ("merchant", "merchant"),
    "merchant": ("merchant", "merchant"),

    "Tienda Nube": ("tiendanube", "tn"),
    "TiendaNube": ("tiendanube", "tn"),
    "TN": ("tiendanube", "tn"),
    "tiendanube": ("tiendanube", "tn"),

    # Google (todavía no transformás Ads/GBP -> lo podés excluir desde n8n)
    "Google Ads": ("google-ads", "google-ads"),
    "google-ads": ("google-ads", "google-ads"),

    "Google Business": ("googleBusiness", "googleBusiness"),
    "Google Business Profile": ("googleBusiness", "googleBusiness"),
    "GBP": ("googleBusiness", "googleBusiness"),
    "googlebusiness": ("googleBusiness", "googleBusiness"),
    "googleBusiness": ("googleBusiness", "googleBusiness"),
}

# Por defecto no excluimos nada (vos querés excluir desde n8n)
EXCLUDE_DEFAULT = set()


def _normalize_source_to_folder_and_platform(src: str) -> Optional[Tuple[str, str]]:
    if not src:
        return None

    s = str(src).strip()
    if not s:
        return None

    # Match directo
    if s in SOURCE_MAP:
        return SOURCE_MAP[s]

    # Match case-insensitive
    low = s.lower().strip()
    for k, v in SOURCE_MAP.items():
        if k.lower() == low:
            return v

    return None


class TransformAllRequest(BaseModel):
    client_key: str
    project_id: str
    syncedSources: Optional[List[str]] = None
    platforms: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    bucket: str = "loopi-data-dev"


@app.post("/transform_all")
def transform_all(req: TransformAllRequest):
    """
    - Recibe una lista de sources (syncedSources o platforms)
    - Normaliza + dedupe preservando orden
    - Para cada una:
        - arma gcs_path con el folder REAL
        - llama run_transform con el platform interno del transformer
    - Devuelve summary con done/skipped/errors
    """
    try:
        raw = req.platforms or req.syncedSources or []
        if not raw:
            return {
                "status": "ok",
                "client_key": req.client_key,
                "project_id": req.project_id,
                "summary": {"requested": 0, "normalized_unique": 0, "done": 0, "skipped": 0, "errors": 0},
                "results": [],
                "note": "No platforms provided",
            }

        exclude = {x.lower().strip() for x in (req.exclude or [])}
        exclude |= {x.lower() for x in EXCLUDE_DEFAULT}

        # Normalizar + dedupe con orden (por folder+platform)
        wanted: List[Tuple[str, str, str]] = []  # (original, folder, platform)
        seen = set()

        for item in raw:
            norm = _normalize_source_to_folder_and_platform(item)
            if not norm:
                # Si viene algo no mapeado, lo ignoramos (no rompe)
                continue

            folder, platform = norm

            # Excluir por nombre original / folder / platform (case-insensitive)
            item_low = str(item).lower().strip()
            if item_low in exclude or folder.lower() in exclude or platform.lower() in exclude:
                continue

            key = f"{folder}::{platform}"
            if key in seen:
                continue
            seen.add(key)

            wanted.append((str(item), folder, platform))

        results: List[Dict[str, Any]] = []
        done = 0
        skipped = 0
        errors = 0

        for original, folder, platform in wanted:
            gcs_path = f"gs://{req.bucket}/{req.client_key}/{folder}/snapshot-latest.json"

            try:
                rows = run_transform(
                    gcs_path=gcs_path,
                    client_key=req.client_key,
                    platform=platform,  # <-- lo que entiende transformer.py (tn, ig, etc.)
                    project_id=req.project_id,
                )

                # Convención:
                # - si rows == 0 puede ser "no había nada" o "skip por snapshot inexistente"
                #   (tu transformer actualizado con gcs_exists devuelve 0 en skip)
                status = "skipped" if rows == 0 else "done"
                if status == "skipped":
                    skipped += 1
                else:
                    done += 1

                results.append({
                    "source": original,
                    "folder": folder,
                    "platform": platform,
                    "ok": True,
                    "status": status,
                    "rows_upserted": rows,
                    "gcs_path": gcs_path,
                })

            except Exception as e:
                errors += 1
                results.append({
                    "source": original,
                    "folder": folder,
                    "platform": platform,
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
                "done": done,
                "skipped": skipped,
                "errors": errors,
            },
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
