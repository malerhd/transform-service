# transformer.py
import logging
import json, os, base64, re
from urllib.parse import urlparse
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Credenciales GCP
# ─────────────────────────────────────────────────────────
def _get_gcp_credentials():
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()
    if not raw:
        return None  # ADC

    if raw.lstrip().startswith("{"):
        info = json.loads(raw)
    else:
        s = re.sub(r"\s+", "", raw)
        s += "=" * (-len(s) % 4)
        info = json.loads(base64.b64decode(s).decode("utf-8"))

    return service_account.Credentials.from_service_account_info(info)


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def _parse_gcs_uri(gcs_path: str):
    u = urlparse(gcs_path)
    return u.netloc, u.path.lstrip("/")


def _to_date_yyyy_mm_dd(s: str):
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()

    s = (
        s.replace("Z", "+00:00")
         .replace("+0000", "+00:00")
         .replace(".000+00:00", "+00:00")
         .replace(".000Z", "+00:00")
    )
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)

    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _month_floor(d):
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc).date()


def _ym_to_date_first(ym: str):
    if not ym or not ym.isdigit():
        return None
    return datetime(int(ym[:4]), int(ym[4:6]), 1, tzinfo=timezone.utc).date()


# ─────────────────────────────────────────────────────────
# IO
# ─────────────────────────────────────────────────────────
def read_json(gcs_path: str) -> dict:
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    creds = _get_gcp_credentials()
    client = storage.Client(credentials=creds) if creds else storage.Client()
    text = client.bucket(bucket).blob(blob_path).download_as_text()
    return json.loads(text)


# ─────────────────────────────────────────────────────────
# (PARSE + UPSERT FUNCTIONS)
# ⚠️ TODAS las funciones que ya tenías van acá IGUAL,
#    solo sin @task. No las repito por espacio.
# ─────────────────────────────────────────────────────────
# parse_meta_ads_monthly
# parse_ga_to_monthly
# parse_tn_sales_to_monthly
# parse_ig_metrics
# parse_bcra_to_metrics
# parse_merchant_to_metrics
# parse_tiktok_openapi
# ensure_bq_objects
# upsert_*


# ─────────────────────────────────────────────────────────
# ENTRYPOINT PRINCIPAL
# ─────────────────────────────────────────────────────────
def run_transform(
    gcs_path: str,
    client_key: str,
    platform: str,
    project_id: str,
) -> int:

    logger.info(f"[START] client={client_key} platform={platform}")
    ensure_bq_objects(project_id)
    payload = read_json(gcs_path)

    p = platform.lower().strip()

    if p == "meta-ads":
        return upsert_ads_monthly(
            parse_meta_ads_monthly(payload, client_key, p),
            project_id
        )

    if p == "ga":
        return upsert_ga_monthly(
            parse_ga_to_monthly(payload, client_key, p),
            project_id
        )

    if p == "tn":
        return upsert_tn_sales_monthly(
            parse_tn_sales_to_monthly(payload, client_key, p),
            project_id
        )

    if p == "ig":
        return upsert_social_metrics(
            parse_ig_metrics(payload, client_key, p),
            project_id
        )

    if p == "bcra":
        df = parse_bcra_to_metrics(payload, client_key, p)
        return upsert_bcra_metrics(df, project_id) if not df.empty else 0

    if p == "merchant":
        return upsert_merchant_metrics(
            parse_merchant_to_metrics(payload, client_key, p),
            project_id
        )

    if p == "tiktok":
        df_v, df_m = parse_tiktok_openapi(payload, client_key, p)
        return (
            upsert_tiktok_video_metrics(df_v, project_id) +
            upsert_tiktok_merchant_metrics(df_m, project_id)
        )

    raise ValueError(f"Plataforma no soportada: {platform}")
