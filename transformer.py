# transformer.py
from __future__ import annotations

import logging
import json, os, base64, re
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import Tuple, List

import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Credenciales tolerantes (SERVICE_ACCOUNT_B64 o ADC)
# ─────────────────────────────────────────────────────────
def _get_gcp_credentials():
    """
    Si existe SERVICE_ACCOUNT_B64:
      - Acepta JSON crudo o Base64 (con padding corregido).
    Si no existe, devuelve None para usar ADC.
    """
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


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def _parse_gcs_uri(gcs_path: str) -> tuple[str, str]:
    """gs://bucket/path/file.json -> (bucket, 'path/file.json')"""
    u = urlparse(gcs_path)
    return u.netloc, u.path.lstrip("/")


def _to_date_yyyy_mm_dd(s: str):
    if not s:
        return None
    s = str(s).strip()
    # 1) GA: 'YYYYMMDD'
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()

    # 2) Normalizaciones comunes
    s_norm = (
        s.replace("Z", "+00:00")
         .replace("+0000", "+00:00")
         .replace(".000+00:00", "+00:00")
         .replace(".000Z", "+00:00")
    )
    if " " in s_norm and "T" not in s_norm:
        s_norm = s_norm.replace(" ", "T", 1)

    try:
        return datetime.fromisoformat(s_norm).date()
    except ValueError:
        from email.utils import parsedate_to_datetime
        try:
            return parsedate_to_datetime(s).date()
        except Exception:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _month_floor(d: datetime.date) -> datetime.date:
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc).date()


def _ym_to_date_first(ym: str):
    ym = str(ym or "")
    if len(ym) < 6 or not ym.isdigit():
        return None
    return datetime(int(ym[:4]), int(ym[4:6]), 1, tzinfo=timezone.utc).date()


# ─────────────────────────────────────────────────────────
# IO
# ─────────────────────────────────────────────────────────
def gcs_exists(gcs_path: str) -> bool:
    """
    Devuelve True si el objeto existe en GCS.
    Sirve para que clientes sin ciertas plataformas no rompan el flujo.
    """
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    creds = _get_gcp_credentials()
    sc = storage.Client(credentials=creds) if creds else storage.Client()
    return sc.bucket(bucket).blob(blob_path).exists(sc)


def read_json(gcs_path: str) -> dict:
    bucket, blob_path = _parse_gcs_uri(gcs_path)
    creds = _get_gcp_credentials()
    sc = storage.Client(credentials=creds) if creds else storage.Client()
    content = sc.bucket(bucket).blob(blob_path).download_as_text()
    return json.loads(content)


# ─────────────────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────────────────
def parse_meta_ads_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    rows = []
    for m in payload.get("metrics", []):
        cid = m.get("campaign_id")
        cname = m.get("campaign_name")
        for mm in m.get("months", []):
            month = _to_date_yyyy_mm_dd(mm.get("month"))
            agg = {
                "spend": 0.0,
                "impressions": 0,
                "clicks": 0,
                "inline_link_clicks": 0,
                "unique_inline_link_clicks": 0,
                "website_purchases": 0,
                "website_purchase_conversion_value": 0.0,
            }
            for d in mm.get("days", []):
                agg["spend"] += float(d.get("spend", 0) or 0)
                agg["impressions"] += int(d.get("impressions", 0) or 0)
                agg["clicks"] += int(d.get("clicks", 0) or 0)
                agg["inline_link_clicks"] += int(d.get("inline_link_clicks", 0) or 0)
                agg["unique_inline_link_clicks"] += int(d.get("unique_inline_link_clicks", 0) or 0)
                agg["website_purchases"] += int(d.get("website_purchases", 0) or 0)
                agg["website_purchase_conversion_value"] += float(d.get("website_purchase_conversion_value", 0) or 0)

            rows.append({
                "client_key": client_key,
                "platform": platform,
                "month": month,
                "campaign_id": str(cid) if cid is not None else None,
                "campaign_name": cname,
                **agg
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ctr"] = (df["clicks"] / df["impressions"]).fillna(0)
    df["roas"] = (df["website_purchase_conversion_value"] / df["spend"]).replace([pd.NA, pd.NaT], 0).fillna(0)
    return df


def parse_ga_to_monthly(payload: dict, client_key: str, platform: str) -> pd.DataFrame:
    daily = payload.get("series", {}).get("daily", [])
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame(daily)
    df["date"] = df["date"].astype(str).str[:8]
    df["date"] = df["date"].apply(_to_date_yyyy_mm_dd)
    df["month"] = df["date"].apply(_month_floor)

    numeric_cols = ["sessions", "transactions", "purchaseRevenue", "averagePurchaseRevenue", "purchaseConversionRate"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    g = df.groupby("month", as_index=False).agg(
        sessions=("sessions", "sum"),
        transactions=("transactions", "sum"),
        purchaseRevenue=("purchaseRevenue", "sum")
    )
    g["averagePurchaseRevenue"] = g.apply(
        lambda row: (row["purchaseRevenue"] / row["transactions"]) if row["transactions"] else 0.0, axis=1
    )
    g["purchaseConversionRate"] = g.apply(
        lambda row: (row["transactions"] / row["sessions"]) if row["sessions"] else 0.0, axis=1
    )

    g.insert(0, "platform", platform)
    g.insert(0, "client_key", client_key)
    return g


def parse_tn_sales_to_monthly(payload: dict, client_key: str, platform: str, only_paid: bool = False) -> pd.DataFrame:
    ventas = payload.get("ventas", [])
    if not ventas:
        vd = payload.get("ventas_diarias", {})
        if isinstance(vd, dict):
            ventas = [o for _, arr in vd.items() for o in (arr or [])]

    if not ventas:
        return pd.DataFrame()

    rows = []
    for v in ventas:
        created = v.get("created_at")
        order_date = _to_date_yyyy_mm_dd(created)
        if not order_date:
            continue
        if only_paid and str(v.get("payment_status", "")).lower() != "paid":
            continue

        month = _month_floor(order_date)
        currency = (v.get("currency") or "NA")
        total = float(v.get("total", 0) or 0)
        subtotal = float(v.get("subtotal", 0) or 0)
        discount = float(v.get("discount", 0) or 0) + float(v.get("discount_gateway", 0) or 0)
        paid = 1 if str(v.get("payment_status", "")).lower() == "paid" else 0

        rows.append({
            "client_key": client_key,
            "platform": platform,
            "month": month,
            "orders": 1,
            "gross_revenue": total,
            "subtotal": subtotal,
            "discount": discount,
            "currency": currency,
            "paid": paid,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    g = df.groupby(["client_key", "platform", "month", "currency"], as_index=False).agg(
        orders=("orders", "sum"),
        gross_revenue=("gross_revenue", "sum"),
        subtotal=("subtotal", "sum"),
        discount=("discount", "sum"),
        paid=("paid", "sum"),
    )
    g["avg_order_value"] = g.apply(
        lambda r: (r["gross_revenue"] / r["orders"]) if r["orders"] else 0.0,
        axis=1
    )
    return g


# ... (el resto de tus funciones queda IGUAL: parse_ig_metrics, parse_bcra_to_metrics, parse_merchant_to_metrics,
# parse_tiktok_openapi, ensure_bq_objects, upserts, etc.)


# ─────────────────────────────────────────────────────────
# ENTRYPOINT PRINCIPAL
# ─────────────────────────────────────────────────────────
def run_transform(gcs_path: str, client_key: str, platform: str, project_id: str) -> int:
    logger.info(f"[START] client={client_key} platform={platform} gcs_path={gcs_path}")

    # ✅ NUEVO: si no existe el snapshot, no rompemos el flujo
    if not gcs_exists(gcs_path):
        logger.warning(f"[SKIP] snapshot not found: {gcs_path}")
        return 0

    ensure_bq_objects(project_id)
    payload = read_json(gcs_path)

    p = platform.lower().strip()

    if p == "meta-ads":
        df = parse_meta_ads_monthly(payload, client_key, p)
        n = upsert_ads_monthly(df, project_id)
        logger.info(f"[META-ADS] upsert rows: {n}")
        return n

    if p == "ga":
        df = parse_ga_to_monthly(payload, client_key, p)
        n = upsert_ga_monthly(df, project_id)
        logger.info(f"[GA] upsert rows: {n}")
        return n

    if p == "tn":
        df = parse_tn_sales_to_monthly(payload, client_key, p)
        n = upsert_tn_sales_monthly(df, project_id)
        logger.info(f"[TN] upsert rows: {n}")
        return n

    if p == "ig":
        df = parse_ig_metrics(payload, client_key, p)
        n = upsert_social_metrics(df, project_id)
        logger.info(f"[IG] upsert rows: {n}")
        return n

    if p == "bcra":
        df = parse_bcra_to_metrics(payload, client_key, p)
        if df.empty:
            logger.warning("[BCRA] snapshot sin 'historicas' o sin periodos. No se upserta.")
            return 0
        n = upsert_bcra_metrics(df, project_id)
        logger.info(f"[BCRA] upsert rows: {n} latest={df.iloc[0]['as_of_month']}")
        return n

    if p == "merchant":
        df = parse_merchant_to_metrics(payload, client_key, p)
        n = upsert_merchant_metrics(df, project_id)
        logger.info(f"[MERCHANT] upsert rows: {n} as_of={df.iloc[0]['as_of_date']} n_products={int(df.iloc[0]['n_products'])}")
        return n

    if p == "tiktok":
        df_v, df_m = parse_tiktok_openapi(payload, client_key, p)
        n1 = upsert_tiktok_video_metrics(df_v, project_id)
        n2 = upsert_tiktok_merchant_metrics(df_m, project_id)
        logger.info(f"[TIKTOK] upsert videos: {n1} upsert merchant: {n2} observable={bool(df_m.iloc[0]['is_observable_30d'])}")
        return int(n1 + n2)

    raise ValueError(f"Plataforma no soportada: {platform}")
