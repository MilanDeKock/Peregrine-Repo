import io
import os
import re
import tempfile
import time
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
import pdfplumber
import camelot
import requests
import streamlit as st
from dotenv import load_dotenv
from openpyxl.utils import get_column_letter
from pytz import timezone

# -------------------------
# Configuration
# -------------------------
load_dotenv()
TZ = timezone("Africa/Johannesburg")

API_BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
CALLS_PER_MINUTE = 50
CALL_INTERVAL = 60.0 / CALLS_PER_MINUTE

st.set_page_config(
    page_title="Peregrine Stocktake Check Tool",
    page_icon="📦",
    layout="wide",
)

st.title("📦 Peregrine Stocktake Check Tool")
st.caption("Upload a Stocktake PDF → Inventory pulled from Cin7 Core → Variance Analysis XLSX download")


# -------------------------
# Cin7 Core API Client
# -------------------------
class Cin7Client:
    """Cin7 Core API client with built-in rate limiting."""

    def __init__(self, account_id: str, api_key: str):
        self.headers = {
            "Content-Type": "application/json",
            "api-auth-accountid": account_id,
            "api-auth-applicationkey": api_key,
        }
        self._last_call_time = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_call_time
        if elapsed < CALL_INTERVAL:
            time.sleep(CALL_INTERVAL - elapsed)
        self._last_call_time = time.time()

    def get(self, endpoint, params=None):
        self._throttle()
        url = f"{API_BASE_URL}/{endpoint}"
        resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        if resp.status_code == 503:
            # Rate limited — wait and retry once
            time.sleep(60)
            self._last_call_time = time.time()
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()


def get_api_credentials():
    """Load API credentials from st.secrets (Streamlit Cloud) or .env (local)."""
    account_id = None
    api_key = None
    try:
        account_id = st.secrets["CIN7_ACCOUNT_ID"]
        api_key = st.secrets["CIN7_API_KEY"]
    except (KeyError, FileNotFoundError, AttributeError):
        pass
    if not account_id or not api_key:
        account_id = os.getenv("CIN7_ACCOUNT_ID")
        api_key = os.getenv("CIN7_API_KEY")
    return account_id, api_key


# -------------------------
# API Data Fetchers
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_inventory_from_api(account_id, api_key):
    """Fetch all products from Cin7 Core API with pagination."""
    client = Cin7Client(account_id, api_key)
    all_products = []
    page = 1
    limit = 1000

    while True:
        data = client.get("product", params={"Page": page, "Limit": limit})
        products = data.get("Products", [])
        all_products.extend(products)
        total = data.get("Total", 0)
        if page * limit >= total:
            break
        page += 1

    rows = []
    for p in all_products:
        rows.append({
            "ProductCode": str(p.get("SKU", "")).strip(),
            "AverageCost": p.get("AverageCost", 0) or 0,
            "Category": p.get("Category", "") or "",
            "DefaultUnitOfMeasure": p.get("UOM", "") or "",
            "CommaDelimitedTags": p.get("Tags", "") or "",
        })
    return pd.DataFrame(rows)


def fetch_availability_by_sku(client, sku):
    """GET /ref/productavailability — SOH per location for a SKU."""
    try:
        data = client.get("ref/productavailability", params={"Sku": sku})
    except requests.HTTPError:
        return []
    if data and data.get("ProductAvailabilityList"):
        return [
            row for row in data["ProductAvailabilityList"]
            if row.get("SKU", "").strip().upper() == sku.strip().upper()
        ]
    return []


def fetch_bom_reverse_index(client):
    """
    Fetch ALL products with BOM data and build a reverse index:
    component SKU → list of parent products that use it.
    """
    bom_index = defaultdict(list)
    page = 1
    limit = 1000

    while True:
        try:
            data = client.get("product", params={
                "Page": page, "Limit": limit, "IncludeBOM": "true",
            })
        except requests.HTTPError:
            break
        products = data.get("Products", [])
        for p in products:
            if p.get("BillOfMaterial") and p.get("BillOfMaterialsProducts"):
                parent_sku = p.get("SKU", "")
                parent_name = p.get("Name", "")
                for comp in p.get("BillOfMaterialsProducts", []):
                    comp_sku = comp.get("ProductCode", "").strip().upper()
                    if comp_sku:
                        bom_index[comp_sku].append({
                            "ParentSKU": parent_sku,
                            "ParentName": parent_name,
                            "Quantity": comp.get("Quantity", 0),
                            "QuantityToProduce": p.get("QuantityToProduce", 1),
                            "WastagePercent": comp.get("WastagePercent", 0),
                        })
        total = data.get("Total", 0)
        if page * limit >= total:
            break
        page += 1

    return bom_index


def fetch_recent_stock_takes(client, limit=5):
    """
    GET /stockTakeList — fetch all completed stock takes with pagination,
    sort by EffectiveDate descending, and return the most recent `limit`.
    """
    all_takes = []
    page = 1
    page_size = 100

    while True:
        try:
            data = client.get("stockTakeList", params={
                "Status": "COMPLETED", "Limit": str(page_size), "Page": str(page),
            })
        except requests.HTTPError:
            break
        takes = data.get("StockAdjustmentList", [])
        all_takes.extend(takes)
        total = data.get("Total", 0)
        if page * page_size >= total:
            break
        page += 1

    # Parse dates properly for reliable sorting (most recent first)
    def parse_date(st_entry):
        raw = st_entry.get("EffectiveDate", "") or ""
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return datetime.min

    all_takes.sort(key=parse_date, reverse=True)
    return all_takes[:limit]


def fetch_stock_take_detail(client, task_id):
    """GET /stocktake?TaskID=... — full detail of a stock take."""
    try:
        return client.get("stocktake", params={"TaskID": task_id})
    except requests.HTTPError:
        return None


def fetch_latest_po_receiving(client, skus, progress_cb=None,
                              max_po_details=50, months_back=6):
    """
    Fetch recent received POs (last N months) and find the latest receiving per SKU.
    Only fetches POs updated in the lookback window to avoid scanning all history.
    Returns (receiving_data, debug_info).
    """
    from datetime import timedelta

    sku_set = {s.upper() for s in skus}
    receiving_data = {}
    debug = {
        "total_po_list": 0, "received_po_count": 0, "details_checked": 0,
        "total_lines_found": 0, "skus_matched": 0, "statuses_seen": {},
        "sample_po": None, "sample_detail_keys": None,
        "sample_stock_received_type": None, "sample_line_keys": None,
        "errors": [],
    }

    # Only look at POs updated in the last N months
    since = (datetime.now() - timedelta(days=30 * months_back)).strftime("%Y-%m-%dT00:00:00")

    # --- Phase 1: Fetch recent purchase list entries ---
    all_purchases = []
    page = 1
    while True:
        try:
            data = client.get("purchaseList", params={
                "Page": page, "Limit": 100, "UpdatedSince": since,
            })
        except requests.HTTPError as e:
            debug["errors"].append(f"purchaseList page {page}: {e}")
            break
        purchases = data.get("PurchaseList", [])
        all_purchases.extend(purchases)
        total = data.get("Total", 0)
        if page * 100 >= total:
            break
        page += 1

    debug["total_po_list"] = len(all_purchases)

    for po in all_purchases:
        status = po.get("CombinedReceivingStatus") or "(empty)"
        debug["statuses_seen"][status] = debug["statuses_seen"].get(status, 0) + 1

    if all_purchases:
        debug["sample_po"] = {
            k: str(v)[:80] for k, v in list(all_purchases[0].items())[:12]
        }

    # --- Phase 2: Filter for received POs, sort newest first ---
    received_pos = [
        po for po in all_purchases
        if (po.get("CombinedReceivingStatus") or "").upper()
        not in ("NOT RECEIVED", "NOT AVAILABLE", "VOIDED", "")
    ]
    debug["received_po_count"] = len(received_pos)

    def parse_po_date(po):
        raw = po.get("OrderDate", "") or ""
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return datetime.min

    received_pos.sort(key=parse_po_date, reverse=True)

    # --- Phase 3: Fetch stock detail for newest POs first ---
    details_checked = 0
    for po in received_pos:
        if details_checked >= max_po_details:
            break
        if len(receiving_data) >= len(sku_set):
            break

        po_id = po.get("ID")
        po_number = po.get("OrderNumber", "")

        lines = []

        # Try advanced purchase detail
        try:
            detail = client.get("advanced-purchase", params={"ID": po_id})
            if debug["sample_detail_keys"] is None:
                debug["sample_detail_keys"] = list(detail.keys())
            stock_received = detail.get("StockReceived", [])
            if debug["sample_stock_received_type"] is None:
                debug["sample_stock_received_type"] = (
                    f"{type(stock_received).__name__}, "
                    f"len={len(stock_received) if isinstance(stock_received, list) else 'N/A'}"
                )
            if isinstance(stock_received, list):
                for sr in stock_received:
                    lines.extend(sr.get("Lines", []))
            elif isinstance(stock_received, dict):
                lines.extend(stock_received.get("Lines", []))
        except requests.HTTPError as e:
            debug["errors"].append(f"advanced-purchase {po_number}: {e}")

        # Fallback: simple purchase detail
        if not lines:
            try:
                detail = client.get("purchase", params={"ID": po_id})
                if debug["sample_detail_keys"] is None:
                    debug["sample_detail_keys"] = list(detail.keys())
                stock_received = detail.get("StockReceived", {})
                if debug["sample_stock_received_type"] is None:
                    debug["sample_stock_received_type"] = type(stock_received).__name__
                if isinstance(stock_received, dict):
                    lines.extend(stock_received.get("Lines", []))
                elif isinstance(stock_received, list):
                    for sr in stock_received:
                        lines.extend(sr.get("Lines", []))
            except requests.HTTPError as e:
                debug["errors"].append(f"purchase {po_number}: {e}")

        if lines and debug["sample_line_keys"] is None:
            debug["sample_line_keys"] = list(lines[0].keys())

        debug["total_lines_found"] += len(lines)

        for line in lines:
            sku_key = (line.get("SKU") or "").strip().upper()
            if sku_key in sku_set and sku_key not in receiving_data:
                receiving_data[sku_key] = {
                    "Last_PO_Number": po_number,
                    "Last_PO_Date": (line.get("Date") or "")[:10],
                    "Last_PO_Qty": line.get("Quantity", 0) or 0,
                    "Last_PO_Location": line.get("Location") or "",
                }

        details_checked += 1
        if progress_cb:
            progress_cb(details_checked)

    debug["details_checked"] = details_checked
    debug["skus_matched"] = len(receiving_data)

    return receiving_data, debug


# -------------------------
# Investigation Enrichment
# -------------------------
def run_investigation(client, skus, progress_bar, status_text,
                      sku_descriptions=None, sku_costs=None):
    """
    Enrich SKUs with SOH per location, last stock take count, latest PO receiving,
    and BOM reverse lookup.
    Returns (df_investigation, df_bom):
      - df_investigation: one row per SKU with SOH, stock take, and PO data
      - df_bom: exploded — one row per SKU per parent BOM it belongs to
    """
    if sku_descriptions is None:
        sku_descriptions = {}
    if sku_costs is None:
        sku_costs = {}
    # SOH per SKU + BOM bulk (~3) + stock takes (~6) + PO receiving (~30)
    total_steps = len(skus) + 40
    step = 0

    # --- Step 1: SOH per location ---
    status_text.text("Fetching stock on hand per location...")
    soh_data = {}
    for sku in skus:
        avail = fetch_availability_by_sku(client, sku)
        if avail:
            loc_parts = []
            total_on_hand = 0
            total_available = 0
            for row in avail:
                loc = row.get("Location", "?")
                oh = row.get("OnHand", 0) or 0
                av = row.get("Available", 0) or 0
                loc_parts.append(f"{loc}: {oh}")
                total_on_hand += oh
                total_available += av
            soh_data[sku.upper()] = {
                "SOH_Total_OnHand": total_on_hand,
                "SOH_Total_Available": total_available,
                "SOH_Per_Location": " | ".join(loc_parts),
            }
        else:
            soh_data[sku.upper()] = {
                "SOH_Total_OnHand": None,
                "SOH_Total_Available": None,
                "SOH_Per_Location": "NOT FOUND",
            }
        step += 1
        progress_bar.progress(step / total_steps, text=f"SOH: {step}/{len(skus)} SKUs")

    # --- Step 2: BOM reverse index (bulk fetch all products) ---
    status_text.text("Building BOM reverse index (fetching all products)...")
    bom_index = fetch_bom_reverse_index(client)
    step += 3
    progress_bar.progress(min(step / total_steps, 0.95), text="BOM index built")

    # --- Step 3: Last stock take counts ---
    status_text.text("Fetching last stock take counts...")
    last_count = {}
    stock_takes = fetch_recent_stock_takes(client, limit=5)
    step += 1
    progress_bar.progress(step / total_steps)

    for st_entry in stock_takes:
        task_id = st_entry.get("TaskID")
        st_number = st_entry.get("StocktakeNumber", "?")
        st_date = st_entry.get("EffectiveDate", "")[:10]
        st_location = st_entry.get("Location", "")

        detail = fetch_stock_take_detail(client, task_id)
        step += 1
        progress_bar.progress(min(step / total_steps, 0.95))

        if not detail:
            continue

        sku_agg = defaultdict(lambda: {"system_soh": 0, "counted": 0})
        for line in detail.get("NonZeroStockOnHandProducts", []):
            sku_key = line.get("SKU", "").strip().upper()
            if sku_key:
                sku_agg[sku_key]["system_soh"] += (line.get("QuantityOnHand", 0) or 0)
                sku_agg[sku_key]["counted"] += (line.get("Adjustment", 0) or 0)

        for line in detail.get("ZeroStockOnHandProducts", []):
            sku_key = line.get("SKU", "").strip().upper()
            if sku_key:
                sku_agg[sku_key]["counted"] += (line.get("Quantity", 0) or 0)

        for sku_key, agg in sku_agg.items():
            if sku_key not in last_count:
                system_soh = agg["system_soh"]
                counted_qty = agg["counted"]
                variance = counted_qty - system_soh
                last_count[sku_key] = {
                    "Last_System_SOH": system_soh,
                    "Last_Counted_Qty": counted_qty,
                    "Last_ST_Variance": variance,
                    "Last_StockTake_Ref": st_number,
                    "Last_StockTake_Date": st_date,
                    "Last_StockTake_Location": st_location,
                }

    # --- Step 4: Latest PO receiving ---
    status_text.text("Fetching latest PO receiving data...")
    def po_progress(n):
        nonlocal step
        step += 1
        progress_bar.progress(min(step / total_steps, 0.99),
                              text=f"PO receiving: checked {n} POs")

    po_data, po_debug = fetch_latest_po_receiving(client, skus, progress_cb=po_progress)

    progress_bar.progress(1.0)
    status_text.text("Building investigation tables...")

    # --- Build investigation DataFrame (SOH + stock take + PO) ---
    inv_rows = []
    for sku in skus:
        key = sku.upper()
        soh = soh_data.get(key, {})
        lc = last_count.get(key, {})
        po = po_data.get(key, {})
        avg_cost = sku_costs.get(sku, 0) or 0

        counted_qty = lc.get("Last_Counted_Qty")
        st_variance = lc.get("Last_ST_Variance")
        last_variance_amount = (st_variance * avg_cost) if st_variance is not None else None

        inv_rows.append({
            "Code": sku,
            "Product Description": sku_descriptions.get(sku, ""),
            "AverageCost": avg_cost,
            "SOH_Total_OnHand": soh.get("SOH_Total_OnHand"),
            "SOH_Total_Available": soh.get("SOH_Total_Available"),
            "SOH_Per_Location": soh.get("SOH_Per_Location", ""),
            "Last_StockTake_Ref": lc.get("Last_StockTake_Ref", ""),
            "Last_StockTake_Date": lc.get("Last_StockTake_Date", ""),
            "Last_StockTake_Location": lc.get("Last_StockTake_Location", ""),
            "Last_System_SOH": lc.get("Last_System_SOH"),
            "Last_Counted_Qty": counted_qty,
            "Last_ST_Variance": st_variance,
            "Last_ST_Variance_Amount": last_variance_amount,
            "Last_PO_Number": po.get("Last_PO_Number", ""),
            "Last_PO_Date": po.get("Last_PO_Date", ""),
            "Last_PO_Qty": po.get("Last_PO_Qty"),
            "Last_PO_Location": po.get("Last_PO_Location", ""),
        })

    df_investigation = pd.DataFrame(inv_rows)

    # --- Build BOM DataFrame (exploded: one row per SKU per parent) ---
    bom_rows = []
    for sku in skus:
        key = sku.upper()
        bom_entries = bom_index.get(key, [])
        for entry in bom_entries:
            bom_rows.append({
                "Code": sku,
                "Product Description": sku_descriptions.get(sku, ""),
                "Parent_SKU": entry["ParentSKU"],
                "Parent_Name": entry["ParentName"],
                "Qty_In_BOM": entry["Quantity"],
                "Qty_To_Produce": entry["QuantityToProduce"],
            })

    df_bom = pd.DataFrame(bom_rows) if bom_rows else pd.DataFrame(
        columns=["Code", "Product Description", "Parent_SKU", "Parent_Name",
                 "Qty_In_BOM", "Qty_To_Produce"]
    )

    return df_investigation, df_bom, po_debug


# -------------------------
# PDF Helpers
# -------------------------
def detect_vertical_splits_from_lines(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        p = pdf.pages[0]
        W, H = p.width, p.height

        text = p.extract_words(use_text_flow=True) or []
        if text:
            y_tops = [w["top"] for w in text][:40]
            y_header_top = min(y_tops) if y_tops else H * 0.10
        else:
            y_header_top = H * 0.10

        band_top = max(0, y_header_top - 30)
        band_bottom = min(H, band_top + 120)

        verts = []
        for ln in p.lines or []:
            x0, x1, y0, y1 = ln["x0"], ln["x1"], ln["y0"], ln["y1"]
            if abs(x1 - x0) < 0.5:
                y_top_l = min(y0, y1)
                y_bot_l = max(y0, y1)
                if not (y_bot_l < band_top or y_top_l > band_bottom):
                    verts.append(round((x0 + x1) / 2.0, 2))

        verts = sorted(verts)
        dedup = []
        for x in verts:
            if not dedup or abs(x - dedup[-1]) > 2.5:
                dedup.append(x)

        if len(dedup) >= 6:
            return dedup, (0, max(0, band_top - 10), W, H - 6)
        return None, (0, 0, W, H - 6)


def to_num(s):
    return (
        s.astype(str)
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(" ", "", regex=True)
        .str.replace(",", "", regex=True)
        .str.replace(r"[^0-9.\-]", "", regex=True)
        .replace({"": np.nan, ".": np.nan, "-.": np.nan})
        .astype(float)
    )


def extract_and_refine_pdf(pdf_path):
    splits, area_bbox = detect_vertical_splits_from_lines(pdf_path)

    if not splits:
        MANUAL_PERC = [0.03, 0.12, 0.70, 0.78, 0.86, 0.93]
        with pdfplumber.open(pdf_path) as pdf:
            W, H = pdf.pages[0].width, pdf.pages[0].height
        splits = [round(W * p, 2) for p in MANUAL_PERC]
        area_bbox = (0, 0, W, H - 6)

    left, top, right, bottom = area_bbox
    table_area = f"{left:.2f},{top:.2f},{right:.2f},{bottom:.2f}"
    columns_csv = ",".join(f"{x:.2f}" for x in splits)

    tables = camelot.read_pdf(
        pdf_path,
        pages="1-end",
        flavor="stream",
        table_areas=[table_area],
        columns=[columns_csv],
        strip_text=" \n",
        edge_tol=500,
        row_tol=10,
        column_tol=12,
    )

    dfs = [pd.DataFrame(t.df).astype(str) for t in tables]
    max_cols = max((d.shape[1] for d in dfs), default=0)
    aligned = []
    for d in dfs:
        if d.shape[1] < max_cols:
            for i in range(d.shape[1] + 1, max_cols + 1):
                d[f"__pad{i}__"] = ""
        d = d.iloc[:, :max_cols]
        d.columns = [f"Col{i}" for i in range(1, max_cols + 1)]
        aligned.append(d)

    raw = pd.concat(aligned, ignore_index=True) if aligned else pd.DataFrame()

    df_refined = raw.copy()
    df_refined = df_refined.iloc[1:].reset_index(drop=True)
    df_refined = df_refined.drop(df_refined.columns[0], axis=1)

    TARGET = [
        "#", "Code", "Product Description", "Unit",
        "Old Quantity", "Stocktake Quantity", "Variance",
    ]
    if df_refined.shape[1] < 7:
        for i in range(df_refined.shape[1], 7):
            df_refined[f"__pad{i}__"] = np.nan
    df_refined = df_refined.iloc[:, :7].copy()
    df_refined.columns = TARGET

    df_refined = df_refined.map(lambda x: x.strip() if isinstance(x, str) else x)
    df_refined = df_refined.replace({"": np.nan, "nan": np.nan})

    def is_numeric_str(s):
        if not isinstance(s, str):
            return False
        return bool(re.fullmatch(r"\d+", s.strip()))

    mask_hash_ok = df_refined["#"].astype(str).apply(is_numeric_str)
    df_refined = df_refined[mask_hash_ok].copy()
    df_refined = df_refined.dropna(how="all").reset_index(drop=True)

    size_like = re.compile(
        r"^(?:[A-Za-z]*\s*\d+(?:\.\d+)?\s*(?:ml|g|kg|l)|\d+(?:\.\d+)?\s*(?:ml|g|kg|l))$",
        re.IGNORECASE,
    )
    cont_mask = df_refined["Code"].isna() & (
        df_refined["Product Description"]
        .fillna("")
        .str.replace(" ", "", regex=False)
        .str.match(size_like)
    )
    df_refined = df_refined[~cont_mask].reset_index(drop=True)

    for c in ["Old Quantity", "Stocktake Quantity", "Variance"]:
        df_refined[c] = to_num(df_refined[c])

    df_refined["Code"] = df_refined["Code"].astype("string")
    return df_refined


# -------------------------
# Grouping & Merging
# -------------------------
def group_and_sort(df_refined):
    df_grouped = (
        df_refined.groupby(["Code", "Product Description"])
        .agg({
            "Old Quantity": "sum",
            "Stocktake Quantity": "sum",
            "Variance": "sum",
        })
        .reset_index()
    )
    return df_grouped.sort_values(by="Variance", ascending=False).reset_index(drop=True)


def merge_with_inventory(df_grouped_sorted, df_inventory):
    df_inv = df_inventory.copy()
    df_inv["ProductCode"] = df_inv["ProductCode"].astype(str).str.strip()

    if "AverageCost" in df_inv.columns:
        df_inv["AverageCost"] = pd.to_numeric(df_inv["AverageCost"], errors="coerce").fillna(0)
    else:
        df_inv["AverageCost"] = 0

    if "Category" in df_inv.columns:
        df_inv["Category"] = df_inv["Category"].astype(str).fillna("")
    else:
        df_inv["Category"] = ""

    if "DefaultUnitOfMeasure" in df_inv.columns:
        df_inv["Unit"] = df_inv["DefaultUnitOfMeasure"].astype(str).fillna("")
    else:
        df_inv["Unit"] = ""

    if "CommaDelimitedTags" in df_inv.columns:
        df_inv["Tags"] = df_inv["CommaDelimitedTags"].astype(str).fillna("")
    else:
        df_inv["Tags"] = ""

    merge_cols = ["ProductCode", "AverageCost", "Category", "Unit", "Tags"]
    df_merged = pd.merge(
        df_grouped_sorted,
        df_inv[merge_cols],
        left_on="Code",
        right_on="ProductCode",
        how="left",
    )

    df_merged["AverageCost"] = df_merged["AverageCost"].fillna(0)
    df_merged["Category"] = df_merged["Category"].fillna("")
    df_merged["Unit"] = df_merged["Unit"].fillna("")
    df_merged["Tags"] = df_merged["Tags"].fillna("")

    df_merged["Amount of Variance"] = df_merged["AverageCost"] * df_merged["Variance"]
    df_merged["Absolute Amount Variance"] = df_merged["Amount of Variance"].abs()

    df_merged = df_merged.sort_values(by="Absolute Amount Variance", ascending=False).reset_index(drop=True)

    total_abs_variance = df_merged["Absolute Amount Variance"].sum()
    if total_abs_variance > 0:
        df_merged["Cumulative Absolute Variance"] = df_merged["Absolute Amount Variance"].cumsum()
        df_merged["Cumulative Percentage"] = (
            df_merged["Cumulative Absolute Variance"] / total_abs_variance
        ) * 100
        prev_cum = df_merged["Cumulative Percentage"].shift(1, fill_value=0)
        df_merged["Pareto Category"] = prev_cum.apply(
            lambda x: "Top 80% Variance" if x < 80 else "Other"
        )
    else:
        df_merged["Cumulative Absolute Variance"] = 0
        df_merged["Cumulative Percentage"] = 0
        df_merged["Pareto Category"] = "No Variance"

    return df_merged


# -------------------------
# Excel Export
# -------------------------
def autofit_sheet(ws):
    """Auto-fit all column widths in a worksheet."""
    for column in ws.columns:
        col_cells = list(column)
        max_length = max(
            (len(str(cell.value)) for cell in col_cells if cell.value is not None),
            default=0,
        )
        ws.column_dimensions[
            get_column_letter(col_cells[0].column)
        ].width = min(max_length + 2, 60)


def build_excel(df_refined, df_merged, df_investigation=None, df_bom=None):
    """Build the Excel workbook in memory and return bytes."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # --- Sheet 1: Grouped & Sorted Data ---
        df_export = df_merged.drop(columns=["ProductCode"], errors="ignore")
        col_order = [
            "Code", "Product Description", "Category", "Unit", "Tags",
            "Old Quantity", "Stocktake Quantity", "Variance",
            "AverageCost", "Amount of Variance", "Absolute Amount Variance",
            "Pareto Category",
        ]
        col_order = [c for c in col_order if c in df_export.columns]
        df_export = df_export[col_order]

        df_export.to_excel(writer, sheet_name="Grouped & Sorted Data", index=False)
        ws_grouped = writer.sheets["Grouped & Sorted Data"]
        ws_grouped.freeze_panes = "D2"

        if "Code" in df_export.columns:
            idx = df_export.columns.get_loc("Code") + 1
            for cell in ws_grouped.iter_rows(min_row=2, min_col=idx, max_col=idx):
                cell[0].number_format = "@"
        if "Amount of Variance" in df_export.columns:
            idx = df_export.columns.get_loc("Amount of Variance") + 1
            for cell in ws_grouped.iter_rows(min_row=2, min_col=idx, max_col=idx):
                cell[0].number_format = "R#,##0.00"
        if "Absolute Amount Variance" in df_export.columns:
            idx = df_export.columns.get_loc("Absolute Amount Variance") + 1
            for cell in ws_grouped.iter_rows(min_row=2, min_col=idx, max_col=idx):
                cell[0].number_format = "R#,##0.00"
        autofit_sheet(ws_grouped)

        # --- Sheet 2: Investigation ---
        if df_investigation is not None and not df_investigation.empty:
            # Drop columns from investigation that already exist in df_export
            inv_drop_cols = [c for c in ["Product Description", "AverageCost"]
                            if c in df_investigation.columns]
            inv_clean = df_investigation.drop(columns=inv_drop_cols, errors="ignore")
            inv_export = pd.merge(
                df_export[["Code", "Product Description", "Variance",
                           "AverageCost", "Amount of Variance", "Pareto Category"]],
                inv_clean,
                on="Code",
                how="inner",
            )
            inv_export.to_excel(writer, sheet_name="Investigation", index=False)
            ws_inv = writer.sheets["Investigation"]
            ws_inv.freeze_panes = "C2"
            if "Code" in inv_export.columns:
                idx = inv_export.columns.get_loc("Code") + 1
                for cell in ws_inv.iter_rows(min_row=2, min_col=idx, max_col=idx):
                    cell[0].number_format = "@"
            autofit_sheet(ws_inv)

        # --- Sheet 3: BOM Analysis ---
        if df_bom is not None and not df_bom.empty:
            df_bom.to_excel(writer, sheet_name="BOM Analysis", index=False)
            ws_bom = writer.sheets["BOM Analysis"]
            ws_bom.freeze_panes = "C2"
            if "Code" in df_bom.columns:
                idx = df_bom.columns.get_loc("Code") + 1
                for cell in ws_bom.iter_rows(min_row=2, min_col=idx, max_col=idx):
                    cell[0].number_format = "@"
            autofit_sheet(ws_bom)

        # --- Sheet 4: Refined Data ---
        df_refined.to_excel(writer, sheet_name="Refined Data", index=False)
        ws_refined = writer.sheets["Refined Data"]
        ws_refined.freeze_panes = "D2"
        if "Code" in df_refined.columns:
            idx = df_refined.columns.get_loc("Code") + 1
            for cell in ws_refined.iter_rows(min_row=2, min_col=idx, max_col=idx):
                cell[0].number_format = "@"
        autofit_sheet(ws_refined)

    return output.getvalue()


# =========================================================
# UI
# =========================================================
account_id, api_key = get_api_credentials()

if not account_id or not api_key:
    st.error(
        "Cin7 Core API credentials not found. "
        "Set `CIN7_ACCOUNT_ID` and `CIN7_API_KEY` in your `.env` file (local) "
        "or in Streamlit secrets (cloud deployment)."
    )
    st.stop()

# --- Step 1: File Upload & Variance Analysis ---
with st.form("inputs"):
    st.subheader("Upload your Stocktake PDF")
    pdf_upload = st.file_uploader("Stocktake Variance Table PDF", type=["pdf"])
    run_btn = st.form_submit_button("Run Analysis")

if run_btn:
    if not pdf_upload:
        st.error("Please upload the Stocktake PDF.")
        st.stop()

    # Clear previous results
    for key in ["df_refined", "df_merged", "df_investigation", "df_bom", "excel_bytes"]:
        st.session_state.pop(key, None)

    with st.spinner("Extracting data from PDF..."):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(pdf_upload.read())
            tmp_pdf_path = tmp.name
        try:
            st.session_state.df_refined = extract_and_refine_pdf(tmp_pdf_path)
        finally:
            os.unlink(tmp_pdf_path)

    with st.spinner("Fetching inventory from Cin7 Core API..."):
        try:
            df_inventory = fetch_inventory_from_api(account_id, api_key)
        except requests.HTTPError as e:
            st.error(f"Cin7 API error: {e}")
            st.stop()
        except requests.ConnectionError:
            st.error("Could not connect to Cin7 Core API. Check your internet connection.")
            st.stop()

    with st.spinner("Grouping and merging..."):
        df_grouped_sorted = group_and_sort(st.session_state.df_refined)
        st.session_state.df_merged = merge_with_inventory(df_grouped_sorted, df_inventory)

    # Build Excel without investigation (user can add it next)
    st.session_state.excel_bytes = build_excel(
        st.session_state.df_refined,
        st.session_state.df_merged,
    )

# --- Show results if available ---
if "df_merged" not in st.session_state:
    st.info("Upload the Stocktake PDF above and click **Run Analysis**.")
    st.stop()

df_merged = st.session_state.df_merged
df_refined = st.session_state.df_refined
df_investigation = st.session_state.get("df_investigation")
df_bom = st.session_state.get("df_bom")

st.divider()

# Summary metrics
total_variance_amount = df_merged["Amount of Variance"].sum()
top80_count = len(df_merged[df_merged["Pareto Category"] == "Top 80% Variance"])
total_products = len(df_merged)

c1, c2, c3 = st.columns(3)
c1.metric("Total Products", f"{total_products:,}")
c2.metric("Total Variance Amount", f"R{total_variance_amount:,.2f}")
c3.metric("Top 80% Variance Items", f"{top80_count:,}")

# Data tables
tab1, tab2, tab3, tab4 = st.tabs([
    "Grouped & Sorted Data", "Investigation", "BOM Analysis", "Refined Data",
])

with tab1:
    display_cols = [
        "Code", "Product Description", "Category", "Unit", "Tags",
        "Old Quantity", "Stocktake Quantity", "Variance",
        "AverageCost", "Amount of Variance", "Absolute Amount Variance",
        "Pareto Category",
    ]
    display_cols = [c for c in display_cols if c in df_merged.columns]
    st.dataframe(
        df_merged.drop(columns=["ProductCode"], errors="ignore")[display_cols],
        use_container_width=True,
        hide_index=True,
    )

with tab2:
    if df_investigation is not None and not df_investigation.empty:
        st.dataframe(df_investigation, use_container_width=True, hide_index=True)
    else:
        st.info("Run the investigation below to enrich top SKUs with SOH, stock take, and PO receiving data.")

with tab3:
    if df_bom is not None and not df_bom.empty:
        st.dataframe(df_bom, use_container_width=True, hide_index=True)
    else:
        st.info("Run the investigation below to see BOM relationships (exploded per parent).")

with tab4:
    st.dataframe(df_refined, use_container_width=True, hide_index=True)

# --- Step 2: Investigation (separate, user-controlled) ---
st.divider()
st.subheader("Variance Investigation")

max_skus = len(df_merged)
default_n = min(25, max_skus)

# Estimate: ~1 SOH per SKU + ~3 BOM bulk + ~6 stock takes + ~30 PO receiving
def estimate_time(n):
    total_calls = n + 40
    minutes = total_calls / CALLS_PER_MINUTE
    if minutes < 1:
        return "< 1 min"
    return f"~{minutes:.0f} min"

top_n = st.slider(
    "Number of top SKUs to investigate (sorted by absolute variance):",
    min_value=5,
    max_value=max_skus,
    value=default_n,
    step=5,
)
st.caption(f"Estimated time: **{estimate_time(top_n)}** ({top_n + 40} API calls at {CALLS_PER_MINUTE}/min)")

investigate_btn = st.button("Run Investigation")

if investigate_btn:
    # Get top N SKUs by absolute variance (df_merged is already sorted desc)
    top_rows = df_merged.head(top_n)
    top_skus = top_rows["Code"].dropna().astype(str).str.strip().tolist()
    sku_desc_map = dict(zip(
        top_rows["Code"].astype(str).str.strip(),
        top_rows["Product Description"].fillna(""),
    ))
    sku_cost_map = dict(zip(
        top_rows["Code"].astype(str).str.strip(),
        top_rows["AverageCost"].fillna(0),
    ))

    st.write(f"Investigating top **{len(top_skus)}** SKUs...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    client = Cin7Client(account_id, api_key)
    po_debug = {}
    try:
        df_inv, df_bom_result, po_debug = run_investigation(
            client, top_skus, progress_bar, status_text, sku_desc_map, sku_cost_map,
        )
        st.session_state.df_investigation = df_inv
        st.session_state.df_bom = df_bom_result
    except requests.HTTPError as e:
        st.warning(f"Investigation partially failed: {e}")
        st.session_state.df_investigation = pd.DataFrame()
        st.session_state.df_bom = pd.DataFrame()

    progress_bar.empty()
    status_text.empty()
    st.success(f"Investigation complete for **{len(top_skus)}** SKUs.")

    # Store PO debug in session_state so it survives st.rerun()
    if po_debug:
        st.session_state.po_debug = po_debug

    # Rebuild Excel with investigation + BOM data
    with st.spinner("Rebuilding Excel file..."):
        st.session_state.excel_bytes = build_excel(
            st.session_state.df_refined,
            st.session_state.df_merged,
            st.session_state.get("df_investigation"),
            st.session_state.get("df_bom"),
        )

    st.rerun()

# --- PO Receiving Debug (persisted in session_state) ---
po_debug = st.session_state.get("po_debug")
if po_debug:
    with st.expander("PO Receiving Debug Info", expanded=True):
        st.write(f"**Total POs in list:** {po_debug.get('total_po_list', '?')}")
        st.write(f"**CombinedReceivingStatus values seen:** {po_debug.get('statuses_seen', {})}")
        st.write(f"**POs passing filter:** {po_debug.get('received_po_count', '?')}")
        st.write(f"**PO details fetched:** {po_debug.get('details_checked', '?')}")
        st.write(f"**Total stock lines found:** {po_debug.get('total_lines_found', '?')}")
        st.write(f"**SKUs matched:** {po_debug.get('skus_matched', '?')}")
        if po_debug.get("sample_po"):
            st.write("**Sample PO (first in list):**")
            st.json(po_debug["sample_po"])
        if po_debug.get("sample_detail_keys"):
            st.write(f"**Detail response keys:** {po_debug['sample_detail_keys']}")
        if po_debug.get("sample_stock_received_type"):
            st.write(f"**StockReceived type:** {po_debug['sample_stock_received_type']}")
        if po_debug.get("sample_line_keys"):
            st.write(f"**Stock line keys:** {po_debug['sample_line_keys']}")
        if po_debug.get("errors"):
            st.write("**Errors:**")
            for err in po_debug["errors"][:10]:
                st.write(f"- {err}")

# --- Download ---
st.divider()
timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
inv_label = ""
if df_investigation is not None and not df_investigation.empty:
    inv_label = f" (incl. {len(df_investigation)} investigated)"
st.download_button(
    label=f"Download Variance Analysis XLSX{inv_label}",
    data=st.session_state.excel_bytes,
    file_name=f"stocktake_variance_analysis_{timestamp}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
