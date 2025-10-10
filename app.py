import io
from datetime import datetime
from pytz import timezone

import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border

TZ = timezone("Africa/Johannesburg")
PEREGRINE_BLUE = "00A9E0"   # RGB(0,169,224) as hex (we'll convert for openpyxl)
DIGISCALE_GREEN = "3E5D3E"  # RGB(62,93,62)

# -------------------------
# Streamlit Page Setup
# -------------------------
st.set_page_config(
    page_title="Core | Yoco | Digiscale Reconciliation",
    page_icon="🧾",
    layout="wide"  # instead of "centered"
)

st.title("🧾 Core | Yoco | Digiscale Reconciliation")
st.caption("BULK sheet + Modifiers + Digiscale checks → one XLSX download")

# -------------------------
# Helpers
# -------------------------
def style_and_autofit_sheet(ws, rgb_hex="00A9E0"):
    """Header style + autofit. rgb_hex like '00A9E0' or '3E5D3E'. Converted to ARGB for openpyxl."""
    rgb = rgb_hex.replace("#", "").upper()
    if len(rgb) == 6:
        argb = "00" + rgb  # common pattern used with openpyxl themes
    elif len(rgb) == 8:
        argb = rgb
    else:
        argb = "00A9E0"

    header_fill = PatternFill(start_color=argb, end_color=argb, fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    header_align = Alignment(horizontal="center", vertical="center")
    no_border = Border()

    if ws.max_row >= 1:
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_align
            cell.border = no_border

    # Autofit widths
    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            if len(v) > max_len:
                max_len = len(v)
        ws.column_dimensions[col_letter].width = max(10, max_len + 2)

def pick_latest_by_name(files, *need_any_substr):
    """Return the last uploaded file whose name contains ANY of the substrings (case-insensitive)."""
    matches = [f for f in files if any(s in f.name.lower() for s in need_any_substr)]
    return matches[-1] if matches else None

def read_core_csv(uploaded):
    try:
        return pd.read_csv(uploaded, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(uploaded)

def read_excel_sheet(uploaded_file, sheet_name):
    xls = pd.ExcelFile(uploaded_file)
    if sheet_name not in xls.sheet_names:
        raise KeyError(f"Sheet '{sheet_name}' not found. Found: {xls.sheet_names}")
    return xls.parse(sheet_name)

def norm(s):
    import re
    return re.sub(r'[^a-z0-9]+', ' ', str(s).lower()).strip()

def map_first_available(df, candidates, require_tokens=None):
    norm_cols = {norm(c): c for c in df.columns}
    for k in candidates:
        if k in norm_cols:
            return norm_cols[k]
    if require_tokens:
        toks = require_tokens if isinstance(require_tokens, (list, tuple)) else [require_tokens]
        for nc, raw in norm_cols.items():
            words = nc.split()
            if all(t in words for t in toks):
                return raw
    return None

# -------------------------
# File Upload Section (single upload control)
# -------------------------
with st.form("inputs"):
    st.subheader("Please upload files")
    uploaded_files = st.file_uploader(
        "Upload the Core Inventory, Yoco Products & Modifiers, and Digiscale export files in the section below:",
        accept_multiple_files=True,
        type=["csv", "xlsx", "xls"]
    )
    run_btn = st.form_submit_button("Run reconciliation")

if not run_btn:
    st.info("Upload all required files above and click **Run reconciliation**.")
    st.stop()

# -------------------------
# Detect file types automatically
# -------------------------
core_file = None
yoco_file = None
modifiers_alt = None
digiscale_file = None

for f in uploaded_files or []:
    name = f.name.lower()
    if "inventorylist" in name:
        core_file = f
    elif "peregrine" in name and "farm" in name and "bulk" in name:
        yoco_file = f
    elif "modifier" in name:
        modifiers_alt = f
    elif "digiscale" in name or "plu listing" in name:
        digiscale_file = f

# Validation
if not core_file or not yoco_file:
    st.error("You must include both a Core Inventory CSV (contains 'inventorylist') and a Yoco BULK XLSX.")
    st.stop()

# -------------------------
# Load Data
# -------------------------
# Core
try:
    core_df = read_core_csv(core_file)
except Exception as e:
    st.error(f"Failed to read Core CSV: {e}")
    st.stop()

if "ProductCode" not in core_df.columns:
    st.error("Core CSV must include 'ProductCode'.")
    st.stop()

need_core = {"ProductCode", "PriceTier1"}
missing_core = need_core - set(core_df.columns)
if missing_core:
    st.error(f"Core CSV missing required column(s): {missing_core}")
    st.stop()

# Yoco BULK sheet
BULK_SHEET = "Bulk Site Values"
try:
    yoco_df = read_excel_sheet(yoco_file, BULK_SHEET)
except Exception as e:
    st.error(f"Failed to read Yoco BULK sheet: {e}")
    st.stop()

need_yoco = {"Product PLU", "Name & Variants", "Selling Price"}
missing_yoco = need_yoco - set(yoco_df.columns)
if missing_yoco:
    st.error(f"Yoco BULK missing columns: {missing_yoco}")
    st.stop()

# Modifiers — prefer same workbook (sheet 'Modifier Items - Template'), else fallback file
mod_df = None
try:
    xls = pd.ExcelFile(yoco_file)
    if "Modifier Items - Template" in xls.sheet_names:
        mod_df = xls.parse("Modifier Items - Template")
except Exception:
    pass

if mod_df is None and modifiers_alt is not None:
    try:
        xls_alt = pd.ExcelFile(modifiers_alt)
        if "Modifier Items - Template" in xls_alt.sheet_names:
            mod_df = xls_alt.parse("Modifier Items - Template")
        else:
            # pick first sheet with "modifier" else first sheet
            cand = next((s for s in xls_alt.sheet_names if "modifier" in s.lower()), xls_alt.sheet_names[0])
            mod_df = xls_alt.parse(cand)
    except Exception as e:
        st.error(f"Failed to read Modifiers from fallback file: {e}")
        st.stop()

if mod_df is None:
    st.error("Could not locate 'Modifier Items - Template' in Yoco file or fallback file.")
    st.stop()

# Optional Digiscale/PLU Listing CSV
digi_df = None
if digiscale_file:
    try:
        digi_df = read_core_csv(digiscale_file)
    except Exception as e:
        st.error(f"Failed to read Digiscale / PLU Listing CSV: {e}")
        st.stop()

# -------------------------
# Reconciliations
# -------------------------
# 1) Yoco products vs Core
prod_merge = yoco_df.merge(
    core_df[["ProductCode", "PriceTier1"]],
    left_on="Product PLU",
    right_on="ProductCode",
    how="left"
)

yoco_not_in_core = (
    prod_merge[prod_merge["ProductCode"].isna()][["Product PLU", "Name & Variants"]]
    .rename(columns={"Product PLU": "Product PLU", "Name & Variants": "Name & Variants"})
)

y_price = pd.to_numeric(prod_merge["Selling Price"], errors="coerce")
c_price = pd.to_numeric(prod_merge["PriceTier1"], errors="coerce")
has_core = prod_merge["ProductCode"].notna()
yoco_price_mismatch = (
    prod_merge.loc[has_core & (y_price.ne(c_price)), ["Product PLU", "Name & Variants", "Selling Price", "PriceTier1"]]
    .rename(columns={"Selling Price": "Yoco Price", "PriceTier1": "Core Price"})
)

# 2) Modifiers not in Core
mod_code_col = map_first_available(
    mod_df,
    candidates=[
        "modifier items plu product modifier",
        "modifier items plu",
        "modifier plu",
        "product modifier plu",
        "modifier product plu",
        "plu",
    ],
    require_tokens=["modifier", "plu"]
) or map_first_available(mod_df, candidates=["plu"])

if not mod_code_col:
    st.error("Modifiers sheet missing a PLU/code column.")
    st.stop()

mod_name_col = map_first_available(
    mod_df,
    candidates=["modifier name", "name"],
    require_tokens=["modifier", "name"]
) or map_first_available(mod_df, candidates=["name"])

mod_type_col = map_first_available(
    mod_df,
    candidates=["type products options", "type products options ", "type"],
)

mod_item_col = map_first_available(
    mod_df,
    candidates=["modifier item", "modifier items", "item"],
    require_tokens=["modifier", "item"]
) or map_first_available(mod_df, candidates=["item"])

missing_disp = [c for c, v in {
    "Modifier Name": mod_name_col,
    "Type (Products / Options)": mod_type_col,
    "Modifier Item": mod_item_col
}.items() if not v]
if missing_disp:
    st.error(f"Modifiers sheet missing columns: {missing_disp}")
    st.stop()

mods_merged = mod_df.merge(
    core_df[["ProductCode"]],
    left_on=mod_code_col,
    right_on="ProductCode",
    how="left"
)
mods_not_in_core = mods_merged[mods_merged["ProductCode"].isna()][[mod_name_col, mod_type_col, mod_item_col]].copy()
mods_not_in_core.columns = ["Modifier Name", "Type (Products / Options)", "Modifier Item"]

# 3) Digiscale (optional) — PLU and price checks
if digi_df is not None and not digi_df.empty:
    need_digi = {"PLU #", "Description Line 1", "Price"}
    missing_digi = need_digi - set(digi_df.columns)
    if missing_digi:
        st.error(f"Digiscale/PLU Listing CSV missing columns: {missing_digi}")
        st.stop()

    digi = digi_df.copy()
    digi["PLU #"] = digi["PLU #"].astype(str).str.strip()
    core_map = core_df[["ProductCode", "PriceTier1"]].copy()
    core_map["ProductCode"] = core_map["ProductCode"].astype(str).str.strip()

    digi_merged = digi.merge(core_map, left_on="PLU #", right_on="ProductCode", how="left")

    digi_not_in_core = (
        digi_merged[digi_merged["ProductCode"].isna()][["PLU #", "Description Line 1"]]
        .rename(columns={"PLU #": "Digiscale SKU", "Description Line 1": "Digiscale Name"})
    )

    d_price = pd.to_numeric(digi_merged["Price"], errors="coerce")
    c_price2 = pd.to_numeric(digi_merged["PriceTier1"], errors="coerce")
    has_core2 = digi_merged["ProductCode"].notna()
    digi_price_mismatch = (
        digi_merged.loc[has_core2 & (d_price.ne(c_price2)), ["PLU #", "Description Line 1", "Price", "PriceTier1"]]
        .rename(columns={
            "PLU #": "Digiscale SKU",
            "Description Line 1": "Digiscale Name",
            "Price": "Yoco Price",
            "PriceTier1": "Core Price"
        })
    )
else:
    digi_not_in_core = pd.DataFrame(columns=["Digiscale SKU", "Digiscale Name"])
    digi_price_mismatch = pd.DataFrame(columns=["Digiscale SKU", "Digiscale Name", "Yoco Price", "Core Price"])

# -------------------------
# Build XLSX in-memory
# -------------------------
ts = datetime.now(TZ).strftime("%Y-%m-%d_%H%M")
out_xlsx = io.BytesIO()

with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
    yoco_not_in_core.to_excel(writer, index=False, sheet_name="Yoco Products - Not in Core")
    yoco_price_mismatch.to_excel(writer, index=False, sheet_name="Yoco Products - Price Mismatch")
    mods_not_in_core.to_excel(writer, index=False, sheet_name="Modifiers - Not in Core")
    digi_not_in_core.to_excel(writer, index=False, sheet_name="Digiscale - Not in Core")
    digi_price_mismatch.to_excel(writer, index=False, sheet_name="Digiscale - Price Mismatch")

    # Style after writing
    wb = writer.book
    # Yoco/Modifiers (blue)
    for s in ["Yoco Products - Not in Core", "Yoco Products - Price Mismatch", "Modifiers - Not in Core"]:
        if s in wb.sheetnames:
            style_and_autofit_sheet(wb[s], rgb_hex=PEREGRINE_BLUE)
    # Digiscale (green)
    for s in ["Digiscale - Not in Core", "Digiscale - Price Mismatch"]:
        if s in wb.sheetnames:
            style_and_autofit_sheet(wb[s], rgb_hex=DIGISCALE_GREEN)

# Prepare download
out_xlsx.seek(0)
download_name = f"yoco_core_reconciliation_{ts}.xlsx"

st.success("✅ Reconciliation complete.")
st.download_button(
    label="⬇️ Download Excel",
    data=out_xlsx.getvalue(),
    file_name=download_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

# Quick stats
with st.expander("Show summary"):
    st.write({
        "Yoco Products - Not in Core": len(yoco_not_in_core),
        "Yoco Products - Price Mismatch": len(yoco_price_mismatch),
        "Modifiers - Not in Core": len(mods_not_in_core),
        "Digiscale - Not in Core": len(digi_not_in_core),
        "Digiscale - Price Mismatch": len(digi_price_mismatch),
    })

