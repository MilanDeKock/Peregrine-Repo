import io
import re
from datetime import datetime
from pytz import timezone

import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border

# -------------------------
# Configuration & Styling
# -------------------------
TZ = timezone("Africa/Johannesburg")
PEREGRINE_BLUE = "00A9E0"   # RGB(0,169,224)
DIGISCALE_GREEN = "3E5D3E"  # RGB(62,93,62)
README_RED = "FF0000"       # RGB(255,0,0)

st.set_page_config(page_title="Core | Yoco | Digiscale Reconciliation", page_icon="🧾", layout="centered")

st.markdown(
    """
    <style>
    h1, .stMarkdown h1, .stCaption p { white-space: nowrap !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Helpers
# -------------------------
def style_and_autofit_sheet(ws, rgb_hex="00A9E0"):
    """Header style + autofit. rgb_hex like '00A9E0' or '3E5D3E'."""
    rgb = rgb_hex.replace("#", "").upper()
    argb = ("00" + rgb) if len(rgb) == 6 else (rgb if len(rgb) == 8 else "00A9E0")

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

    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            if len(v) > max_len:
                max_len = len(v)
        ws.column_dimensions[col_letter].width = max(10, max_len + 2)

def create_readme(writer):
    """Creates the README sheet at index 0 with a Red Tab."""
    wb = writer.book
    ws = wb.create_sheet("README", 0)
    ws.sheet_properties.tabColor = README_RED
    
    content = [
        ["SHEET NAME", "PURPOSE / ACTION REQUIRED"],
        ["Yoco Products - Not in Core", "Items found in Yoco but missing from Core. Action: Add to Core or fix SKU."],
        ["Yoco Products - Price Mismatch", "Selling prices don't match. Action: Sync prices between systems."],
        ["Core Sellable - Not in Yoco", "Items marked 'Sellable' in Core but missing from Yoco. Action: Add to Yoco."],
        ["Modifiers - Not in Core", "Modifier items missing from Core. Action: Ensure all modifiers have Core PLUs."],
        ["Digiscale - Not in Core", "Items on the Digiscale list not found in Core. Action: Update Core inventory."],
        ["Digiscale - Price Mismatch", "Scale prices vs Core prices. Action: Update scale pricing."]
    ]
    for row in content:
        ws.append(row)
    style_and_autofit_sheet(ws, rgb_hex=README_RED)

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

def norm_plu_series(s):
    return s.astype(str).str.strip().str.upper()

# -------------------------
# UI: File Upload Section
# -------------------------
st.title("🧾 Core | Yoco | Digiscale Reconciliation")
st.caption("BULK sheet + Modifiers + Digiscale checks → one XLSX download")

with st.form("inputs"):
    st.subheader("Please upload reconciliation files")
    uploaded_files = st.file_uploader(
        "Upload Core CSV, Yoco XLSX, Modifiers, and Digiscale CSV all at once:",
        accept_multiple_files=True,
        type=["csv", "xlsx", "xls"]
    )
    run_btn = st.form_submit_button("Run reconciliation")

if not run_btn:
    st.info("Upload required files above to start Phase 1.")
    st.stop()

# -------------------------
# Phase 1 Logic
# -------------------------
core_file = yoco_file = modifiers_alt = digiscale_file = None

for f in uploaded_files or []:
    name = f.name.lower()
    if "inventorylist" in name: core_file = f
    elif "peregrine" in name and "farm" in name and "bulk" in name: yoco_file = f
    elif "modifier" in name: modifiers_alt = f
    elif "digiscale" in name or "plu listing" in name: digiscale_file = f

if not core_file or not yoco_file:
    st.error("Missing Core Inventory CSV or Yoco BULK XLSX.")
    st.stop()

# Data Loading
core_df = read_core_csv(core_file)
yoco_df = read_excel_sheet(yoco_file, "Bulk Site Values")

# Core/Yoco Recon
yoco_df["PLU_norm"] = norm_plu_series(yoco_df["Product PLU"])
core_df["ProductCode_norm"] = norm_plu_series(core_df["ProductCode"])

prod_merge = yoco_df.merge(core_df, left_on="PLU_norm", right_on="ProductCode_norm", how="left")
yoco_not_in_core = prod_merge[prod_merge["ProductCode"].isna()][["Product PLU", "Name & Variants"]]

y_price = pd.to_numeric(prod_merge["Selling Price"], errors="coerce")
c_price = pd.to_numeric(prod_merge["PriceTier1"], errors="coerce")
yoco_price_mismatch = prod_merge.loc[prod_merge["ProductCode"].notna() & (y_price.ne(c_price)), 
                                     ["Product PLU", "Name & Variants", "Selling Price", "PriceTier1"]] \
                                     .rename(columns={"Selling Price": "Yoco Price", "PriceTier1": "Core Price"})

sellable_mask = core_df["Sellable"].astype(str).str.strip().str.lower().eq("yes")
core_sellable_merge = core_df.loc[sellable_mask].merge(yoco_df, left_on="ProductCode_norm", right_on="PLU_norm", how="left")
core_sellable_not_in_yoco = core_sellable_merge[core_sellable_merge["Product PLU"].isna()][["ProductCode", "Name"]]

# Modifiers
try:
    mod_df = read_excel_sheet(yoco_file, "Modifier Items - Template")
except:
    mod_df = pd.read_excel(modifiers_alt) if modifiers_alt else None

if mod_df is not None:
    mod_df["_PLU_norm"] = norm_plu_series(mod_df.iloc[:, 0])
    mods_merged = mod_df.merge(core_df, left_on="_PLU_norm", right_on="ProductCode_norm", how="left")
    mods_not_in_core = mods_merged[mods_merged["ProductCode"].isna()].iloc[:, :3]
else:
    mods_not_in_core = pd.DataFrame()

# Digiscale
digi_not_in_core = pd.DataFrame()
digi_price_mismatch = pd.DataFrame()
if digiscale_file:
    digi_df = read_core_csv(digiscale_file)
    digi_df["_PLU_norm"] = norm_plu_series(digi_df["PLU #"])
    digi_merged = digi_df.merge(core_df, left_on="_PLU_norm", right_on="ProductCode_norm", how="left")
    digi_not_in_core = digi_merged[digi_merged["ProductCode"].isna()][["PLU #", "Description Line 1"]]
    d_p = pd.to_numeric(digi_merged["Price"], errors="coerce")
    c_p = pd.to_numeric(digi_merged["PriceTier1"], errors="coerce")
    digi_price_mismatch = digi_merged.loc[digi_merged["ProductCode"].notna() & (d_p.ne(c_p)), 
                                         ["PLU #", "Description Line 1", "Price", "PriceTier1"]]

# -------------------------
# Export Phase 1
# -------------------------
ts = datetime.now(TZ).strftime("%Y-%m-%d_%H%M")
out_xlsx = io.BytesIO()
with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
    create_readme(writer)
    yoco_not_in_core.to_excel(writer, index=False, sheet_name="Yoco Products - Not in Core")
    yoco_price_mismatch.to_excel(writer, index=False, sheet_name="Yoco Products - Price Mismatch")
    core_sellable_not_in_yoco.to_excel(writer, index=False, sheet_name="Core Sellable - Not in Yoco")
    mods_not_in_core.to_excel(writer, index=False, sheet_name="Modifiers - Not in Core")
    digi_not_in_core.to_excel(writer, index=False, sheet_name="Digiscale - Not in Core")
    digi_price_mismatch.to_excel(writer, index=False, sheet_name="Digiscale - Price Mismatch")

    wb = writer.book
    for s in wb.sheetnames:
        if s == "README": continue
        color = DIGISCALE_GREEN if "Digiscale" in s else PEREGRINE_BLUE
        style_and_autofit_sheet(wb[s], rgb_hex=color)

st.success("✅ Reconciliation Complete.")
st.download_button(
    label="⬇️ Download Reconciliation XLSX",
    data=out_xlsx.getvalue(),
    file_name=f"recon_report_{ts}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# -------------------------
# Phase 2: Monthly Health Check
# -------------------------
st.divider()
st.subheader("🔍 Phase 2: Monthly Health Check")
st.caption("Upload the pre-filtered analysis file with 5 sheets.")
health_file = st.file_uploader("Upload Health Check XLSX:", type=["xlsx"])

if health_file:
    try:
        # Load sheets
        df_dupes = pd.read_excel(health_file, sheet_name="Duplicate Sales")
        df_margin = pd.read_excel(health_file, sheet_name="Assembly BOM vs Margin Check")
        df_stock = pd.read_excel(health_file, sheet_name="Stock Adjustment Analysis")
        df_bom = pd.read_excel(health_file, sheet_name="Deleted BOM Lines Check")

        # Counts & Logic
        dupe_count = len(df_dupes)
        stock_issues = df_stock[df_stock["Unit Cost"].isin([0, 1])]
        bom_issues = df_bom[df_bom["Status"].astype(str).str.contains("❌ MISSING", na=False)]
        
        # Margin sorting
        low_margin = df_margin.sort_values("GP Margin").head(5)[["ProductSKU", "ProductName", "GP Margin"]]
        high_margin = df_margin.sort_values("GP Margin", ascending=False).head(5)[["ProductSKU", "ProductName", "GP Margin"]]

        # Display Metrics
        c1, c2, c3 = st.columns(3)
        c1.metric("Duplicate Sales", f"{dupe_count} rows")
        c2.metric("R0/R1 Stock Adjusts", f"{len(stock_issues)} items")
        c3.metric("BOM Missing Lines", f"{len(bom_issues)} assemblies")

        # Margin Tables
        st.write("### 📊 Margin Highlights")
        m_col1, m_col2 = st.columns(2)
        with m_col1:
            st.write("**Bottom 5 (Lowest)**")
            st.dataframe(low_margin, hide_index=True)
        with m_col2:
            st.write("**Top 5 (Highest)**")
            st.dataframe(high_margin, hide_index=True)

        # --- Email Summary Generation ---
        st.divider()
        st.subheader("✉️ Email Summary")
        email_body = f"""Hi Team,

Please find the reconciliation and health check summary for the period:

RECONCILIATION SUMMARY:
- Yoco Items not in Core: {len(yoco_not_in_core)}
- Price Mismatches (Yoco vs Core): {len(yoco_price_mismatch)}
- Missing from Yoco (Sellable in Core): {len(core_sellable_not_in_yoco)}
- Digiscale Price Mismatches: {len(digi_price_mismatch)}

MONTHLY HEALTH CHECK:
- Duplicate Sales: {dupe_count} rows found.
- Stock Adjustments: {len(stock_issues)} items adjusted at R0 or R1.
- BOM Integrity: {len(bom_issues)} assemblies found with missing BOM lines.
- Lowest Margin Item: {low_margin.iloc[0]['ProductName'] if not low_margin.empty else 'N/A'} ({low_margin.iloc[0]['GP Margin'] if not low_margin.empty else '0'}%)

Please refer to the attached reports for the full details.

Best regards,"""

        st.text_area("Copy and paste this summary into your email body:", value=email_body, height=380)

    except Exception as e:
        st.error(f"Error processing health check file: {e}")
