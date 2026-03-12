import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta
import json

# ── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EDI Corporate Actions",
    page_icon="📊",
    layout="wide",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f1117; }
    .stApp { background-color: #0f1117; color: #e0e0e0; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; color: #ffffff; }
    div[data-testid="stMetricLabel"] { font-size: 0.75rem; color: #888; }
    .event-badge {
        display: inline-block; padding: 2px 10px; border-radius: 12px;
        font-size: 0.75rem; font-weight: 600;
    }
    .badge-cash   { background: #1a3a2a; color: #4caf50; border: 1px solid #4caf50; }
    .badge-special{ background: #3a2a1a; color: #ff9800; border: 1px solid #ff9800; }
    .badge-stock  { background: #1a2a3a; color: #2196f3; border: 1px solid #2196f3; }
    .badge-split  { background: #2a1a3a; color: #9c27b0; border: 1px solid #9c27b0; }
    .badge-rights { background: #3a1a1a; color: #f44336; border: 1px solid #f44336; }
    .badge-other  { background: #2a2a2a; color: #aaa;    border: 1px solid #aaa; }
    section[data-testid="stSidebar"] { background-color: #161b22; }
    .stDataFrame { background-color: #161b22; }
    h1, h2, h3 { color: #ffffff; }
    .stAlert { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)


# ── Classification Logic (EDI Spec) ──────────────────────────────────────────
US_MICS = {"XNAS", "XNYS"}

def classify_event(row: dict) -> dict:
    """
    Apply EDI specification classification rules.
    Returns dict with: event_type, subtype, dividend_amount, tax_marker,
    dividend_currency, stock_dividend_pct, stock_dividend_ratio,
    split_ratio, subscription_price, subscription_currency, subscription_ratio
    """
    result = {
        "event_type": "Other",
        "subtype": "",
        "dividend_amount": "",
        "tax_marker": "",
        "dividend_currency": "",
        "stock_dividend_pct": "",
        "stock_dividend_ratio": "",
        "split_ratio": "",
        "subscription_price": "",
        "subscription_currency": "",
        "subscription_ratio": "",
        "ignore": False,
    }

    eventcd     = (row.get("eventcd")     or "").upper().strip()
    marker      = (row.get("marker")      or "").upper().strip()
    paytypecd   = (row.get("paytypecd")   or "").upper().strip()
    outsectycd  = (row.get("outsectycd")  or "").upper().strip()
    op_mic      = (row.get("operationalmic") or "").upper().strip()

    gross       = row.get("grossdividend") or ""
    net         = row.get("netdividend")   or ""
    cashback    = row.get("cashback")      or ""
    ratecurencd = row.get("ratecurencd")   or ""
    rationew    = row.get("rationew")      or ""
    ratioold    = row.get("ratioold")      or ""
    issueprice  = row.get("issueprice")    or ""
    entissueprice = row.get("entissueprice") or ""
    depfees     = row.get("depfees")       or ""

    is_us       = op_mic in US_MICS
    is_au       = op_mic == "XASX"
    is_br       = op_mic == "BVMF"

    # ── Helper: safe ratio calc ──
    def safe_div(a, b):
        try:
            return float(a) / float(b)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    # ── Rights Issue ──────────────────────────────────────────────────────────
    if eventcd in {"RTS", "ENT"}:
        result["event_type"] = "Rights Issue"
        sub_price = issueprice if issueprice else entissueprice
        result["subscription_price"]    = sub_price
        result["subscription_currency"] = ratecurencd
        ratio = safe_div(rationew, ratioold)
        result["subscription_ratio"] = f"{ratio:.6f}" if ratio else ""
        return result

    # ── Stock Split ───────────────────────────────────────────────────────────
    if eventcd in {"SD", "FSPLT"}:
        result["event_type"] = "Stock Split"
        result["subtype"]    = "Forward Stock Split"
        ratio = safe_div(rationew, ratioold)
        result["split_ratio"] = f"{ratio:.6f}" if ratio else ""
        return result

    if eventcd in {"CONSD", "RSPLT"}:
        result["event_type"] = "Stock Split"
        result["subtype"]    = "Reverse Stock Split"
        ratio = safe_div(rationew, ratioold)
        result["split_ratio"] = f"{ratio:.6f}" if ratio else ""
        return result

    # ── US: DIV/BON + paytypecd=S → Stock Split ──────────────────────────────
    if is_us and eventcd in {"DIV", "BON"} and paytypecd == "S":
        if outsectycd == "WAR":
            result["ignore"] = True
            return result
        result["event_type"] = "Stock Split"
        result["subtype"]    = "Forward Stock Split"
        try:
            rn = float(rationew)
            ro = float(ratioold)
            result["split_ratio"] = f"{(rn + ro) / ro:.6f}"
        except (TypeError, ValueError):
            pass
        return result

    # ── non-US: DIV/BON + paytypecd=S → Stock Dividend ───────────────────────
    if not is_us and eventcd in {"DIV", "BON"} and paytypecd == "S":
        if outsectycd == "WAR":
            result["ignore"] = True
            return result
        result["event_type"] = "Stock Dividend"
        result["subtype"]    = "Bonus Issue" if eventcd == "BON" else ""
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            result["stock_dividend_pct"]   = f"{ratio * 100:.4f}%"
            result["stock_dividend_ratio"] = f"{1 + ratio:.6f}"
        return result

    # ── DIV + paytypecd=B → Both Cash & Stock Dividend ────────────────────────
    if eventcd == "DIV" and paytypecd == "B":
        result["event_type"] = "Cash + Stock Dividend"
        result["subtype"]    = "Both"
        # Cash side
        if gross:
            result["dividend_amount"]   = gross
            result["tax_marker"]        = "GROSS"
        elif net:
            result["dividend_amount"]   = net
            result["tax_marker"]        = "GROSS"
        result["dividend_currency"] = ratecurencd
        # Stock side
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            result["stock_dividend_pct"]   = f"{ratio * 100:.4f}%"
            result["stock_dividend_ratio"] = f"{1 + ratio:.6f}"
        return result

    # ── Return of Capital (Special Dividend) ──────────────────────────────────
    if eventcd == "RCAP":
        result["event_type"]        = "Special Dividend"
        result["subtype"]           = "Return of Capital"
        result["dividend_amount"]   = cashback
        result["tax_marker"]        = "NET"
        result["dividend_currency"] = ratecurencd
        return result

    # ── Liquidation / Memorial (Special Dividend) ─────────────────────────────
    if eventcd in {"LIQ", "MEM"}:
        result["event_type"]  = "Special Dividend"
        result["subtype"]     = "Liquidation" if eventcd == "LIQ" else "Memorial"
        if gross:
            result["dividend_amount"] = gross
            result["tax_marker"]      = "GROSS"
        elif net:
            result["dividend_amount"] = net
            result["tax_marker"]      = "GROSS"
        result["dividend_currency"] = ratecurencd
        return result

    # ── DIV, DIVIF, DRIP, FRANK, PID ─────────────────────────────────────────
    if eventcd in {"DIV", "DIVIF", "DRIP", "FRANK", "PID"}:
        # Subtype detection
        if marker == "SPL":
            result["event_type"] = "Special Dividend"
        elif marker == "ISC":
            result["event_type"] = "Cash Dividend"
            result["subtype"]    = "Interest on Capital"
        elif marker in {"CGS"}:
            result["event_type"] = "Special Dividend"
            result["subtype"]    = "Short-Term Capital Gains"
        elif marker in {"CGL"}:
            result["event_type"] = "Special Dividend"
            result["subtype"]    = "Long-Term Capital Gains"
        elif eventcd == "PID":
            result["event_type"] = "Cash Dividend"
            result["subtype"]    = "Property Income Distribution (PID)"
        else:
            result["event_type"] = "Cash Dividend"

        # Amount logic
        if is_au:
            result["dividend_amount"] = net if net else gross
            result["tax_marker"]      = "GROSS"
        else:
            if gross:
                result["dividend_amount"] = gross
                result["tax_marker"]      = "GROSS"
            elif net:
                result["dividend_amount"] = net
                result["tax_marker"]      = "GROSS"

        result["dividend_currency"] = ratecurencd

        # Brazil ISC override
        if is_br and marker == "ISC":
            result["subtype"]    = "Interest on Capital"
            result["tax_marker"] = "GROSS (15% WHT)"

        return result

    return result


# ── Raw field display config ──────────────────────────────────────────────────
RAW_COLUMNS = [
    "eventid", "optionid", "eventcd", "marker", "paytypecd", "mandvoluflag",
    "exdt", "paydt", "recorddt", "declarationdt",
    "grossdividend", "netdividend", "divrate", "cashback",
    "ratioold", "rationew", "ratecurencd",
    "issueprice", "entissueprice", "depfees",
    "outsectycd", "operationalmic", "isin", "issuername",
    "frequency", "periodenddt", "ntschangedt",
    "feedgendate", "evtactioncd", "lstactioncd", "ntsactioncd",
    "voting", "defaultoptionflag", "optionelectiondt",
]

EVENT_TYPE_COLORS = {
    "Cash Dividend":          "badge-cash",
    "Special Dividend":       "badge-special",
    "Stock Dividend":         "badge-stock",
    "Cash or Stock Dividend": "badge-stock",
    "Cash + Stock Dividend":  "badge-stock",
    "Stock Split":            "badge-split",
    "Rights Issue":           "badge-rights",
    "Other":                  "badge-other",
}


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://via.placeholder.com/180x40/0f1117/4a90d9?text=NaroIX", width=160)
    st.markdown("## ⚙️ EDI API Settings")

    api_key = st.text_input("API Key", type="password", placeholder="Bearer token...")
    st.divider()

    st.markdown("### 🔍 Query Parameters")
    isin = st.text_input("ISIN", placeholder="e.g. CH1256740924")
    op_mic = st.text_input("Operational MIC", placeholder="e.g. XSWX")
    
    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("From Ex-Date", value=date.today() - timedelta(days=365))
    with col2:
        to_date = st.date_input("To Ex-Date", value=date.today() + timedelta(days=180))

    st.divider()
    st.markdown("### 🎛️ Display Filters")
    show_ignored = st.checkbox("Show ignored events (WAR)", value=False)
    
    event_type_filter = st.multiselect(
        "Filter by Event Type",
        options=["Cash Dividend", "Special Dividend", "Stock Dividend",
                 "Cash or Stock Dividend", "Cash + Stock Dividend",
                 "Stock Split", "Rights Issue", "Other"],
        default=[]
    )

    fetch_btn = st.button("🔄 Fetch Corporate Actions", use_container_width=True, type="primary")


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("📊 EDI Corporate Actions Viewer")
st.caption("Live data from Exchange Data International (EDI) API")

if not fetch_btn:
    st.info("👈 Configure query parameters in the sidebar and click **Fetch Corporate Actions**.")
    
    with st.expander("📖 Classification Logic Reference", expanded=False):
        st.markdown("""
        | Event Type | Subtype | EDI Condition |
        |---|---|---|
        | Cash Dividend | — | `eventcd` ∈ {DIV, DIVIF, DRIP, FRANK, PID}, `marker` ≠ SPL |
        | Cash Dividend | Interest on Capital | `marker` = ISC |
        | Cash Dividend | PID | `eventcd` = PID |
        | Special Dividend | — | `marker` = SPL |
        | Special Dividend | Return of Capital | `eventcd` = RCAP |
        | Special Dividend | Liquidation | `eventcd` = LIQ |
        | Special Dividend | Short-Term Cap Gains | `marker` = CGS |
        | Special Dividend | Long-Term Cap Gains | `marker` = CGL |
        | Stock Dividend | — | non-US, `eventcd` ∈ {DIV,BON}, `paytypecd` = S |
        | Stock Split | Forward | US only, `eventcd` ∈ {DIV,BON}, `paytypecd` = S or `eventcd` ∈ {SD, FSPLT} |
        | Stock Split | Reverse | `eventcd` ∈ {CONSD, RSPLT} |
        | Rights Issue | — | `eventcd` ∈ {RTS, ENT} |
        """)
    st.stop()

# ── API Call ──────────────────────────────────────────────────────────────────
if not api_key:
    st.error("⚠️ Please enter your EDI API Key in the sidebar.")
    st.stop()

if not isin:
    st.error("⚠️ Please enter an ISIN.")
    st.stop()

url = (
    f"https://api3.exchange-data.com/GetHistoricalCorporateActions"
    f"?format=JSON&ISIN={isin}"
    f"{'&operationalMic=' + op_mic if op_mic else ''}"
    f"&fromexdate={from_date.strftime('%Y-%m-%d')}"
    f"&toexdate={to_date.strftime('%Y-%m-%d')}"
)

headers = {"authorization": api_key}

with st.spinner("Fetching data from EDI API..."):
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        # Show response meta
        rec_count   = response.headers.get("X-Record-Count", "–")
        total_recs  = response.headers.get("X-Total-Records", "–")
        rate_remain = response.headers.get("X-Ratelimit-Remaining", "–")
        rate_limit  = response.headers.get("X-Ratelimit-Limit", "–")

        if response.status_code != 200:
            st.error(f"API Error {response.status_code}: {response.text[:500]}")
            st.stop()

        data = response.json()
        records = data.get("jsondata", [])

    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to EDI API. Check your network or API URL.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Unexpected error: {e}")
        st.stop()

# ── Meta Row ──────────────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("ISIN", isin)
m2.metric("Records Returned", rec_count)
m3.metric("Total Records", total_recs)
m4.metric("Rate Limit", rate_limit)
m5.metric("Rate Remaining", rate_remain)

st.divider()

if not records:
    st.warning("No corporate action records found for the given parameters.")
    st.stop()

# ── Step 1: Deduplicate — per eventid+optionid keep latest feedgendate ────────
def parse_feedgendate(val):
    if not val:
        return pd.Timestamp.min
    try:
        return pd.Timestamp(val)
    except Exception:
        return pd.Timestamp.min

# Build raw dataframe for dedup
raw_df = pd.DataFrame(records)
if "feedgendate" in raw_df.columns:
    raw_df["_feedgendate_ts"] = raw_df["feedgendate"].apply(parse_feedgendate)
    raw_df = (
        raw_df
        .sort_values("_feedgendate_ts", ascending=False)
        .drop_duplicates(subset=["eventid", "optionid"], keep="first")
        .drop(columns=["_feedgendate_ts"])
    )
deduped_records = raw_df.to_dict(orient="records")

# ── Step 2: Merge Election Events (voting=V, multiple optionids) ──────────────
def merge_election_events(records_list):
    """
    Group by eventid. If voting=V and multiple optionids exist,
    merge into a single row combining Cash (optionid=1) and Stock (optionid=2) data.
    """
    from collections import defaultdict
    groups = defaultdict(list)
    for r in records_list:
        groups[r.get("eventid", "")].append(r)

    merged = []
    for eid, group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Check if this is an election event
        votings = [r.get("voting", "") for r in group]
        is_election = any(v == "V" for v in votings)
        option_ids  = [r.get("optionid", "") for r in group]

        if not is_election or len(set(option_ids)) < 2:
            # Not an election — keep all records separately
            merged.extend(group)
            continue

        # Find cash option (optionid=1, paytypecd=C) and stock option (optionid=2, paytypecd=S)
        cash_row  = next((r for r in group if str(r.get("optionid","")) == "1"), None)
        stock_row = next((r for r in group if str(r.get("optionid","")) == "2"), None)

        if not cash_row or not stock_row:
            merged.extend(group)
            continue

        # Use cash_row as base (has grossdividend, netdividend, ratecurencd)
        combined = dict(cash_row)
        combined["_is_election"]         = True
        combined["_opt1_paytypecd"]      = cash_row.get("paytypecd", "")
        combined["_opt2_paytypecd"]      = stock_row.get("paytypecd", "")
        combined["_opt1_grossdividend"]  = cash_row.get("grossdividend", "")
        combined["_opt1_netdividend"]    = cash_row.get("netdividend", "")
        combined["_opt1_defaultflag"]    = cash_row.get("defaultoptionflag", "")
        combined["_opt2_rationew"]       = stock_row.get("rationew", "")
        combined["_opt2_ratioold"]       = stock_row.get("ratioold", "")
        combined["_opt2_defaultflag"]    = stock_row.get("defaultoptionflag", "")
        combined["optionelectiondt"]     = stock_row.get("optionelectiondt", "") or cash_row.get("optionelectiondt", "")
        # Keep stock ratio fields for classification
        combined["rationew"]             = stock_row.get("rationew", "")
        combined["ratioold"]             = stock_row.get("ratioold", "")
        combined["paytypecd"]            = "B"  # signal both options present

        merged.append(combined)

    return merged

processed_records = merge_election_events(deduped_records)

# ── Step 3: Classify + Build Final DataFrame ──────────────────────────────────
rows = []
for r in processed_records:
    is_election = r.get("_is_election", False)
    classification = classify_event(r)

    if classification["ignore"] and not show_ignored:
        continue

    row = {}
    for col in RAW_COLUMNS:
        row[col] = r.get(col, "")

    # Override Event_Type for election events
    if is_election:
        row["Event_Type"]      = "Cash or Stock Dividend"
        row["Subtype"]         = "Shareholder Election"
        # Cash side
        row["Dividend_Amount"] = r.get("_opt1_grossdividend") or r.get("_opt1_netdividend") or ""
        row["Tax_Marker"]      = "GROSS"
        row["Dividend_Currency"] = r.get("ratecurencd", "")
        # Stock side
        try:
            rn = float(r.get("_opt2_rationew") or 0)
            ro = float(r.get("_opt2_ratioold") or 0)
            if ro:
                row["Stock_Div_Pct"]   = f"{(rn/ro)*100:.4f}%"
                row["Stock_Div_Ratio"] = f"{1+(rn/ro):.6f}"
            else:
                row["Stock_Div_Pct"]   = ""
                row["Stock_Div_Ratio"] = ""
        except (TypeError, ValueError):
            row["Stock_Div_Pct"]   = ""
            row["Stock_Div_Ratio"] = ""
        # Default option — always Cash for election events (voting=V)
        row["Default_Option"]  = "Cash"
        row["Split_Ratio"]     = ""
        row["Sub_Price"]       = ""
        row["Sub_Currency"]    = ""
        row["Sub_Ratio"]       = ""
    else:
        row["Event_Type"]      = classification["event_type"]
        row["Subtype"]         = classification["subtype"]
        row["Dividend_Amount"] = classification["dividend_amount"]
        row["Tax_Marker"]      = classification["tax_marker"]
        row["Dividend_Currency"] = classification["dividend_currency"]
        row["Stock_Div_Pct"]   = classification["stock_dividend_pct"]
        row["Stock_Div_Ratio"] = classification["stock_dividend_ratio"]
        row["Split_Ratio"]     = classification["split_ratio"]
        row["Sub_Price"]       = classification["subscription_price"]
        row["Sub_Currency"]    = classification["subscription_currency"]
        row["Sub_Ratio"]       = classification["subscription_ratio"]
        row["Default_Option"]  = ""

    row["_ignored"] = classification["ignore"]
    rows.append(row)

df = pd.DataFrame(rows)

# Apply event type filter
if event_type_filter:
    df = df[df["Event_Type"].isin(event_type_filter)]

if df.empty:
    st.warning("No events match the current filters.")
    st.stop()

# ── Summary Metrics ───────────────────────────────────────────────────────────
st.subheader(f"📋 {len(df)} Events — {df['issuername'].iloc[0] if 'issuername' in df.columns and len(df) > 0 else isin}")

type_counts = df["Event_Type"].value_counts()
cols = st.columns(min(len(type_counts), 6))
for i, (etype, cnt) in enumerate(type_counts.items()):
    badge_cls = EVENT_TYPE_COLORS.get(etype, "badge-other")
    cols[i % len(cols)].markdown(
        f'<div style="text-align:center">'
        f'<span class="event-badge {badge_cls}">{etype}</span>'
        f'<br><b style="font-size:1.5rem">{cnt}</b></div>',
        unsafe_allow_html=True
    )

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["🏷️ Classified Events", "📄 Raw API Fields", "🔎 Event Detail"])

with tab1:
    display_cols = [
        "Event_Type", "Subtype", "eventcd", "marker", "paytypecd",
        "exdt", "paydt", "recorddt",
        "Dividend_Amount", "Tax_Marker", "Dividend_Currency",
        "Stock_Div_Pct", "Stock_Div_Ratio", "Split_Ratio",
        "Sub_Price", "Sub_Currency", "Sub_Ratio",
        "Default_Option", "optionelectiondt",
        "feedgendate", "evtactioncd", "lstactioncd", "ntsactioncd",
        "eventid", "optionid", "isin", "issuername", "operationalmic",
    ]
    display_cols = [c for c in display_cols if c in df.columns]
    
    st.dataframe(
        df[display_cols].drop(columns=["_ignored"], errors="ignore"),
        use_container_width=True,
        height=500,
        column_config={
            "Event_Type":       st.column_config.TextColumn("Event Type", width=150),
            "Subtype":          st.column_config.TextColumn("Subtype", width=180),
            "exdt":             st.column_config.DateColumn("Ex-Date"),
            "paydt":            st.column_config.DateColumn("Pay Date"),
            "recorddt":         st.column_config.DateColumn("Record Date"),
            "Dividend_Amount":  st.column_config.NumberColumn("Div Amount", format="%.4f"),
            "Sub_Price":           st.column_config.NumberColumn("Sub Price", format="%.4f"),
            "Default_Option":      st.column_config.TextColumn("Default Option", width=110),
            "optionelectiondt":    st.column_config.TextColumn("Election Deadline", width=130),
            "feedgendate":         st.column_config.TextColumn("Feed Gen Date", width=130),
            "evtactioncd":         st.column_config.TextColumn("Evt Action", width=90),
            "lstactioncd":         st.column_config.TextColumn("LST Action", width=90),
            "ntsactioncd":         st.column_config.TextColumn("NTS Action", width=90),
            "optionid":            st.column_config.TextColumn("Option ID", width=80),
        }
    )

with tab2:
    raw_cols = [c for c in RAW_COLUMNS if c in df.columns]
    st.dataframe(df[raw_cols], use_container_width=True, height=500)

with tab3:
    if len(df) > 0:
        event_options = [
            f"{row['eventid']} | {row['Event_Type']} | Ex: {row['exdt']}"
            for _, row in df.iterrows()
        ]
        selected = st.selectbox("Select Event", event_options)
        idx = event_options.index(selected)
        sel_row = df.iloc[idx]
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**🏷️ Classification**")
            st.json({
                "Event_Type":        sel_row.get("Event_Type"),
                "Subtype":           sel_row.get("Subtype"),
                "Dividend_Amount":   sel_row.get("Dividend_Amount"),
                "Tax_Marker":        sel_row.get("Tax_Marker"),
                "Dividend_Currency": sel_row.get("Dividend_Currency"),
                "Stock_Div_Pct":     sel_row.get("Stock_Div_Pct"),
                "Stock_Div_Ratio":   sel_row.get("Stock_Div_Ratio"),
                "Split_Ratio":       sel_row.get("Split_Ratio"),
                "Sub_Price":         sel_row.get("Sub_Price"),
                "Sub_Currency":      sel_row.get("Sub_Currency"),
                "Sub_Ratio":         sel_row.get("Sub_Ratio"),
                "Default_Option":    sel_row.get("Default_Option"),
                "Election_Deadline": sel_row.get("optionelectiondt"),
                "Feed_Gen_Date":     sel_row.get("feedgendate"),
                "Evt_Action":        sel_row.get("evtactioncd"),
                "LST_Action":        sel_row.get("lstactioncd"),
                "NTS_Action":        sel_row.get("ntsactioncd"),
            })
        with c2:
            st.markdown("**📄 Raw Fields**")
            raw_dict = {col: sel_row.get(col, "") for col in RAW_COLUMNS if col in sel_row}
            st.json(raw_dict)

# ── Export ────────────────────────────────────────────────────────────────────
st.divider()
col_dl1, col_dl2, _ = st.columns([1, 1, 4])
with col_dl1:
    csv = df.drop(columns=["_ignored"], errors="ignore").to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, f"edi_ca_{isin}.csv", "text/csv")
with col_dl2:
    json_export = df.drop(columns=["_ignored"], errors="ignore").to_json(orient="records", indent=2)
    st.download_button("⬇️ Download JSON", json_export, f"edi_ca_{isin}.json", "application/json")
