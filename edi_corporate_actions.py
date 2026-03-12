import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="EDI Corporate Actions", page_icon="📊", layout="wide")

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
    .badge-cash     { background: #1a3a2a; color: #4caf50; border: 1px solid #4caf50; }
    .badge-special  { background: #3a2a1a; color: #ff9800; border: 1px solid #ff9800; }
    .badge-stock    { background: #1a2a3a; color: #2196f3; border: 1px solid #2196f3; }
    .badge-split    { background: #2a1a3a; color: #9c27b0; border: 1px solid #9c27b0; }
    .badge-rights   { background: #3a1a1a; color: #f44336; border: 1px solid #f44336; }
    .badge-takeover { background: #1a2a2a; color: #00bcd4; border: 1px solid #00bcd4; }
    .badge-demerger { background: #2a1a2a; color: #e040fb; border: 1px solid #e040fb; }
    .badge-merger   { background: #1a1a2a; color: #7986cb; border: 1px solid #7986cb; }
    .badge-other    { background: #2a2a2a; color: #aaa;    border: 1px solid #aaa; }
    section[data-testid="stSidebar"] { background-color: #161b22; }
    h1, h2, h3 { color: #ffffff; }
    .stAlert { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
US_MICS = {"XNAS", "XNYS"}

EVENT_TYPE_COLORS = {
    "Cash Dividend":          "badge-cash",
    "Special Dividend":       "badge-special",
    "Stock Dividend":         "badge-stock",
    "Cash or Stock Dividend": "badge-stock",
    "Cash + Stock Dividend":  "badge-stock",
    "Stock Split":            "badge-split",
    "Rights Issue":           "badge-rights",
    "Merger & Acquisition":   "badge-takeover",
    "Spin-Off":               "badge-demerger",
    "Stock Distribution":     "badge-demerger",
    "Other":                  "badge-other",
}

RAW_COLUMNS = [
    "eventid", "optionid", "eventcd", "relatedeventcd", "eventsubtypecd",
    "marker", "paytypecd", "mandvoluflag",
    "exdt", "paydt", "recorddt", "declarationdt", "effectivedt",
    "expcompletiondt",
    "grossdividend", "netdividend", "divrate", "cashback",
    "ratioold", "rationew", "ratecurencd",
    "issueprice", "entissueprice", "depfees",
    "outsectycd", "operationalmic", "isin", "issuername",
    "offerorname", "outisin", "outbbgcompticker",
    "minimumprice", "maximumprice", "hostile", "mrgrstatus",
    "unconditionaldt", "compulsoryacqdt",
    "frequency", "periodenddt", "ntschangedt",
    "eventcreatedt", "feedgendate", "evtactioncd", "lstactioncd", "ntsactioncd",
    "voting", "defaultoptionflag", "optionelectiondt",
    "closedt",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_div(a, b):
    try:
        return float(a) / float(b)
    except (TypeError, ValueError, ZeroDivisionError):
        return None

def parse_feedgendate(val):
    if not val:
        return pd.Timestamp.min
    try:
        return pd.Timestamp(val)
    except Exception:
        return pd.Timestamp.min


# ── Classification ────────────────────────────────────────────────────────────
def classify_event(row: dict) -> dict:
    result = {
        "event_type": "Other", "subtype": "",
        "dividend_amount": "", "tax_marker": "", "dividend_currency": "",
        "stock_dividend_pct": "", "stock_dividend_ratio": "",
        "split_ratio": "",
        "subscription_price": "", "subscription_currency": "", "subscription_ratio": "",
        # MA / Deal fields (shared across TKOVR, DMRGR, MRGR, DIST)
        "ma_subtype": "", "ma_deal_type": "", "ma_offeror": "", "ma_hostile": "",
        "ma_cash_terms": "", "ma_cash_terms_currency": "",
        "ma_stock_ratio": "", "ma_offeror_isin": "", "ma_offeror_ticker": "",
        "ma_cash_terms": "", "ma_cash_terms_currency": "",
        "ma_mandatory_voluntary": "",
        "ma_effective_date": "", "ma_exp_completion": "",
        "ma_merger_status": "", "ma_event_subtype": "",
        "ignore": False,
    }

    eventcd    = (row.get("eventcd")        or "").upper().strip()
    marker     = (row.get("marker")         or "").upper().strip()
    paytypecd  = (row.get("paytypecd")      or "").upper().strip()
    outsectycd = (row.get("outsectycd")     or "").upper().strip()
    op_mic     = (row.get("operationalmic") or "").upper().strip()
    gross         = row.get("grossdividend")  or ""
    net           = row.get("netdividend")    or ""
    cashback      = row.get("cashback")       or ""
    ratecurencd   = row.get("ratecurencd")    or ""
    rationew      = row.get("rationew")       or ""
    ratioold      = row.get("ratioold")       or ""
    issueprice    = row.get("issueprice")     or ""
    entissueprice = row.get("entissueprice")  or ""
    is_us = op_mic in US_MICS
    is_au = op_mic == "XASX"
    is_br = op_mic == "BVMF"

    # ── TKOVR ─────────────────────────────────────────────────────────────────
    if eventcd == "TKOVR":
        result["event_type"]             = "Merger & Acquisition"
        result["ma_subtype"]             = ""
        result["ma_offeror"]             = row.get("offerorname") or ""
        result["ma_hostile"]             = row.get("hostile")     or ""
        result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        result["ma_event_subtype"]       = row.get("eventsubtypecd") or ""
        if paytypecd == "C":
            result["ma_deal_type"]     = "Cash"
            result["ma_cash_terms"]    = row.get("minimumprice") or row.get("maximumprice") or ""
            result["ma_cash_terms_currency"] = row.get("ratecurencd") or row.get("tradingcurencd") or ""
        elif paytypecd == "S":
            result["ma_deal_type"]      = "Stock"
            result["ma_offeror_isin"]   = row.get("outisin")         or ""
            result["ma_offeror_ticker"] = row.get("outbbgcompticker") or ""
            ratio = safe_div(rationew, ratioold)
            result["ma_stock_ratio"]    = f"{ratio:.6f}" if ratio else ""
        elif paytypecd == "B":
            result["ma_deal_type"]         = "Cash & Stock"
            result["ma_cash_terms"]        = row.get("minimumprice") or row.get("maximumprice") or ""
            result["ma_cash_terms_currency"]    = row.get("ratecurencd") or row.get("tradingcurencd") or ""
            result["ma_cash_terms_currency"]     = row.get("ratecurencd") or row.get("tradingcurencd") or ""
            result["ma_offeror_isin"]      = row.get("outisin")         or ""
            result["ma_offeror_ticker"]    = row.get("outbbgcompticker") or ""
            ratio = safe_div(rationew, ratioold)
            result["ma_stock_ratio"] = f"{ratio:.6f}" if ratio else ""
        elif paytypecd == "D":
            result["ma_deal_type"] = "Debenture"
        else:
            result["ma_deal_type"] = paytypecd
        return result

    # ── DMRGR (Spin-Off / Demerger) ───────────────────────────────────────────
    if eventcd == "DMRGR":
        result["event_type"]             = "Spin-Off"
        result["ma_subtype"]             = "Demerger"
        result["ma_deal_type"]           = "Stock"
        result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        result["ma_offeror_ticker"]      = row.get("outbbgcompticker") or ""
        result["ma_offeror_isin"]        = row.get("outisin") or ""
        result["ma_effective_date"]      = row.get("effectivedt") or ""
        result["ma_exp_completion"]      = row.get("expcompletiondt") or ""
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            rn = float(rationew); ro = float(ratioold)
            if rn == int(rn) and ro == int(ro):
                result["ma_stock_ratio"] = f"{int(rn)} : {int(ro)}"
            else:
                result["ma_stock_ratio"] = f"{ratio:.6f}"
        return result

    # ── MRGR (Merger — target distributes acquirer shares) ────────────────────
    if eventcd == "MRGR":
        result["event_type"]             = "Merger & Acquisition"
        result["ma_subtype"]             = ""
        result["ma_deal_type"]           = "Stock" if paytypecd == "S" else ("Cash" if paytypecd == "C" else (paytypecd or "Stock"))
        result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        result["ma_offeror_ticker"]      = row.get("outbbgcompticker") or ""
        result["ma_offeror_isin"]        = row.get("outisin") or ""
        result["ma_merger_status"]       = row.get("mrgrstatus") or ""
        result["ma_effective_date"]      = row.get("effectivedt") or ""
        result["ma_exp_completion"]      = row.get("expcompletiondt") or ""
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            result["ma_stock_ratio"] = f"{ratio:.6f}"
        if paytypecd in ("", None) or paytypecd == "C":
            result["ma_cash_terms"]    = row.get("minimumprice") or row.get("maximumprice") or ""
            result["ma_cash_terms_currency"] = row.get("ratecurencd") or row.get("tradingcurencd") or ""
        return result

    # ── DIST (Stock Distribution — e.g. Reverse Morris Trust distribution) ────
    if eventcd == "DIST":
        result["event_type"]             = "Stock Distribution"
        result["ma_subtype"]             = "Share Distribution"
        result["ma_deal_type"]           = "Stock"
        result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        result["ma_offeror_ticker"]      = row.get("outbbgcompticker") or ""
        result["ma_offeror_isin"]        = row.get("outisin") or ""
        result["ma_effective_date"]      = row.get("effectivedt") or ""
        result["ma_exp_completion"]      = row.get("expcompletiondt") or ""
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            result["ma_stock_ratio"] = f"{ratio:.6f}"
        return result

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

    # ── US: DIV/BON + S → Stock Split ────────────────────────────────────────
    if is_us and eventcd in {"DIV", "BON"} and paytypecd == "S":
        if outsectycd == "WAR":
            result["ignore"] = True
            return result
        result["event_type"] = "Stock Split"
        result["subtype"]    = "Forward Stock Split"
        try:
            rn = float(rationew); ro = float(ratioold)
            result["split_ratio"] = f"{(rn + ro) / ro:.6f}"
        except (TypeError, ValueError):
            pass
        return result

    # ── non-US: DIV/BON + S → Stock Dividend ─────────────────────────────────
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

    # ── DIV + B → Cash & Stock Dividend ──────────────────────────────────────
    if eventcd == "DIV" and paytypecd == "B":
        result["event_type"] = "Cash + Stock Dividend"
        result["subtype"]    = "Both"
        if gross:
            result["dividend_amount"] = gross; result["tax_marker"] = "GROSS"
        elif net:
            result["dividend_amount"] = net;   result["tax_marker"] = "GROSS"
        result["dividend_currency"] = ratecurencd
        ratio = safe_div(rationew, ratioold)
        if ratio is not None:
            result["stock_dividend_pct"]   = f"{ratio * 100:.4f}%"
            result["stock_dividend_ratio"] = f"{1 + ratio:.6f}"
        return result

    # ── RCAP ──────────────────────────────────────────────────────────────────
    if eventcd == "RCAP":
        result["event_type"]        = "Special Dividend"
        result["subtype"]           = "Return of Capital"
        result["dividend_amount"]   = cashback
        result["tax_marker"]        = "NET"
        result["dividend_currency"] = ratecurencd
        return result

    # ── LIQ / MEM ─────────────────────────────────────────────────────────────
    if eventcd in {"LIQ", "MEM"}:
        result["event_type"] = "Special Dividend"
        result["subtype"]    = "Liquidation" if eventcd == "LIQ" else "Memorial"
        if gross:
            result["dividend_amount"] = gross; result["tax_marker"] = "GROSS"
        elif net:
            result["dividend_amount"] = net;   result["tax_marker"] = "GROSS"
        result["dividend_currency"] = ratecurencd
        return result

    # ── DIV / DIVIF / DRIP / FRANK / PID ─────────────────────────────────────
    if eventcd in {"DIV", "DIVIF", "DRIP", "FRANK", "PID"}:
        if marker == "SPL":
            result["event_type"] = "Special Dividend"
        elif marker == "ISC":
            result["event_type"] = "Cash Dividend"; result["subtype"] = "Interest on Capital"
        elif marker == "CGS":
            result["event_type"] = "Special Dividend"; result["subtype"] = "Short-Term Capital Gains"
        elif marker == "CGL":
            result["event_type"] = "Special Dividend"; result["subtype"] = "Long-Term Capital Gains"
        elif eventcd == "PID":
            result["event_type"] = "Cash Dividend"; result["subtype"] = "Property Income Distribution (PID)"
        else:
            result["event_type"] = "Cash Dividend"

        if is_au:
            result["dividend_amount"] = net if net else gross
            result["tax_marker"]      = "GROSS"
        else:
            if gross:
                result["dividend_amount"] = gross; result["tax_marker"] = "GROSS"
            elif net:
                result["dividend_amount"] = net;   result["tax_marker"] = "GROSS"

        result["dividend_currency"] = ratecurencd
        if is_br and marker == "ISC":
            result["subtype"] = "Interest on Capital"; result["tax_marker"] = "GROSS (15% WHT)"
        return result

    # ── ANN (Announcement) → route by relatedeventcd ─────────────────────────
    if eventcd == "ANN":
        related = (row.get("relatedeventcd") or "").upper().strip()
        if related == "MRGR":
            result["event_type"] = "Merger & Acquisition"
            result["ma_subtype"] = "Announcement"
            result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        elif related == "DMRGR":
            result["event_type"] = "Spin-Off"
            result["ma_subtype"] = "Announcement"
            result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        elif related == "TKOVR":
            result["event_type"] = "Merger & Acquisition"
            result["ma_subtype"] = "Announcement"
            result["ma_mandatory_voluntary"] = row.get("mandvoluflag") or ""
        return result

    return result


# ── Step 1: Deduplicate ───────────────────────────────────────────────────────
def deduplicate(records):
    raw_df = pd.DataFrame(records)
    raw_df["_ts"] = raw_df["feedgendate"].apply(parse_feedgendate)
    raw_df = (
        raw_df
        .sort_values("_ts", ascending=False)
        .drop_duplicates(subset=["eventid", "optionid", "operationalmic"], keep="first")
        .drop(columns=["_ts"])
    )
    return raw_df.to_dict(orient="records")


# ── Step 2: Merge ─────────────────────────────────────────────────────────────
def merge_events(records_list):
    groups = defaultdict(list)
    for r in records_list:
        key = (r.get("eventid", ""), r.get("operationalmic", ""))
        groups[key].append(r)

    merged = []
    for (eid, mic), group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        eventcd    = (group[0].get("eventcd") or "").upper().strip()
        option_ids = [r.get("optionid", "") for r in group]

        # ── TKOVR: multiple optionids → merge all options ─────────────────────
        if eventcd == "TKOVR" and len(set(option_ids)) > 1:
            base = dict(sorted(group, key=lambda r: str(r.get("optionid", "")))[0])
            base["_is_tkovr_election"] = True
            paytypes = sorted(set(r.get("paytypecd", "") for r in group))
            base["_tkovr_paytypes"] = paytypes

            cash_opt  = next((r for r in group if r.get("paytypecd") == "C"), None)
            stock_opt = next((r for r in group if r.get("paytypecd") == "S"), None)
            mixed_opt = next((r for r in group if r.get("paytypecd") == "B"), None)

            if cash_opt:
                base["_ma_cash_terms"]    = cash_opt.get("minimumprice") or cash_opt.get("maximumprice") or ""
                base["_ma_cash_terms_currency"] = cash_opt.get("ratecurencd") or cash_opt.get("tradingcurencd") or ""
            if stock_opt:
                base["_ma_offeror_isin"]   = stock_opt.get("outisin")          or ""
                base["_ma_offeror_ticker"] = stock_opt.get("outbbgcompticker")  or ""
                ratio = safe_div(stock_opt.get("rationew"), stock_opt.get("ratioold"))
                base["_ma_stock_ratio"]    = f"{ratio:.6f}" if ratio else ""
            if mixed_opt:
                base["_ma_cash_terms"]        = mixed_opt.get("minimumprice") or mixed_opt.get("maximumprice") or ""
                base["_ma_cash_terms_currency"]    = mixed_opt.get("ratecurencd") or mixed_opt.get("tradingcurencd") or ""
                base["_ma_offeror_isin"]      = base.get("_ma_offeror_isin")   or mixed_opt.get("outisin")          or ""
                base["_ma_offeror_ticker"]    = base.get("_ma_offeror_ticker") or mixed_opt.get("outbbgcompticker")  or ""
                ratio = safe_div(mixed_opt.get("rationew"), mixed_opt.get("ratioold"))
                if ratio and not base.get("_ma_stock_ratio"):
                    base["_ma_stock_ratio"] = f"{ratio:.6f}"
                elif ratio:
                    base["_ma_cash_terms_ratio"] = f"{ratio:.6f}"  # preserve for Mixed Cash component

            merged.append(base)
            continue

        # ── Dividend Election: voting=V, multiple optionids ───────────────────
        votings = [r.get("voting", "") for r in group]
        if any(v == "V" for v in votings) and len(set(option_ids)) > 1:
            cash_row  = next((r for r in group if str(r.get("optionid", "")) == "1"), None)
            stock_row = next((r for r in group if str(r.get("optionid", "")) == "2"), None)
            if not cash_row or not stock_row:
                merged.extend(group)
                continue
            combined = dict(cash_row)
            combined["_is_election"]        = True
            combined["_opt1_grossdividend"] = cash_row.get("grossdividend", "")
            combined["_opt1_netdividend"]   = cash_row.get("netdividend", "")
            combined["_opt2_rationew"]      = stock_row.get("rationew", "")
            combined["_opt2_ratioold"]      = stock_row.get("ratioold", "")
            combined["optionelectiondt"]    = (stock_row.get("optionelectiondt") or
                                               cash_row.get("optionelectiondt") or "")
            combined["rationew"]            = stock_row.get("rationew", "")
            combined["ratioold"]            = stock_row.get("ratioold", "")
            combined["paytypecd"]           = "B"
            merged.append(combined)
            continue

        merged.extend(group)

    return merged


# ── Step 3: Build rows ────────────────────────────────────────────────────────
MA_FIELDS = [
    "MA_Offeror", "MA_Hostile", "MA_Mand_Vol", "MA_Event_Subtype",
    "Deal_Type",
    "MA_Cash_Terms", "MA_Cash_Terms_Currency",
    "Spun_Off_Terms", "MA_Stock_Ratio", "MA_Offeror_ISIN", "MA_Offeror_Ticker",
    "MA_Effective_Date", "MA_Exp_Completion",
    "MA_Merger_Status",
    "MA_Close_Date",
]
DIV_FIELDS = ["Dividend_Amount","Tax_Marker","Dividend_Currency",
              "Stock_Div_Pct","Stock_Div_Ratio","Split_Ratio",
              "Sub_Price","Sub_Currency","Sub_Ratio","Default_Option",
              "Creation_Date"]

def build_rows(processed_records, show_ignored):
    rows = []
    for r in processed_records:
        is_election       = r.get("_is_election", False)
        is_tkovr_election = r.get("_is_tkovr_election", False)
        cl                = classify_event(r)

        if cl["ignore"] and not show_ignored:
            continue

        row = {col: r.get(col, "") for col in RAW_COLUMNS}
        # initialise derived fields
        for f in DIV_FIELDS + MA_FIELDS:
            row[f] = ""

        if is_tkovr_election:
            paytypes = r.get("_tkovr_paytypes", [])
            label_map = {"C": "Cash", "S": "Stock", "B": "Cash & Stock", "D": "Debenture"}
            deal_type_label = " + ".join(label_map.get(p, p) for p in paytypes)
            row["Event_Type"]        = "Merger & Acquisition"
            row["Subtype"]           = "Election"
            row["Deal_Type"]         = deal_type_label
            row["MA_Offeror"]        = r.get("offerorname", "")
            row["MA_Hostile"]        = r.get("hostile", "")
            row["MA_Mand_Vol"]       = r.get("mandvoluflag", "")
            row["MA_Event_Subtype"]  = r.get("eventsubtypecd", "")
            row["MA_Cash_Terms"]     = r.get("_ma_cash_terms", "")
            row["MA_Cash_Terms_Currency"]  = r.get("_ma_cash_terms_currency", "")
            row["MA_Stock_Ratio"]    = r.get("_ma_stock_ratio", "")
            row["MA_Offeror_ISIN"]   = r.get("_ma_offeror_isin", "")
            row["MA_Offeror_Ticker"] = r.get("_ma_offeror_ticker", "")
            row["MA_Close_Date"]     = r.get("closedt", "")

        elif cl["event_type"] == "Merger & Acquisition":
            row["Event_Type"]        = "Merger & Acquisition"
            row["Subtype"]           = cl["ma_subtype"]   # "" or "Announcement"
            row["Deal_Type"]         = cl["ma_deal_type"]
            row["MA_Offeror"]        = cl["ma_offeror"]
            row["MA_Hostile"]        = cl["ma_hostile"]
            row["MA_Mand_Vol"]       = cl["ma_mandatory_voluntary"]
            row["MA_Event_Subtype"]  = cl["ma_event_subtype"]
            row["MA_Cash_Terms"]     = cl["ma_cash_terms"]
            row["MA_Cash_Terms_Currency"]  = cl["ma_cash_terms_currency"]
            row["MA_Stock_Ratio"]    = cl["ma_stock_ratio"]
            row["MA_Cash_Terms"]     = cl["ma_cash_terms"]
            row["MA_Cash_Terms_Currency"] = cl["ma_cash_terms_currency"]
            row["MA_Offeror_ISIN"]   = cl["ma_offeror_isin"]
            row["MA_Offeror_Ticker"] = cl["ma_offeror_ticker"]
            row["MA_Effective_Date"] = cl["ma_effective_date"]
            row["MA_Exp_Completion"] = cl["ma_exp_completion"]
            row["MA_Merger_Status"]  = cl["ma_merger_status"]
            row["MA_Close_Date"]     = r.get("closedt", "")

        elif cl["event_type"] in ("Spin-Off", "Stock Distribution"):
            row["Event_Type"]        = cl["event_type"]
            row["Subtype"]           = cl["ma_subtype"]   # "Demerger", "Share Distribution", "Announcement"
            row["Deal_Type"]         = cl["ma_deal_type"]
            row["MA_Mand_Vol"]       = cl["ma_mandatory_voluntary"]
            row["MA_Stock_Ratio"]    = cl["ma_stock_ratio"]
            spun_terms = safe_div(r.get("rationew"), r.get("ratioold"))
            row["Spun_Off_Terms"]    = f"{spun_terms:.6f}" if spun_terms else ""
            row["MA_Offeror_ISIN"]   = cl["ma_offeror_isin"]
            row["MA_Offeror_Ticker"] = cl["ma_offeror_ticker"]
            row["MA_Effective_Date"] = cl["ma_effective_date"]
            row["MA_Exp_Completion"] = cl["ma_exp_completion"]
            row["MA_Merger_Status"]  = cl["ma_merger_status"]
            row["MA_Cash_Terms"]     = cl["ma_cash_terms"]
            row["MA_Cash_Terms_Currency"]  = cl["ma_cash_terms_currency"]

        elif is_election:
            row["Event_Type"]        = "Cash or Stock Dividend"
            row["Subtype"]           = "Shareholder Election"
            row["Dividend_Amount"]   = r.get("_opt1_grossdividend") or r.get("_opt1_netdividend") or ""
            row["Tax_Marker"]        = "GROSS"
            row["Dividend_Currency"] = r.get("ratecurencd", "")
            ratio = safe_div(r.get("_opt2_rationew"), r.get("_opt2_ratioold"))
            row["Stock_Div_Pct"]     = f"{ratio*100:.4f}%" if ratio else ""
            row["Stock_Div_Ratio"]   = f"{1+ratio:.6f}"    if ratio else ""
            row["Default_Option"]    = "Cash"

        else:
            row["Event_Type"]        = cl["event_type"]
            row["Subtype"]           = cl["subtype"]
            row["Dividend_Amount"]   = cl["dividend_amount"]
            row["Tax_Marker"]        = cl["tax_marker"]
            row["Dividend_Currency"] = cl["dividend_currency"]
            row["Stock_Div_Pct"]     = cl["stock_dividend_pct"]
            row["Stock_Div_Ratio"]   = cl["stock_dividend_ratio"]
            row["Split_Ratio"]       = cl["split_ratio"]
            row["Sub_Price"]         = cl["subscription_price"]
            row["Sub_Currency"]      = cl["subscription_currency"]
            row["Sub_Ratio"]         = cl["subscription_ratio"]

        row["_ignored"] = cl["ignore"]
        # Creation_Date — universal across all event types
        row["Creation_Date"] = r.get("eventcreatedt", "")
        rows.append(row)
    return rows


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ EDI API Settings")
    api_key = st.text_input("API Key", type="password", placeholder="Bearer token...")
    st.divider()
    st.markdown("### 🔍 Query Parameters")
    isin   = st.text_input("ISIN", placeholder="e.g. CH1256740924")
    op_mic = st.text_input("Operational MIC", placeholder="e.g. XSWX")
    use_dates = st.checkbox("Filter by Ex-Date range", value=False)
    if use_dates:
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("From Ex-Date", value=date.today() - timedelta(days=365))
        with col2:
            to_date   = st.date_input("To Ex-Date",   value=date.today() + timedelta(days=180))
    else:
        from_date = None
        to_date   = None
    st.divider()
    st.markdown("### 🎛️ Display Filters")
    show_ignored = st.checkbox("Show ignored events (WAR)", value=False)
    event_type_filter = st.multiselect(
        "Filter by Event Type",
        options=["Cash Dividend", "Special Dividend", "Stock Dividend",
                 "Cash or Stock Dividend", "Cash + Stock Dividend",
                 "Stock Split", "Rights Issue",
                 "Merger & Acquisition", "Spin-Off", "Stock Distribution",
                 "Other"],
        default=[]
    )
    fetch_btn = st.button("🔄 Fetch Corporate Actions", use_container_width=True, type="primary")


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("📊 EDI Corporate Actions Viewer")
st.caption("Live data from Exchange Data International (EDI) API")

if not fetch_btn:
    with st.expander("📖 Classification Logic Reference", expanded=False):
        st.markdown("""
        **Dividends**
        | Event Type | Subtype | EDI Condition |
        |---|---|---|
        | Cash Dividend | — | `eventcd` ∈ {DIV,DIVIF,DRIP,FRANK,PID}, `marker` ≠ SPL |
        | Cash Dividend | Interest on Capital | `marker` = ISC |
        | Special Dividend | — | `marker` = SPL |
        | Special Dividend | Return of Capital | `eventcd` = RCAP |
        | Special Dividend | Liquidation/Memorial | `eventcd` ∈ {LIQ,MEM} |
        | Stock Dividend | — | non-US, `eventcd` ∈ {DIV,BON}, `paytypecd` = S |
        | Cash or Stock Dividend | Shareholder Election | `voting`=V, multiple optionids |

        **Corporate Events**
        | Event Type | Subtype | EDI Condition |
        |---|---|---|
        | Stock Split | Forward | US: `eventcd` ∈ {DIV,BON,SD,FSPLT}, `paytypecd`=S |
        | Stock Split | Reverse | `eventcd` ∈ {CONSD,RSPLT} |
        | Rights Issue | — | `eventcd` ∈ {RTS,ENT} |

        **Takeovers / M&A / Spin-Offs**
        | Event Type | Subtype | Deal Type | EDI Condition |
        |---|---|---|---|
        | Merger & Acquisition | — | Cash | `eventcd`=TKOVR, `paytypecd`=C |
        | Merger & Acquisition | — | Stock | `eventcd`=TKOVR, `paytypecd`=S |
        | Merger & Acquisition | — | Cash & Stock | `eventcd`=TKOVR, `paytypecd`=B |
        | Merger & Acquisition | — | Debenture | `eventcd`=TKOVR, `paytypecd`=D |
        | Merger & Acquisition | Election | Cash + Stock + … | `eventcd`=TKOVR, multiple optionids |
        | Merger & Acquisition | — | Stock | `eventcd`=MRGR, `paytypecd`=S |
        | Merger & Acquisition | Announcement | — | `eventcd`=ANN, `relatedeventcd`=MRGR/TKOVR |
        | Spin-Off | Demerger | Stock | `eventcd`=DMRGR |
        | Spin-Off | Announcement | — | `eventcd`=ANN, `relatedeventcd`=DMRGR |
        | Stock Distribution | Share Distribution | Stock | `eventcd`=DIST |
        """)
    # Only stop if there's no cached data to show
    if "edi_records" not in st.session_state:
        st.info("👈 Configure query parameters in the sidebar and click **Fetch Corporate Actions**.")
        st.stop()

# ── API Call ──────────────────────────────────────────────────────────────────
if not api_key:
    st.error("⚠️ Please enter your EDI API Key in the sidebar.")
    st.stop()
if not isin:
    st.error("⚠️ Please enter an ISIN.")
    st.stop()

# Cache results in session_state — only re-fetch when button is clicked
if fetch_btn:
    url = (
        f"https://api3.exchange-data.com/GetHistoricalCorporateActions"
        f"?format=JSON&ISIN={isin}"
        f"{'&operationalMic=' + op_mic if op_mic else ''}"
        f"{'&fromexdate=' + from_date.strftime('%Y-%m-%d') if from_date else ''}"
        f"{'&toexdate='   + to_date.strftime('%Y-%m-%d')   if to_date   else ''}"
    )
    with st.spinner("Fetching data from EDI API..."):
        try:
            response = requests.get(url, headers={"authorization": api_key}, timeout=30)
            st.session_state["edi_records"]     = response.json().get("jsondata", [])
            st.session_state["edi_rec_count"]   = response.headers.get("X-Record-Count",       "–")
            st.session_state["edi_total_recs"]  = response.headers.get("X-Total-Records",      "–")
            st.session_state["edi_rate_remain"] = response.headers.get("X-Ratelimit-Remaining","–")
            st.session_state["edi_rate_limit"]  = response.headers.get("X-Ratelimit-Limit",    "–")
            st.session_state["edi_isin"]        = isin
            if response.status_code != 200:
                st.error(f"API Error {response.status_code}: {response.text[:500]}")
                st.stop()
        except requests.exceptions.ConnectionError:
            st.error("❌ Could not connect to EDI API.")
            st.stop()
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
            st.stop()

if "edi_records" not in st.session_state:
    st.info("👈 Configure query parameters in the sidebar and click **Fetch Corporate Actions**.")
    st.stop()

records     = st.session_state["edi_records"]
rec_count   = st.session_state["edi_rec_count"]
total_recs  = st.session_state["edi_total_recs"]
rate_remain = st.session_state["edi_rate_remain"]
rate_limit  = st.session_state["edi_rate_limit"]

# ── Meta ──────────────────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("ISIN", st.session_state.get("edi_isin", isin))
m2.metric("Records Returned", rec_count)
m3.metric("Total Records", total_recs)
m4.metric("Rate Limit", rate_limit)
m5.metric("Rate Remaining", rate_remain)
st.divider()

if not records:
    st.warning("No corporate action records found for the given parameters.")
    st.stop()

# ── Process ───────────────────────────────────────────────────────────────────
deduped   = deduplicate(records)
processed = merge_events(deduped)
rows      = build_rows(processed, show_ignored)
df        = pd.DataFrame(rows)

if event_type_filter:
    df = df[df["Event_Type"].isin(event_type_filter)]

if df.empty:
    st.warning("No events match the current filters.")
    st.stop()

# ── Summary ───────────────────────────────────────────────────────────────────
issuer = df["issuername"].iloc[0] if "issuername" in df.columns else isin
st.subheader(f"📋 {len(df)} Events — {issuer}")

type_counts = df["Event_Type"].value_counts()
cols = st.columns(min(len(type_counts), 7))
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
    hide_other = st.toggle("Hide 'Other' events", value=True)
    df_display = df[df["Event_Type"] != "Other"] if hide_other else df
    div_display = [
        "Event_Type", "Subtype", "eventcd", "marker", "paytypecd",
        "exdt", "paydt", "recorddt",
        "Dividend_Amount", "Tax_Marker", "Dividend_Currency",
        "Stock_Div_Pct", "Stock_Div_Ratio", "Split_Ratio",
        "Sub_Price", "Sub_Currency", "Sub_Ratio",
        "Default_Option", "optionelectiondt",
    ]
    ma_display = [
        "MA_Offeror", "MA_Hostile", "MA_Mand_Vol", "MA_Event_Subtype",
        "Deal_Type",
        "MA_Cash_Terms", "MA_Cash_Terms_Currency",
        "Spun_Off_Terms", "MA_Stock_Ratio", "MA_Offeror_ISIN", "MA_Offeror_Ticker",
        "MA_Effective_Date", "MA_Exp_Completion",
        "MA_Merger_Status",
        "MA_Close_Date",
    ]
    meta_display = [
        "Creation_Date",
        "feedgendate", "evtactioncd", "lstactioncd", "ntsactioncd",
        "eventid", "optionid", "isin", "issuername", "operationalmic",
    ]
    display_cols = [c for c in div_display + ma_display + meta_display if c in df.columns]
    st.dataframe(
        df_display[display_cols],
        use_container_width=True,
        height=500,
        column_config={
            "Event_Type":           st.column_config.TextColumn("Event Type",          width=160),
            "Subtype":              st.column_config.TextColumn("Subtype",              width=210),
            "exdt":                 st.column_config.DateColumn("Ex-Date"),
            "paydt":                st.column_config.DateColumn("Pay Date"),
            "recorddt":             st.column_config.DateColumn("Record Date"),
            "Dividend_Amount":      st.column_config.NumberColumn("Div Amount",        format="%.4f"),
            "Sub_Price":            st.column_config.NumberColumn("Sub Price",          format="%.4f"),
            "MA_Cash_Terms":          st.column_config.NumberColumn("Cash Terms",          format="%.4f"),
            "MA_Cash_Terms_Currency": st.column_config.TextColumn("Cash Terms Currency",  width=120),
            "Default_Option":       st.column_config.TextColumn("Default Option",       width=110),
            "optionelectiondt":     st.column_config.TextColumn("Election DL",          width=120),
            "MA_Offeror":           st.column_config.TextColumn("Offeror",              width=190),
            "MA_Hostile":           st.column_config.TextColumn("Hostile",              width=70),
            "MA_Mand_Vol":          st.column_config.TextColumn("M/V",                 width=50),
            "MA_Event_Subtype":     st.column_config.TextColumn("Deal Subtype",         width=120),
            "Deal_Type":            st.column_config.TextColumn("Deal Type",             width=120),
            "Spun_Off_Terms":       st.column_config.NumberColumn("Spun-Off Terms",     width=115, format="%.6f"),
            "MA_Stock_Ratio":       st.column_config.TextColumn("Stock Terms",            width=120),
            "MA_Offeror_ISIN":      st.column_config.TextColumn("Counterparty ISIN",    width=140),
            "MA_Offeror_Ticker":    st.column_config.TextColumn("Counterparty Ticker",  width=130),
            "MA_Effective_Date":    st.column_config.TextColumn("Effective Date",        width=120),
            "MA_Exp_Completion":    st.column_config.TextColumn("Exp. Completion",      width=125),
            "MA_Merger_Status":     st.column_config.TextColumn("Merger Status",        width=100),
            "MA_Close_Date":        st.column_config.TextColumn("Close Date",           width=110),
            "Creation_Date":        st.column_config.TextColumn("Creation Date",         width=130),
            "feedgendate":          st.column_config.TextColumn("Feed Gen Date",         width=130),
            "evtactioncd":          st.column_config.TextColumn("Evt Action",            width=80),
            "lstactioncd":          st.column_config.TextColumn("LST Action",            width=80),
            "ntsactioncd":          st.column_config.TextColumn("NTS Action",            width=80),
            "optionid":             st.column_config.TextColumn("Option ID",             width=75),
        }
    )

with tab2:
    raw_cols = [c for c in RAW_COLUMNS if c in df.columns]
    st.dataframe(df[raw_cols], use_container_width=True, height=500)

with tab3:
    if len(df) > 0:
        def _event_label(row):
            date_hint = (row["exdt"] or
                         str(row.get("Creation_Date", ""))[:10] or
                         str(row.get("feedgendate", ""))[:10])
            subtype   = row.get("Subtype", "")
            deal_type = row.get("Deal_Type", "")
            type_hint = " · ".join(filter(None, [subtype, deal_type])) or "—"
            return (f"{row['eventid']} | {row['Event_Type']} — "
                    f"{type_hint} | {row.get('issuername','')} | {date_hint}")

        event_options = [_event_label(row) for _, row in df.iterrows()]
        selected = st.selectbox("Select Event", event_options, key="tab3_select")
        idx = event_options.index(selected)
        sel = df.iloc[idx].to_dict()

        c1, c2 = st.columns(2)
        with c1:
            # ── Classification ─────────────────────────────────────────────
            st.markdown("**🏷️ Classification**")
            evt = str(sel.get("Event_Type", ""))
            is_deal = evt in ("Merger & Acquisition", "Spin-Off", "Stock Distribution")

            if is_deal:
                detail = {
                    "Event_Type":          sel.get("Event_Type"),
                    "Subtype":             sel.get("Subtype"),
                    "Deal_Type":           sel.get("Deal_Type"),
                    "Mandatory_Voluntary": sel.get("MA_Mand_Vol"),
                }
                if evt == "Merger & Acquisition":
                    detail.update({
                        "Offeror":           sel.get("MA_Offeror"),
                        "Hostile":           sel.get("MA_Hostile"),
                        "Deal_Subtype_Code": sel.get("MA_Event_Subtype"),
                    })
                detail.update({
                    "Counterparty_Ticker": sel.get("MA_Offeror_Ticker"),
                    "Counterparty_ISIN":   sel.get("MA_Offeror_ISIN"),
                    "Spun_Off_Terms":      sel.get("Spun_Off_Terms"),
                    "Stock_Terms":         sel.get("MA_Stock_Ratio"),
                    "Cash_Terms":          sel.get("MA_Cash_Terms"),
                    "Cash_Terms_Currency": sel.get("MA_Cash_Terms_Currency"),
                    "Effective_Date":      sel.get("MA_Effective_Date"),
                    "Exp_Completion":      sel.get("MA_Exp_Completion"),
                    "Merger_Status":       sel.get("MA_Merger_Status"),
                    "Election_Deadline":   sel.get("optionelectiondt"),
                    "Unconditional_Date":  sel.get("unconditionaldt"),
                    "Compulsory_Acq_Date": sel.get("compulsoryacqdt"),
                    "Close_Date":          sel.get("MA_Close_Date"),
                })
                st.json({k: v for k, v in detail.items() if v not in (None, "")})
            else:
                st.json({k: v for k, v in {
                    "Event_Type":        sel.get("Event_Type"),
                    "Subtype":           sel.get("Subtype"),
                    "Dividend_Amount":   sel.get("Dividend_Amount"),
                    "Tax_Marker":        sel.get("Tax_Marker"),
                    "Dividend_Currency": sel.get("Dividend_Currency"),
                    "Stock_Div_Pct":     sel.get("Stock_Div_Pct"),
                    "Stock_Div_Ratio":   sel.get("Stock_Div_Ratio"),
                    "Split_Ratio":       sel.get("Split_Ratio"),
                    "Sub_Price":         sel.get("Sub_Price"),
                    "Sub_Currency":      sel.get("Sub_Currency"),
                    "Sub_Ratio":         sel.get("Sub_Ratio"),
                    "Default_Option":    sel.get("Default_Option"),
                    "Election_Deadline": sel.get("optionelectiondt"),
                }.items() if v not in (None, "")})

            # ── Lifecycle ──────────────────────────────────────────────────
            st.markdown("**⏱️ Lifecycle**")
            st.json({k: v for k, v in {
                "Ex_Date":       sel.get("exdt"),
                "Pay_Date":      sel.get("paydt"),
                "Record_Date":   sel.get("recorddt"),
                "Creation_Date": sel.get("Creation_Date"),
                "Feed_Gen_Date": sel.get("feedgendate"),
                "Evt_Action":    sel.get("evtactioncd"),
                "LST_Action":    sel.get("lstactioncd"),
                "NTS_Action":    sel.get("ntsactioncd"),
            }.items() if v not in (None, "")})

        with c2:
            st.markdown("**📄 Raw Fields**")
            st.json({col: sel.get(col, "") for col in RAW_COLUMNS})
            st.markdown("**🔧 Derived Fields**")
            derived_cols = ["Event_Type", "Subtype", "Deal_Type",
                            "Dividend_Amount", "Tax_Marker", "Dividend_Currency",
                            "Stock_Div_Pct", "Stock_Div_Ratio", "Split_Ratio",
                            "Sub_Price", "Sub_Currency", "Sub_Ratio", "Default_Option",
                            "MA_Offeror", "MA_Hostile", "MA_Mand_Vol", "MA_Event_Subtype",
                            "MA_Cash_Terms", "MA_Cash_Terms_Currency",
                            "MA_Stock_Ratio", "Spun_Off_Terms",
                            "MA_Offeror_ISIN", "MA_Offeror_Ticker",
                            "MA_Effective_Date", "MA_Exp_Completion",
                            "MA_Merger_Status", "MA_Close_Date", "Creation_Date"]
            st.json({col: sel.get(col, "") for col in derived_cols})

# ── Export ────────────────────────────────────────────────────────────────────
st.divider()
col_dl1, col_dl2, _ = st.columns([1, 1, 4])
with col_dl1:
    csv = df.drop(columns=["_ignored"], errors="ignore").to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", csv, f"edi_ca_{isin}.csv", "text/csv")
with col_dl2:
    json_out = df.drop(columns=["_ignored"], errors="ignore").to_json(orient="records", indent=2)
    st.download_button("⬇️ Download JSON", json_out, f"edi_ca_{isin}.json", "application/json")
