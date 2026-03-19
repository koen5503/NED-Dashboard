"""
NED Energy Dashboard
====================
Single-file Streamlit app that retrieves Dutch renewable energy hourly
capacity-factor data from the ned.nl API, caches it in Excel, validates
quality, and displays interactive Plotly charts.

Usage:
    streamlit run ned_dashboard.py
"""

import os
import time
import datetime

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# ── Constants ──────────────────────────────────────────────────────────
BASE_URL = "https://api.ned.nl/v1"
EXCEL_FILE = "energy_data_ned.xlsx"

# Human-readable label → API type name
# (The NED API uses English names, not Dutch.  The FSD's Dutch names
#  "Zonne-energie", "Wind op land", "Wind op zee" map to the API names below.)
SOURCE_LABELS = {
    "Solar": "Solar",
    "Wind Onshore": "Wind",
    "Wind Offshore": "WindOffshoreC",
    "Biogas": "Biogas",
    "Nuclear": "Nuclear",
    "Fossil Gas": "FossilGasPower",
    "Fossil Coal": "FossilHardCoal",
    "Waste": "WastePower",
    "Biomass": "BiomassPower",
}

# Expected Full-Load-Hour ranges per source (FSD §2)
# Added placeholders for new sources.
FLH_RANGES = {
    "Solar": (800, 1200),
    "Wind Onshore": (1800, 3000),
    "Wind Offshore": (3000, 5000),
    "Biogas": (4000, 8000),
    "Nuclear": (7000, 8760),
    "Fossil Gas": (1000, 6000),
    "Fossil Coal": (1000, 6000),
    "Waste": (4000, 8000),
    "Biomass": (4000, 8000),
}


# ── API helpers ────────────────────────────────────────────────────────
def _headers(api_key: str, accept: str = "application/json") -> dict:
    return {"X-AUTH-TOKEN": api_key, "Accept": accept}


def _extract_items(data) -> list[dict]:
    """
    Extract the list of item dicts from an API response,
    regardless of whether it uses JSON-LD, HAL, or plain JSON format.
    """
    if isinstance(data, list):
        return data

    if not isinstance(data, dict):
        return []

    # JSON-LD (API Platform default): items under "hydra:member"
    if "hydra:member" in data:
        return data["hydra:member"]

    # HAL format: items under "_embedded" → first collection key
    if "_embedded" in data and isinstance(data["_embedded"], dict):
        for key, val in data["_embedded"].items():
            if isinstance(val, list):
                return val

    # Some API Platform setups return a plain list under a generic key
    for key in ("items", "data", "results", "member"):
        if key in data and isinstance(data[key], list):
            return data[key]

    return []


def _item_id(item: dict) -> int | None:
    """Extract numeric ID from an item, trying common key names."""
    for key in ("id", "@id", "typeId", "type_id"):
        val = item.get(key)
        if val is not None:
            # @id may be a URI like "/v1/types/2" — extract trailing int
            if isinstance(val, str):
                parts = val.rstrip("/").split("/")
                try:
                    return int(parts[-1])
                except ValueError:
                    continue
            try:
                return int(val)
            except (ValueError, TypeError):
                continue
    return None


def _item_name(item: dict) -> str:
    """Extract display name from an item, trying common key names."""
    for key in ("name", "description", "label", "title"):
        val = item.get(key)
        if isinstance(val, str) and val:
            return val
    return ""


def get_type_mapping(api_key: str) -> dict[str, int]:
    """
    Call /v1/types and dynamically map human names to type IDs.
    Returns e.g. {"Solar": 2, "Wind Onshore": 1, "Wind Offshore": 51}.

    Tries multiple Accept headers to find one the API responds to,
    then adaptively parses the response structure.
    """
    url = f"{BASE_URL}/types?itemsPerPage=100"

    # Try different content-type negotiations
    accept_types = [
        "application/json",
        "application/ld+json",
        "application/hal+json",
    ]

    data = None
    for accept in accept_types:
        try:
            resp = requests.get(
                url,
                headers={"X-AUTH-TOKEN": api_key, "Accept": accept},
                timeout=30,
            )
        except requests.exceptions.RequestException as exc:
            st.error(f"🌐 Network error fetching types: {exc}")
            st.stop()

        if resp.status_code in (401, 403):
            st.error("🔑 **Authentication failed.** Please check your API key.")
            st.stop()

        if resp.status_code == 200:
            data = resp.json()
            items = _extract_items(data)
            if items:
                break  # found a format that works
    else:
        # None of the accept types yielded items
        if data is not None:
            # Show raw response for debugging
            import json as _json
            raw = _json.dumps(data, indent=2, default=str)[:2000]
            st.error(f"Could not parse `/v1/types` response. Raw (truncated):\n```\n{raw}\n```")
        else:
            st.error("All requests to `/v1/types` failed.")
        return {}

    # Build reverse lookup: API name → id
    api_name_to_id: dict[str, int] = {}
    for t in items:
        name = _item_name(t)
        tid = _item_id(t)
        if tid is not None and name:
            api_name_to_id[name] = tid

    if not api_name_to_id:
        import json as _json
        sample = _json.dumps(items[:3], indent=2, default=str)[:1500]
        st.error(f"Parsed {len(items)} items but couldn't extract name/id. Sample:\n```\n{sample}\n```")
        return {}

    # Map our labels to API names
    mapping: dict[str, int] = {}
    for label, api_name in SOURCE_LABELS.items():
        if api_name in api_name_to_id:
            mapping[label] = api_name_to_id[api_name]
        else:
            # Try case-insensitive / substring matching as fallback
            for aname, aid in api_name_to_id.items():
                if aname.lower() == api_name.lower():
                    mapping[label] = aid
                    break
            else:
                st.warning(
                    f"⚠️ Could not find type '{api_name}' in API. "
                    f"Available: {list(api_name_to_id.keys())}"
                )

    return mapping


def fetch_year_data(api_key: str, type_id: int, year: int, start_date: str | None = None) -> pd.DataFrame:
    """
    Fetch hourly data for one source/year.
    Extracts percentage (0-1) and volume (converted to MW).
    Returns a pandas DataFrame indexed by UTC timestamp.
    """
    if start_date is None:
        start_date = f"{year}-01-01"

    initial_params = {
        "point": 0,
        "type": type_id,
        "granularity": 5,            # Hour
        "granularitytimezone": 0,     # UTC
        "classification": 2,         # Current (actual)
        "activity": 1,               # Providing
        "validfrom[after]": start_date,
        "validfrom[strictly_before]": f"{year + 1}-01-01",
        "itemsPerPage": 500,
    }

    timestamps: list[str] = []
    percentages: list[float] = []
    volumes_mw: list[float] = []
    page = 0
    progress = st.empty()

    # First request: use params.  Subsequent: follow absolute next URL.
    next_url: str | None = None
    use_params = True

    while True:
        if page > 0:
            time.sleep(0.5)  # slightly faster rate limiting

        progress.text(f"Page {page + 1} — {len(percentages)} records so far...")

        try:
            if use_params:
                resp = requests.get(
                    f"{BASE_URL}/utilizations",
                    params=initial_params,
                    headers=_headers(api_key, accept="application/ld+json"),
                    timeout=60,
                )
                use_params = False   # subsequent pages use the next URL directly
            else:
                resp = requests.get(
                    next_url,
                    headers=_headers(api_key, accept="application/ld+json"),
                    timeout=60,
                )
        except requests.exceptions.RequestException as exc:
            st.error(f"🌐 Network error during fetch: {exc}")
            st.stop()

        if resp.status_code in (401, 403):
            st.error(
                "🔑 **Authentication failed** or parameters not allowed by your "
                f"subscription.\n\n`{resp.text[:300]}`"
            )
            st.stop()

        if resp.status_code != 200:
            st.error(f"API returned status {resp.status_code}: {resp.text[:300]}")
            st.stop()

        data = resp.json()

        # Extract items — JSON-LD puts them in hydra:member
        items = _extract_items(data)

        for item in items:
            ts = item.get("validfrom", "")
            pct = item.get("percentage", 0.0)
            vol_kwh = item.get("volume", 0.0)
            if ts:
                timestamps.append(ts)
                percentages.append(float(pct))
                # volume in kWh for 1 hour = mean power in kW. divide by 1000 for MW.
                volumes_mw.append(float(vol_kwh) / 1000.0)

        # Follow pagination — JSON-LD: hydra:view → hydra:next
        next_url = None
        if isinstance(data, dict):
            view = data.get("hydra:view", {})
            if isinstance(view, dict) and "hydra:next" in view:
                next_url = view["hydra:next"]
                # Make relative URLs absolute
                if next_url.startswith("/"):
                    next_url = f"https://api.ned.nl{next_url}"

            # Also try HAL _links.next as fallback
            if not next_url:
                links = data.get("_links", {})
                nxt = links.get("next", {})
                if isinstance(nxt, dict) and "href" in nxt:
                    next_url = nxt["href"]
                    if next_url.startswith("/"):
                        next_url = f"https://api.ned.nl{next_url}"

        if not next_url:
            break   # no more pages

        page += 1
    progress.empty()

    if not timestamps:
        st.warning(f"No data returned for type {type_id}, year {year}.")
        return pd.DataFrame()

    df = pd.DataFrame(
        {"pct": percentages, "mw": volumes_mw},
        index=pd.to_datetime(timestamps, utc=True)
    )
    return df.sort_index()


# ── Excel caching ──────────────────────────────────────────────────────
def sheet_name(year: int) -> str:
    return f"Y{year}"


def load_existing_years(path: str) -> dict[int, pd.DataFrame]:
    """Load all previously cached year sheets from the Excel file."""
    result: dict[int, pd.DataFrame] = {}
    if not os.path.exists(path):
        return result
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        for sn in xls.sheet_names:
            if sn.startswith("Y") and sn[1:].isdigit():
                yr = int(sn[1:])
                df = pd.read_excel(xls, sheet_name=sn, index_col=0, engine="openpyxl")
                # Restore UTC — Excel stores tz-naive datetimes
                df.index = pd.to_datetime(df.index, utc=True)
                result[yr] = df
    except Exception as exc:
        st.warning(f"Could not read existing Excel file: {exc}")
    return result


def save_year(path: str, year: int, df: pd.DataFrame):
    """Append or create a sheet for the given year in the Excel file."""
    sn = sheet_name(year)
    # Excel does not support tz-aware datetimes — strip UTC before writing.
    # We re-add UTC on load in load_existing_years().
    df_out = df.copy()
    if hasattr(df_out.index, "tz") and df_out.index.tz is not None:
        df_out.index = df_out.index.tz_localize(None)
    if os.path.exists(path):
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_out.to_excel(writer, sheet_name=sn)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            df_out.to_excel(writer, sheet_name=sn)


GAS_SHEET = "Gas_TTF"


def fetch_gas_prices(start_date: str | None = None) -> pd.DataFrame:
    """
    Fetch daily Dutch TTF Natural Gas Futures close prices (EUR/MWh)
    from Yahoo Finance via yfinance.  Returns a DataFrame with date index.
    """
    if not HAS_YFINANCE:
        st.error(
            "⚠️ `yfinance` is not installed.  "
            "Run `pip install yfinance` and restart the app."
        )
        return pd.DataFrame()

    ticker = yf.Ticker("TTF=F")
    if start_date:
        hist = ticker.history(start=start_date, interval="1d")
    else:
        hist = ticker.history(period="max", interval="1d")

    if hist.empty:
        return pd.DataFrame()

    df = hist[["Close"]].rename(columns={"Close": "TTF_EUR_MWh"})
    df.index = df.index.tz_localize(None)  # strip tz for Excel compat
    df.index.name = "date"
    return df


def load_gas_prices(path: str) -> pd.DataFrame:
    """Load cached gas price data from the Excel file."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        if GAS_SHEET in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=GAS_SHEET, index_col=0, engine="openpyxl")
            df.index = pd.to_datetime(df.index)
            return df
    except Exception as exc:
        st.warning(f"Could not read gas price sheet: {exc}")
    return pd.DataFrame()


def save_gas_prices(path: str, df: pd.DataFrame):
    """Save gas price data to the Gas_TTF sheet in the Excel file."""
    if os.path.exists(path):
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            df.to_excel(w, sheet_name=GAS_SHEET)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
            df.to_excel(w, sheet_name=GAS_SHEET)


# ── Data quality verification ─────────────────────────────────────────
def verify_data(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Run three verification tests on a year DataFrame.
    Returns a styled report DataFrame.
    """
    rows = len(df)
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    expected_max = 8784 if is_leap else 8760

    report_rows = []

    # ── Test 1: Completeness ──
    if rows < 8700:
        comp_status = "❌ Incomplete"
    elif 8700 <= rows <= 8784:
        comp_status = "✅ Pass"
    else:
        comp_status = "⚠️ Extra rows"

    report_rows.append({
        "Test": "Completeness",
        "Details": f"{rows} rows (expected {expected_max})",
        **{label: comp_status for label in SOURCE_LABELS.keys()}
    })

    # ── Test 2 & 3 per column ──
    flh_row = {"Test": "Full Load Hours", "Details": "Sum of %"}
    physics_row = {"Test": "Physics Check", "Details": "Values in [0, 1.05]"}

    for label in SOURCE_LABELS.keys():
        col = f"{label} (%)"
        if col not in df.columns:
            flh_row[label] = "❌ Missing"
            physics_row[label] = "❌ Missing"
            continue

        series = df[col].astype(float)
        flh = series.sum()
        
        # Check if we have a range for this source
        lo, hi = FLH_RANGES.get(label, (0, 8760))

        if lo <= flh <= hi:
            flh_row[label] = f"✅ {flh:.0f} h"
        else:
            flh_row[label] = f"⚠️ {flh:.0f} h (exp {lo}–{hi})"

        neg = (series < 0).sum()
        over = (series > 1.05).sum()
        if neg == 0 and over == 0:
            physics_row[label] = "✅ Pass"
        else:
            physics_row[label] = f"❌ {neg} neg, {over} >1.05"

    report_rows.append(flh_row)
    report_rows.append(physics_row)

    return pd.DataFrame(report_rows).set_index("Test")


def clamp_physics(df: pd.DataFrame) -> pd.DataFrame:
    """Clamp capacity factor values: negatives → 0, >1.0 → 1.0."""
    for label in SOURCE_LABELS.keys():
        col = f"{label} (%)"
        if col in df.columns:
            df[col] = df[col].clip(lower=0.0, upper=1.0)
    return df


# ── Main application ──────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="NED Energy Dashboard", layout="wide")
    st.title("🇳🇱 NED Energy Dashboard")
    st.caption("Dutch Renewable Energy — Historical Capacity Factor Viewer")

    # ── Sidebar ──
    with st.sidebar:
        st.header("🔧 Configuration")
        api_key = st.text_input("NED API Key", type="password", help="Get your key at ned.nl → My Account → API")
        st.divider()

        current_year = datetime.datetime.now().year
        col1, col2 = st.columns(2)
        with col1:
            start_year = st.number_input("Start Year", min_value=2015, max_value=current_year,
                                         value=current_year, step=1)
        with col2:
            end_year = st.number_input("End Year", min_value=2015, max_value=current_year,
                                       value=current_year, step=1)
        if start_year > end_year:
            st.error("Start year must be ≤ end year.")
            st.stop()

        fetch_btn = st.button("📥 Fetch Data", type="primary", use_container_width=True)

    # ── Load existing data ──
    all_data = load_existing_years(EXCEL_FILE)

    # ── Fetch new data ──
    if fetch_btn:
        if not api_key:
            st.error("Please enter your NED API key in the sidebar.")
            st.stop()

        years = list(range(int(start_year), int(end_year) + 1))

        # Get dynamic type mapping
        with st.spinner("Resolving energy types from NED API..."):
            type_map = get_type_mapping(api_key)

        if not type_map:
            st.error("Could not resolve any energy type IDs. Check API key and try again.")
            st.stop()

        st.info(f"Resolved types: {type_map}")

        for yr in years:
            if yr in all_data and yr < current_year:
                st.info(f"📋 Year {yr} already cached — skipping download.")
                continue

            # For current year, fetch incrementally starting from latest timestamp
            start_date_param = None
            if yr in all_data and yr == current_year:
                last_dt = all_data[yr].index.max()
                if pd.notnull(last_dt):
                    # API uses > validation for [after], so requesting the same day is fine.
                    # Duplicates are dropped later. We request from the date of the last timestamp.
                    start_date_param = last_dt.strftime("%Y-%m-%d")
                    st.info(f"🔄 Current year {yr} partially cached. Fetching updates from {start_date_param}...")
            else:
                st.subheader(f"Fetching {yr}...")

            year_frames_list: list[pd.DataFrame] = []

            for label, tid in type_map.items():
                with st.spinner(f"  ↳ {label} ({yr})..."):
                    df_source = fetch_year_data(api_key, tid, yr, start_date=start_date_param)
                    if not df_source.empty:
                        # Rename columns to Source (%) and Source (MW)
                        df_source = df_source.rename(columns={
                            'pct': f"{label} (%)", 
                            'mw': f"{label} (MW)"
                        })
                        year_frames_list.append(df_source)

            if year_frames_list:
                df_new = pd.concat(year_frames_list, axis=1)
                df_new.index.name = "timestamp_utc"

                # If updating the current year, merge with existing
                if yr in all_data and yr == current_year:
                    # Merge on index, prioritize new data
                    df_year = pd.concat([all_data[yr], df_new])
                    df_year = df_year[~df_year.index.duplicated(keep="last")].sort_index()
                else:
                    df_year = df_new

                save_year(EXCEL_FILE, yr, df_year)
                all_data[yr] = df_year
                if start_date_param:
                    st.success(f"✅ {yr}: appended {len(df_new)} periods, total {len(df_year)} rows saved.")
                else:
                    st.success(f"✅ {yr}: saved {len(df_year)} rows to {EXCEL_FILE}")
            else:
                if yr in all_data and yr == current_year:
                    st.info(f"✨ Year {yr} is fully up to date.")
                else:
                    st.warning(f"⚠️ No data retrieved for {yr}.")

    # ── Gas prices ──
    gas_df = load_gas_prices(EXCEL_FILE)

    if fetch_btn or st.sidebar.button("⛽ Fetch Gas Prices", use_container_width=True):
        with st.spinner("Fetching TTF gas prices from Yahoo Finance..."):
            start = None
            if not gas_df.empty:
                start = (gas_df.index.max() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                st.info(f"🔄 Updating gas prices from {start}...")
            new_gas = fetch_gas_prices(start_date=start)
            if not new_gas.empty:
                if not gas_df.empty:
                    gas_df = pd.concat([gas_df, new_gas])
                    gas_df = gas_df[~gas_df.index.duplicated(keep="last")].sort_index()
                else:
                    gas_df = new_gas
                save_gas_prices(EXCEL_FILE, gas_df)
                st.success(f"✅ Gas prices: {len(gas_df)} days cached.")
            elif gas_df.empty:
                st.warning("No gas price data retrieved.")

    # ── Nothing loaded? ──
    if not all_data and gas_df.empty:
        st.info("No data loaded yet. Enter your API key and click **Fetch Data** to begin.")
        st.stop()

    # ── Verification ──
    if all_data:
        st.header("📊 Data Quality Report")
        for yr in sorted(all_data.keys()):
            df = all_data[yr]
            report = verify_data(df, yr)
            st.subheader(f"Year {yr}")
            st.dataframe(report, use_container_width=True)
            # Apply physics clamping
            all_data[yr] = clamp_physics(df)

        # ── Combine all years ──
        combined = pd.concat(all_data.values()).sort_index()
        combined = combined[~combined.index.duplicated(keep="first")]

        # ── Dashboard controls ──
        st.header("📈 Dashboard")

        # Date range slider
        min_dt = combined.index.min().to_pydatetime()
        max_dt = combined.index.max().to_pydatetime()

        # Default view: Jan 1–14 of the first year
        first_year = min(all_data.keys())
        default_start = datetime.datetime(first_year, 1, 1, tzinfo=datetime.timezone.utc)
        default_end = datetime.datetime(first_year, 1, 14, 23, 0, tzinfo=datetime.timezone.utc)
        default_start = max(default_start, min_dt)
        default_end = min(default_end, max_dt)

        date_range = st.slider(
            "Select date range",
            min_value=min_dt,
            max_value=max_dt,
            value=(default_start, default_end),
            format="YYYY-MM-DD HH:mm",
        )

        mask = (combined.index >= pd.Timestamp(date_range[0])) & (combined.index <= pd.Timestamp(date_range[1]))
        df_view = combined.loc[mask]

        if df_view.empty:
            st.warning("No data in the selected range.")
            st.stop()

        # View options
        ui_col1, ui_col2 = st.columns(2)
        with ui_col1:
            data_type = st.radio("Data Type", ["Capacity Factor (%)", "Production (MW)"], horizontal=True)
        with ui_col2:
            view_mode = st.radio("View Mode", ["Individual Profiles", "Stacked Generation"], horizontal=True)

        # Source Selection
        all_labels = list(SOURCE_LABELS.keys())
        default_sources = ["Solar", "Wind Onshore", "Wind Offshore"]
        available_sources = [label for label in all_labels if f"{label} (%)" in df_view.columns]
        
        selected_sources = st.multiselect(
            "Select Energy Sources",
            options=available_sources,
            default=[s for s in default_sources if s in available_sources]
        )

        if not selected_sources:
            st.info("Select at least one energy source to display.")
            st.stop()

        suffix = " (%)" if data_type == "Capacity Factor (%)" else " (MW)"
        colors = {
            "Solar": "#FFB300", "Wind Onshore": "#43A047", "Wind Offshore": "#1E88E5",
            "Biogas": "#8D6E63", "Nuclear": "#9C27B0", "Fossil Gas": "#757575",
            "Fossil Coal": "#212121", "Waste": "#795548", "Biomass": "#66BB6A"
        }

        # ── Graph 1: Individual Profiles ──
        if view_mode == "Individual Profiles":
            fig = go.Figure()
            for label in selected_sources:
                col = label + suffix
                if col in df_view.columns:
                    fig.add_trace(go.Scatter(
                        x=df_view.index,
                        y=df_view[col],
                        mode="lines",
                        name=label,
                        line=dict(color=colors.get(label), width=1.5),
                    ))

            fig.update_layout(
                title=f"{data_type} over Time",
                yaxis=dict(title=data_type),
                xaxis=dict(title="Time (UTC)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=500,
                template="plotly_white",
            )
            if data_type == "Capacity Factor (%)":
                fig.update_layout(yaxis=dict(range=[0, 1.05]))
            
            st.plotly_chart(fig, use_container_width=True)

        # ── Graph 2: Stacked Generation ──
        else:
            if data_type == "Capacity Factor (%)":
                st.subheader("Simulated Generation (Installed Capacity Assumptions)")
                st.caption("Enter hypothetical installed capacities (GW) to simulate power output.")
                cap_cols = st.columns(len(selected_sources))
                caps = {}
                for i, label in enumerate(selected_sources):
                    with cap_cols[i]:
                        caps[label] = st.number_input(f"{label} (GW)", min_value=0.0, value=10.0, step=1.0)
                
                # Compute P(t) = C × CF(t)
                plot_data = {
                    label: df_view[f"{label} (%)"] * caps[label]
                    for label in selected_sources
                }
                title = "Simulated Renewable Power Output"
                y_title = "Power Output (GW)"
            else:
                st.subheader("Actual Hourly Generation Stack")
                plot_data = {
                    label: df_view[f"{label} (MW)"]
                    for label in selected_sources
                }
                title = "Actual Power Generation (from NED API)"
                y_title = "Power Output (MW)"

            fig = go.Figure()
            for label in selected_sources:
                fig.add_trace(go.Scatter(
                    x=df_view.index, y=plot_data[label],
                    mode="lines", name=label,
                    line=dict(width=0), 
                    fillcolor=colors.get(label, "grey"),
                    stackgroup="one",
                ))

            fig.update_layout(
                title=title,
                yaxis=dict(title=y_title),
                xaxis=dict(title="Time (UTC)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=550,
                template="plotly_white",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Summary stats for the selected range
            total = pd.DataFrame(plot_data).sum(axis=1)
            unit = "GW" if data_type == "Capacity Factor (%)" else "MW"
            total_unit = "TWh" if data_type == "Capacity Factor (%)" else "GWh"
            divisor = 1000.0
            
            st.markdown(f"""
            **Selected range statistics ({data_type}):**
            - Peak combined output: **{total.max():.1f} {unit}**
            - Average combined output: **{total.mean():.1f} {unit}**
            - Minimum combined output: **{total.min():.1f} {unit}**
            - Total energy: **{total.sum() / divisor:.1f} {total_unit}** (assuming hourly data)
            """)

    # ── Gas Price Chart ──
    if not gas_df.empty:
        st.header("⛽ TTF Natural Gas — Day Ahead Price")

        if all_data:
            gas_mask = (
                (gas_df.index >= pd.Timestamp(date_range[0]).tz_localize(None))
                & (gas_df.index <= pd.Timestamp(date_range[1]).tz_localize(None))
            )
            gas_view = gas_df.loc[gas_mask]
        else:
            gas_view = pd.DataFrame()

        if gas_view.empty:
            # Show full gas range if the energy date slider doesn't overlap
            gas_view = gas_df
            st.caption("Showing full available gas price range.")

        fig_gas = go.Figure()
        fig_gas.add_trace(go.Scatter(
            x=gas_view.index,
            y=gas_view["TTF_EUR_MWh"],
            mode="lines",
            name="TTF Day Ahead",
            line=dict(color="#FF6F00", width=2),
        ))
        fig_gas.update_layout(
            title="Dutch TTF Natural Gas Price",
            yaxis=dict(title="Price (EUR/MWh)"),
            xaxis=dict(title="Date"),
            height=400,
            template="plotly_white",
        )
        st.plotly_chart(fig_gas, use_container_width=True)

        st.markdown(f"""
        **Gas price statistics ({gas_view.index.min().date()} — {gas_view.index.max().date()}):**
        - Current: **€{gas_view['TTF_EUR_MWh'].iloc[-1]:.2f}/MWh**
        - Average: **€{gas_view['TTF_EUR_MWh'].mean():.2f}/MWh**
        - Min: **€{gas_view['TTF_EUR_MWh'].min():.2f}/MWh**
        - Max: **€{gas_view['TTF_EUR_MWh'].max():.2f}/MWh**
        """)


if __name__ == "__main__":
    main()
