# 🇳🇱 NED Energy Dashboard

Interactive Streamlit dashboard for Dutch renewable energy capacity factor data, powered by the [Nationaal Energie Dashboard](https://ned.nl) API.

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Streamlit](https://img.shields.io/badge/streamlit-1.30%2B-red)
![License](https://img.shields.io/badge/license-MIT-green)

## Features

- **Dynamic API integration** — fetches hourly capacity factor data from `api.ned.nl` for Solar, Wind Onshore, and Wind Offshore
- **Smart caching & Incremental Updates** — stores data per year in `energy_data_ned.xlsx`. Completely skips already-downloaded historical years, and incrementally fetches only missing new records for the current ongoing year.
- **Autonomous data verification** — completeness, Full Load Hours, and physics checks with color-coded reports
- **Individual Profiles** — line chart of capacity factors (0–1) with per-source toggles
- **Stacked Simulation** — configurable installed capacity (GW) to simulate combined renewable power output
- **HAL/JSON-LD pagination** — handles the NED API's paginated responses transparently
- **TTF gas prices** — fetches Dutch TTF Natural Gas day-ahead prices from Yahoo Finance (`yfinance`), cached with incremental updates

## Quick Start

```bash
# Install dependencies
pip install streamlit pandas requests openpyxl plotly yfinance

# Run the dashboard
streamlit run ned_dashboard.py
```

1. Get an API key at [ned.nl](https://ned.nl) → My Account → API
2. Paste it into the sidebar
3. Select a year range and click **Fetch Data**

## Screenshots

After fetching, the dashboard displays:

1. **Data Quality Report** — per-year verification table
2. **Individual Profiles** — zoomable line chart of capacity factors
3. **Stacked Simulation** — area chart of simulated GW output with adjustable installed capacities

## API Notes

| Parameter | Value | Meaning |
|-----------|-------|---------|
| `point` | `0` | Nederland |
| `granularity` | `5` | Hour |
| `granularitytimezone` | `0` | UTC |
| `classification` | `2` | Current (actual) |
| `activity` | `1` | Providing |
| `type` | `2` / `1` / `51` | Solar / Wind / WindOffshoreC |

The type IDs are resolved dynamically via `/v1/types` — they are not hardcoded.

## Project Structure

```
NED/
├── ned_dashboard.py          # Single-file Streamlit app
├── energy_data_ned.xlsx      # Auto-generated data cache (per-year sheets)
├── zon-2025-uur-data.csv     # Reference: Solar hourly data 2025
├── wind-2025-uur-data.csv    # Reference: Wind Onshore hourly data 2025
├── zeewind-2025-uur-data.csv # Reference: Wind Offshore hourly data 2025
└── FSD.txt                   # Functional Specification Document
```

## Reference Data (2025)

| Source | Rows | FLH | Max CF | Type (API) |
|--------|------|-----|--------|------------|
| Solar | 8760 | 1131 h | 0.837 | `Solar` |
| Wind Onshore | 8760 | 2615 h | 1.000 | `Wind` |
| Wind Offshore | 8760 | 3453 h | 0.998 | `WindOffshoreC` |

---

## Functional Specification Document

### Context

Simulation for the Dutch power grid using historical data from the Nationaal Energie Dashboard (ned.nl). The 2025 data has been manually verified, so expected profile characteristics are known.

### 1. Data Retrieval (ned.nl API)

**Inputs:**

- **API Key**: Streamlit `text_input` (type=`"password"`)
- **Year Range**: Two inputs for Start/End Year

**Dynamic Configuration:**

- Call `/v1/types` first
- Dynamically map names to IDs (do not hardcode, as IDs may change):
  - `"Zonne-energie"` → Solar
  - `"Wind op land"` → Wind Onshore
  - `"Wind op zee"` → Wind Offshore

**Fetch Logic** (Endpoint: `/v1/utilizations`):

- Parameters: `point=0` (Netherlands), `granularity=5` (Hour), `classification=2` (Actual), `granularitytimezone=0` (UTC)
- Metric: Retrieve the `percentage` column. If `max(value) > 1.5`, divide entire series by 100 to normalize to 0–1
- Pagination: The NED API uses JSON-LD/HAL pagination. Recursively follow the next link until no next link exists

**Storage Strategy:**

- File: `energy_data_ned.xlsx`
- Check if file exists → load with `pd.ExcelFile`
- **Past Years**: Check if the specific year already exists as a sheet → SKIP downloading completely (prevents API waste).
- **Current Year**: Check if the year exists. If yes, dynamically find the latest cached timestamp and incrementally fetch only new data from that point forward, appending to the cache.

### 2. Autonomous Self-Verification

Immediately after downloading or loading a year's data, run `verify_data(df)`:

| Test | Condition | Action |
|------|-----------|--------|
| **Completeness** | 8760 ≤ rows ≤ 8784 | Flag `< 8700` as "Incomplete" |
| **Full Load Hours** | `sum(CF)` per column | Solar: 800–1200, Onshore: 1800–3000, Offshore: 3000–5000 |
| **Physics Check** | All values in [0, 1.05] | Clamp values > 1.0 to 1.0 |

Output: Color-coded Data Quality Report displayed in Streamlit before graphs.

### 3. Interactive Dashboard

**Global Controls:**

- **Date Slider**: Double-ended slider covering the full available range (default: Jan 1–14 of first loaded year)
- **View Mode**: Radio button — "Individual Profiles" vs "Stacked Simulation"

**Individual Profiles Mode:**

- Checkboxes for Solar, Wind Onshore, Wind Offshore
- Line chart of Capacity Factors (0–1)

**Stacked Simulation Mode:**

- Three number inputs for Installed Capacity (GW) per source (default: 10 each)
- Calculation: `P_total(t) = C_solar × CF_solar(t) + C_onshore × CF_onshore(t) + C_offshore × CF_offshore(t)`
- Stacked Area Chart of resulting GW output

### Technical Constraints

- `pandas` for all data handling
- `plotly.graph_objects` for charts
- `time.sleep(1)` between pagination calls (rate limiting)
- 401/403 → clear "Please check your credentials" message, no stack trace

## AI Attribution

This code was made fully with **Claude Opus 4.6 (thinking)** without user intervention other than noting the first time the dashboard did not work. Following that feedback, the AI autonomously debugged the live API, identified three root causes (wrong type names, missing parameters, and incorrect date formats), and successfully implemented the final working version.

## License

MIT
