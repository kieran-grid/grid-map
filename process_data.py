"""
AUS Grid Map - Data Processing Pipeline
Converts AEMO/TNSP data into GeoJSON for the interactive map
"""

import json
import pandas as pd
import re
import os
import warnings
from shapely.geometry import shape, mapping
warnings.filterwarnings('ignore')

BASE = r"C:\Users\kaztr\Downloads\AEMO Data\AEMO Data"
OUT  = r"C:\Users\kaztr\Documents\grid-map\data"

def normalize_line_name(s):
    """Normalize line names for matching: 'X - Y' and 'X to Y' both → 'xy' """
    s = str(s).lower()
    s = re.sub(r'\s*-\s*|\s+to\s+', ' ', s)
    s = re.sub(r'[^a-z0-9 ]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ─────────────────────────────────────────────────────────────
# 1. TRANSMISSION LINES
# ─────────────────────────────────────────────────────────────
print("Processing transmission lines...")

with open(f"{BASE}/Maps/Digital Atlas Data/Electricity_Transmission_Lines.geojson") as f:
    lines_geo = json.load(f)

# Load TAPR line data (ElectraNet SA)
tapr = pd.read_excel(
    f"{BASE}/APR/Transmission-Line-Data-2025.xlsx",
    sheet_name="TransmissionLines", header=0, skiprows=[1, 2]
)
tapr.columns = ["line_name", "line_id", "lat_start", "lon_start", "lat_end", "lon_end", "voltage_kv"]
tapr = tapr.dropna(subset=["line_id"])

# Historic ratings — most recent normal and emergency rating per line
ratings = pd.read_excel(
    f"{BASE}/APR/Transmission-Line-Data-2025.xlsx",
    sheet_name="HistoricRatings", header=0, skiprows=[1, 2]
)
ratings.columns = ["line_name", "line_id", "date_commenced", "rating_mva", "rating_type"]
ratings = ratings.dropna(subset=["line_id", "rating_mva"])
ratings["date_commenced"] = pd.to_datetime(ratings["date_commenced"], errors="coerce")

latest_ratings = {}
for line_id, grp in ratings.groupby("line_id"):
    grp_s = grp.sort_values("date_commenced", ascending=False)
    normal = grp_s[grp_s["rating_type"].astype(str).str.contains("normal|summer|winter", case=False, na=False)]
    emerg  = grp_s[grp_s["rating_type"].astype(str).str.contains("emerg", case=False, na=False)]
    latest_ratings[str(line_id)] = {
        "current_rating_mva":   float(normal.iloc[0]["rating_mva"]) if len(normal) else None,
        "emergency_rating_mva": float(emerg.iloc[0]["rating_mva"])  if len(emerg)  else None,
        "rating_type":          str(normal.iloc[0]["rating_type"])  if len(normal) else None,
    }

# Forecast ratings
forecast = pd.read_excel(
    f"{BASE}/APR/Transmission-Line-Data-2025.xlsx",
    sheet_name="ForecastRatings", header=0, skiprows=[1, 2]
)
forecast.columns = ["line_name", "line_id", "season"] + [f"FY{y}" for y in range(2025, 2035)]
forecast = forecast.dropna(subset=["line_id"])
forecast_lookup = {}
for line_id, grp in forecast.groupby("line_id"):
    forecast_lookup[str(line_id)] = {
        str(row["season"]): {f"FY{y}": (float(row[f"FY{y}"]) if pd.notna(row.get(f"FY{y}")) else None) for y in range(2025, 2035)}
        for _, row in grp.iterrows()
    }

# Projects per line
projects_df = pd.read_excel(
    f"{BASE}/APR/Transmission-Line-Data-2025.xlsx",
    sheet_name="Projects", header=0, skiprows=[1, 2]
)
projects_df = projects_df.iloc[:, :18]
projects_df.columns = [
    "line_name", "line_id", "proj_no", "project", "driver", "investment_desc",
    "cost_year", "capex", "opex", "cost_accuracy", "proposed_timing",
    "demand_reduction", "deferral_value", "economic_cost", "vcr",
    "annual_duration", "peak_duration", "note"
]
projects_df = projects_df.dropna(subset=["line_id", "project"])
projects_lookup = {}
for line_id, grp in projects_df.groupby("line_id"):
    projects_lookup[str(line_id)] = [
        {
            "project": str(row["project"]),
            "timing":  str(row["proposed_timing"]),
            "capex":   str(row["capex"]),
            "driver":  str(row["driver"]),
        }
        for _, row in grp.iterrows()
    ]

# Build name-normalised lookup for matching
tapr_by_norm = {
    normalize_line_name(row["line_name"]): {
        "line_id":   str(row["line_id"]),
        "line_name": str(row["line_name"]),
        "tapr_voltage_kv": str(row["voltage_kv"]),
    }
    for _, row in tapr.iterrows()
}

# Enrich GeoJSON features
enriched_features = []
matched = 0
for feat in lines_geo["features"]:
    props = feat["properties"].copy()
    props["voltage_kv"] = props.pop("capacitykv", None)
    props["status"]     = props.pop("operationalstatus", None)
    props["line_class"] = props.pop("class", None)
    for k in ["objectid", "featuretype", "description", "spatialconfidence", "revised", "ga_guid", "st_lengthshape", "comment_"]:
        props.pop(k, None)

    norm = normalize_line_name(props.get("name", ""))
    match = tapr_by_norm.get(norm)
    if match:
        matched += 1
        line_id = match["line_id"]
        props.update(match)
        props.update(latest_ratings.get(line_id, {}))
        props["forecast_ratings"] = forecast_lookup.get(line_id, {})
        props["projects"]         = projects_lookup.get(line_id, [])

    enriched_features.append({**feat, "properties": props})

print(f"  Matched {matched}/{len(lines_geo['features'])} lines to TAPR ratings")

# Simplify geometries to reduce file size (tolerance ~100m in degrees)
simplified_features = []
for feat in enriched_features:
    try:
        geom = shape(feat["geometry"])
        geom_simple = geom.simplify(0.001, preserve_topology=True)
        simplified_features.append({**feat, "geometry": mapping(geom_simple)})
    except Exception:
        simplified_features.append(feat)

lines_out = {**lines_geo, "features": simplified_features}
with open(f"{OUT}/transmission_lines.geojson", "w") as f:
    json.dump(lines_out, f)
print(f"  Saved {len(simplified_features)} transmission line features")


# ─────────────────────────────────────────────────────────────
# 2. CONNECTION POINTS (SUBSTATIONS)
# ─────────────────────────────────────────────────────────────
print("\nProcessing connection points...")

cp = pd.read_excel(
    f"{BASE}/APR/Connection-Point-Data-2025.xlsx",
    sheet_name="ConnectionPoints", header=0, skiprows=[1, 2]
)
cp.columns = [
    "index", "cp_name", "cp_id", "cp_group",
    "lat", "lon",
    "residential_customers", "industrial_customers",
    "commercial_customers", "voltage_kv"
]
cp = cp.dropna(subset=["lat", "lon", "cp_id"])
cp = cp[pd.to_numeric(cp["lat"], errors="coerce").notna()]
cp["lat"] = cp["lat"].astype(float)
cp["lon"] = cp["lon"].astype(float)

# Fault levels
fault = pd.read_excel(
    f"{BASE}/APR/Connection-Point-Data-2025.xlsx",
    sheet_name="FaultLevels", header=0, skiprows=[1, 2]
)
fault.columns = ["index", "cp_name", "cp_id", "voltage_kv", "fault_3ph_max", "fault_pe_max", "fault_3ph_min", "fault_pe_min"]
fault = fault.dropna(subset=["cp_id"])
fault_lookup = {}
for cp_id, grp in fault.groupby("cp_id"):
    fault_lookup[str(cp_id)] = [
        {
            "voltage_kv":   str(row["voltage_kv"]),
            "fault_3ph_max": float(row["fault_3ph_max"]) if pd.notna(row["fault_3ph_max"]) else None,
            "fault_3ph_min": float(row["fault_3ph_min"]) if pd.notna(row["fault_3ph_min"]) else None,
            "fault_pe_max":  float(row["fault_pe_max"])  if pd.notna(row["fault_pe_max"])  else None,
            "fault_pe_min":  float(row["fault_pe_min"])  if pd.notna(row["fault_pe_min"])  else None,
        }
        for _, row in grp.iterrows()
    ]

# Historic ratings - most recent per CP
cp_ratings = pd.read_excel(
    f"{BASE}/APR/Connection-Point-Data-2025.xlsx",
    sheet_name="HistoricRatings", header=0, skiprows=[1, 2]
)
cp_ratings.columns = ["index", "cp_name", "cp_id", "date_commenced", "rating_mva"]
cp_ratings = cp_ratings.dropna(subset=["cp_id", "rating_mva"])
cp_ratings["date_commenced"] = pd.to_datetime(cp_ratings["date_commenced"], errors="coerce")
cp_rating_lookup = {}
for cp_id, grp in cp_ratings.groupby("cp_id"):
    latest = grp.sort_values("date_commenced", ascending=False).iloc[0]
    cp_rating_lookup[str(cp_id)] = float(latest["rating_mva"])

# Forecast load (50 POE)
f50 = pd.read_excel(
    f"{BASE}/APR/Connection-Point-Data-2025.xlsx",
    sheet_name="Forecast50", header=0, skiprows=[1, 2]
)
f50.columns = ["index", "cp_name", "cp_id"] + [f"FY{y}" for y in range(2025, 2035)]
f50 = f50.dropna(subset=["cp_id"])
f50_lookup = {
    str(row["cp_id"]): {
        f"FY{y}": float(row[f"FY{y}"]) if pd.notna(row.get(f"FY{y}")) else None
        for y in range(2025, 2035)
    }
    for _, row in f50.iterrows()
}

# CP projects
cp_proj = pd.read_excel(
    f"{BASE}/APR/Connection-Point-Data-2025.xlsx",
    sheet_name="Projects", header=0, skiprows=[1, 2]
)
cp_proj = cp_proj.dropna(subset=[cp_proj.columns[2]])  # project name col
cp_proj_lookup = {}
for _, row in cp_proj.iterrows():
    cp_id = str(row.iloc[2])
    if cp_id not in cp_proj_lookup:
        cp_proj_lookup[cp_id] = []
    cp_proj_lookup[cp_id].append({
        "project": str(row.iloc[3]) if pd.notna(row.iloc[3]) else "",
        "driver":  str(row.iloc[4]) if pd.notna(row.iloc[4]) else "",
        "timing":  str(row.iloc[11]) if pd.notna(row.iloc[11]) else "",
    })

# Build GeoJSON
cp_features = []
for _, row in cp.iterrows():
    cp_id = str(row["cp_id"])
    props = {
        "cp_name":              str(row["cp_name"]),
        "cp_id":                cp_id,
        "cp_group":             str(row["cp_group"]) if pd.notna(row["cp_group"]) else None,
        "voltage_kv":           str(row["voltage_kv"]),
        "current_rating_mva":   cp_rating_lookup.get(cp_id),
        "fault_levels":         fault_lookup.get(cp_id, []),
        "forecast_load_50poe":  f50_lookup.get(cp_id, {}),
        "projects":             cp_proj_lookup.get(cp_id, []),
        "residential_customers": int(float(row["residential_customers"])) if pd.notna(row["residential_customers"]) and str(row["residential_customers"]).replace('.','').isdigit() else str(row["residential_customers"]) if pd.notna(row["residential_customers"]) else None,
        "commercial_customers":  int(float(row["commercial_customers"]))  if pd.notna(row["commercial_customers"])  and str(row["commercial_customers"]).replace('.','').isdigit()  else str(row["commercial_customers"])  if pd.notna(row["commercial_customers"])  else None,
        "industrial_customers":  int(float(row["industrial_customers"]))  if pd.notna(row["industrial_customers"])  and str(row["industrial_customers"]).replace('.','').isdigit()  else str(row["industrial_customers"])  if pd.notna(row["industrial_customers"])  else None,
    }
    cp_features.append({
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [float(row["lon"]), float(row["lat"])]},
        "properties": props
    })

cp_geojson = {"type": "FeatureCollection", "features": cp_features}
with open(f"{OUT}/connection_points.geojson", "w") as f:
    json.dump(cp_geojson, f)
print(f"  Saved {len(cp_features)} connection points")


# ─────────────────────────────────────────────────────────────
# 3. MARGINAL LOSS FACTORS
# ─────────────────────────────────────────────────────────────
print("\nProcessing MLFs...")

mlf_file    = f"{BASE}/MLF/Marginal Loss Factors for the 2026-27 Financial Year XLS.xlsx"
regions     = ["QLD", "NSW", "ACT", "VIC", "SA", "TAS"]
mlf_records = []

for region in regions:
    for mlf_type in ["Gen", "Load"]:
        sheet = f"{region} {mlf_type}"
        try:
            df = pd.read_excel(mlf_file, sheet_name=sheet, header=None)
            # Find header row containing "2026-27 MLF"
            header_row = None
            for i, row in df.iterrows():
                if any("2026-27 MLF" in str(v) for v in row.values):
                    header_row = i
                    break
            if header_row is None:
                continue
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:].reset_index(drop=True)
            df = df.dropna(how="all")

            # Normalise column names
            col_map = {}
            for c in df.columns:
                cs = str(c).strip()
                if re.search(r"generator|load\s*point|connection\s*point\s*name", cs, re.I) and "name" not in col_map.values():
                    col_map[c] = "name"
                elif re.search(r"voltage", cs, re.I):
                    col_map[c] = "voltage_kv"
                elif re.search(r"\bduid\b", cs, re.I):
                    col_map[c] = "duid"
                elif re.search(r"connection point id", cs, re.I):
                    col_map[c] = "cp_id"
                elif re.search(r"\btni\b", cs, re.I):
                    col_map[c] = "tni"
                elif re.search(r"2026-27", cs, re.I) and "mlf_2026_27" not in col_map.values():
                    col_map[c] = "mlf_2026_27"
                elif re.search(r"2025-26", cs, re.I) and "mlf_2025_26" not in col_map.values():
                    col_map[c] = "mlf_2025_26"
            df = df.rename(columns=col_map)

            for _, row in df.iterrows():
                mlf_val = pd.to_numeric(row.get("mlf_2026_27"), errors="coerce")
                if pd.isna(mlf_val):
                    continue
                prev_val = pd.to_numeric(row.get("mlf_2025_26"), errors="coerce")
                mlf_records.append({
                    "region":       region,
                    "type":         mlf_type,
                    "name":         str(row.get("name", "")).strip(),
                    "voltage_kv":   str(row.get("voltage_kv", "")).strip(),
                    "duid":         str(row.get("duid", "")).strip(),
                    "cp_id":        str(row.get("cp_id", "")).strip(),
                    "tni":          str(row.get("tni", "")).strip(),
                    "mlf_2026_27":  float(mlf_val),
                    "mlf_2025_26":  float(prev_val) if pd.notna(prev_val) else None,
                    "mlf_change":   round(float(mlf_val) - float(prev_val), 4) if pd.notna(prev_val) else None,
                })
        except Exception as e:
            print(f"  Warning: could not parse {sheet}: {e}")

with open(f"{OUT}/mlf_2026_27.json", "w") as f:
    json.dump(mlf_records, f)
print(f"  Saved {len(mlf_records)} MLF records across {len(regions)} regions")


# ─────────────────────────────────────────────────────────────
# 4. CONNECTION QUEUE (new generators connecting)
# ─────────────────────────────────────────────────────────────
print("\nProcessing connection queue...")

try:
    cq = pd.read_excel(
        f"{BASE}/APR/New-generator-connection-data-as-of-30-Jul-2025.xlsx",
        sheet_name="Data", header=None
    )
    # Data starts at row 4, columns 1-4 (col 0 is rule labels)
    cq = cq.iloc[4:, 1:].reset_index(drop=True)
    cq.columns = ["capacity_mw", "connection_point", "technology", "status"]
    cq = cq.dropna(subset=["connection_point"])

    queue_records = []
    for _, row in cq.iterrows():
        queue_records.append({
            "capacity_mw":      str(row["capacity_mw"]),
            "connection_point": str(row["connection_point"]),
            "technology":       str(row["technology"]),
            "status":           str(row["status"]),
        })

    with open(f"{OUT}/connection_queue.json", "w") as f:
        json.dump(queue_records, f)
    print(f"  Saved {len(queue_records)} queued generator connections")
except Exception as e:
    print(f"  Warning: {e}")


# ─────────────────────────────────────────────────────────────
# 5. SUMMARY
# ─────────────────────────────────────────────────────────────
print("\n=== Data processing complete ===")
for fname in sorted(os.listdir(OUT)):
    fpath = os.path.join(OUT, fname)
    size_kb = os.path.getsize(fpath) / 1024
    print(f"  {fname}: {size_kb:.0f} KB")
