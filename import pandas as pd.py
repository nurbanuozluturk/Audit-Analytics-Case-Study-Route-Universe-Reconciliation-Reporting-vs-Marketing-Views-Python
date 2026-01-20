import pandas as pd
import numpy as np
import os

# === Base folder (your path) ===
BASE_DIR = r"C:\Users\nurba\Desktop\Portfolyo"

REPORTING_PATH = os.path.join(BASE_DIR, "T_ONTIME_REPORTING.csv")
MARKETING_PATH = os.path.join(BASE_DIR, "T_ONTIME_MARKETING.csv")

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Read files ===
rep = pd.read_csv(REPORTING_PATH)
mkt = pd.read_csv(MARKETING_PATH)

KEY = ["ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID"]

# --- A) Basic profiling ---
summary = {
    "reporting_rows": len(rep),
    "marketing_rows": len(mkt),
    "reporting_unique_routes": rep[KEY].drop_duplicates().shape[0],
    "marketing_unique_routes": mkt[KEY].drop_duplicates().shape[0],
    "reporting_unique_origin_airports": rep["ORIGIN_AIRPORT_ID"].nunique(),
    "marketing_unique_origin_airports": mkt["ORIGIN_AIRPORT_ID"].nunique(),
    "reporting_unique_dest_airports": rep["DEST_AIRPORT_ID"].nunique(),
    "marketing_unique_dest_airports": mkt["DEST_AIRPORT_ID"].nunique(),
    "reporting_null_cells": int(rep.isna().sum().sum()),
    "marketing_null_cells": int(mkt.isna().sum().sum()),
}
pd.DataFrame([summary]).to_csv(os.path.join(OUTPUT_DIR, "audit_kpi_summary.csv"), index=False)

# --- B) Route universe reconciliation ---
routes_rep = rep[KEY].drop_duplicates()
routes_mkt = mkt[KEY].drop_duplicates()

marketing_only_routes = routes_mkt.merge(routes_rep, on=KEY, how="left", indicator=True)
marketing_only_routes = marketing_only_routes[marketing_only_routes["_merge"] == "left_only"].drop(columns=["_merge"])
marketing_only_routes.to_csv(os.path.join(OUTPUT_DIR, "exceptions_routes_marketing_only.csv"), index=False)

# --- C) Airport universe gaps ---
airports_rep = pd.unique(pd.concat([routes_rep["ORIGIN_AIRPORT_ID"], routes_rep["DEST_AIRPORT_ID"]]))
airports_mkt = pd.unique(pd.concat([routes_mkt["ORIGIN_AIRPORT_ID"], routes_mkt["DEST_AIRPORT_ID"]]))

airports_rep = set(airports_rep.tolist())
airports_mkt = set(airports_mkt.tolist())

airports_only_in_marketing = sorted(list(airports_mkt - airports_rep))
pd.DataFrame({"AIRPORT_ID_only_in_marketing": airports_only_in_marketing}).to_csv(
    os.path.join(OUTPUT_DIR, "exceptions_airports_marketing_only.csv"),
    index=False
)

# --- D) Overcounting / population definition risk ---
rep_counts = rep.groupby(KEY).size().reset_index(name="reporting_count")
mkt_counts = mkt.groupby(KEY).size().reset_index(name="marketing_count")

counts = rep_counts.merge(mkt_counts, on=KEY, how="outer").fillna(0)
counts["reporting_count"] = counts["reporting_count"].astype(int)
counts["marketing_count"] = counts["marketing_count"].astype(int)

# ratio: only meaningful when reporting_count > 0
counts["ratio_mkt_vs_rep"] = np.where(
    counts["reporting_count"] > 0,
    counts["marketing_count"] / counts["reporting_count"],
    np.nan,
)

# flag rules (tunable):
# - high ratio and enough volume to avoid noise
flags = counts[
    (counts["reporting_count"] >= 20) &
    (counts["ratio_mkt_vs_rep"] >= 3.0)
].sort_values("ratio_mkt_vs_rep", ascending=False)

flags.to_csv(os.path.join(OUTPUT_DIR, "exceptions_overcounting_flags.csv"), index=False)

print(f"Audit outputs created under: {OUTPUT_DIR}")


# =========================
# E) Auto-generate "Audit Findings" text for your 1-page report
# =========================
def fmt_int(x: int) -> str:
    return f"{x:,}"

def fmt_pct(x: float) -> str:
    return f"{x:.1%}"

# Unique universes
n_routes_rep = routes_rep.shape[0]
n_routes_mkt = routes_mkt.shape[0]

# Intersection / coverage
routes_intersection = routes_mkt.merge(routes_rep, on=KEY, how="inner").shape[0]
coverage_pct = (routes_intersection / n_routes_mkt) if n_routes_mkt else 0

# Exceptions
n_mkt_only_routes = marketing_only_routes.shape[0]
pct_mkt_only_routes = (n_mkt_only_routes / n_routes_mkt) if n_routes_mkt else 0

n_airports_only_mkt = len(airports_only_in_marketing)

n_flags = flags.shape[0]
max_ratio = float(flags["ratio_mkt_vs_rep"].max()) if n_flags else np.nan
median_ratio = float(flags["ratio_mkt_vs_rep"].median()) if n_flags else np.nan

# Top examples for narrative (safe sample)
top5_routes_mkt_only = marketing_only_routes.head(5)
top5_flags = flags.head(5)[KEY + ["reporting_count", "marketing_count", "ratio_mkt_vs_rep"]] if n_flags else pd.DataFrame()

findings_lines = []
findings_lines.append("AUDIT FINDINGS – NUMERIC SUMMARY (AUTO-GENERATED)")
findings_lines.append("------------------------------------------------")
findings_lines.append(f"- Reporting route universe (unique ORIGIN-DEST pairs): {fmt_int(n_routes_rep)}")
findings_lines.append(f"- Marketing route universe (unique ORIGIN-DEST pairs): {fmt_int(n_routes_mkt)}")
findings_lines.append(
    f"- Coverage: Reporting includes {fmt_int(routes_intersection)} of {fmt_int(n_routes_mkt)} marketing routes "
    f"({fmt_pct(coverage_pct)} coverage)."
)
findings_lines.append(
    f"- Exceptions (Marketing-only routes): {fmt_int(n_mkt_only_routes)} "
    f"({fmt_pct(pct_mkt_only_routes)} of marketing route universe)."
)
findings_lines.append(
    f"- Exceptions (Airports only present in Marketing view): {fmt_int(n_airports_only_mkt)} airport IDs."
)

# Overcounting flags
if n_flags:
    findings_lines.append(
        f"- Potential population definition/overcounting flags: {fmt_int(n_flags)} routes "
        f"(criteria: reporting_count >= 20 and marketing/reporting ratio >= 3.0)."
    )
    findings_lines.append(f"- Flagged routes ratio stats: median={median_ratio:.2f}, max={max_ratio:.2f}")
else:
    findings_lines.append("- Potential population definition/overcounting flags: 0 routes matched the defined criteria.")

# Add small samples (optional)
findings_lines.append("")
findings_lines.append("SAMPLE – Marketing-only routes (first 5 unique ORIGIN-DEST pairs):")
if not top5_routes_mkt_only.empty:
    for _, r in top5_routes_mkt_only.iterrows():
        findings_lines.append(f"  - ORIGIN={int(r['ORIGIN_AIRPORT_ID'])}, DEST={int(r['DEST_AIRPORT_ID'])}")
else:
    findings_lines.append("  - None")

findings_lines.append("")
findings_lines.append("SAMPLE – Top 5 overcounting flags (if any):")
if not top5_flags.empty:
    for _, r in top5_flags.iterrows():
        findings_lines.append(
            f"  - ORIGIN={int(r['ORIGIN_AIRPORT_ID'])}, DEST={int(r['DEST_AIRPORT_ID'])} | "
            f"rep={int(r['reporting_count'])}, mkt={int(r['marketing_count'])}, ratio={float(r['ratio_mkt_vs_rep']):.2f}"
        )
else:
    findings_lines.append("  - None")

# Write TXT for easy copy/paste into Word/PDF
txt_path = os.path.join(OUTPUT_DIR, "audit_findings_for_report.txt")
with open(txt_path, "w", encoding="utf-8") as f:
    f.write("\n".join(findings_lines))

# Write Markdown version (optional)
md_lines = []
md_lines.append("# Audit Findings – Numeric Summary (Auto-generated)")
md_lines.append("")
md_lines.append("## Coverage & Exceptions")
md_lines.append(f"- Reporting unique routes: **{fmt_int(n_routes_rep)}**")
md_lines.append(f"- Marketing unique routes: **{fmt_int(n_routes_mkt)}**")
md_lines.append(
    f"- Reporting coverage of marketing route universe: **{fmt_int(routes_intersection)} / {fmt_int(n_routes_mkt)} "
    f"({fmt_pct(coverage_pct)})**"
)
md_lines.append(
    f"- Marketing-only routes (exceptions): **{fmt_int(n_mkt_only_routes)} ({fmt_pct(pct_mkt_only_routes)})**"
)
md_lines.append(f"- Airports only in Marketing view: **{fmt_int(n_airports_only_mkt)}**")

md_lines.append("")
md_lines.append("## Overcounting / Population Definition Flags")
if n_flags:
    md_lines.append(
        f"- Flagged routes: **{fmt_int(n_flags)}** (rep_count ≥ 20 and ratio ≥ 3.0)"
    )
    md_lines.append(f"- Ratio stats: median **{median_ratio:.2f}**, max **{max_ratio:.2f}**")
else:
    md_lines.append("- No routes matched the overcounting flag criteria.")

md_lines.append("")
md_lines.append("## Samples")
md_lines.append("**Marketing-only routes (first 5):**")
if not top5_routes_mkt_only.empty:
    for _, r in top5_routes_mkt_only.iterrows():
        md_lines.append(f"- ORIGIN={int(r['ORIGIN_AIRPORT_ID'])}, DEST={int(r['DEST_AIRPORT_ID'])}")
else:
    md_lines.append("- None")

md_lines.append("")
md_lines.append("**Top 5 overcounting flags:**")
if not top5_flags.empty:
    for _, r in top5_flags.iterrows():
        md_lines.append(
            f"- ORIGIN={int(r['ORIGIN_AIRPORT_ID'])}, DEST={int(r['DEST_AIRPORT_ID'])} | "
            f"rep={int(r['reporting_count'])}, mkt={int(r['marketing_count'])}, ratio={float(r['ratio_mkt_vs_rep']):.2f}"
        )
else:
    md_lines.append("- None")

md_path = os.path.join(OUTPUT_DIR, "audit_findings_for_report.md")
with open(md_path, "w", encoding="utf-8") as f:
    f.write("\n".join(md_lines))

print(f"Report-ready findings written to:\n- {txt_path}\n- {md_path}")
