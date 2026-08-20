#!/usr/bin/env python3
"""Reproduce the escalation measurement behind the default ladder.

Usage: python evidence_escalation.py /path/to/whitefield /path/to/hinjewadi
Reads SUBPROJECTSUMMARY.xlsx from each and reports launch->current CAGR by
age band. Those figures set LF_ESCALATION_EARLY / LF_ESCALATION_LATE.
"""
import sys
import pandas as pd


def main(dirs):
    frames = []
    for d in dirs:
        f = pd.read_excel(f"{d}/projects/SUBPROJECTSUMMARY.xlsx")
        frames.append(f[f["FY_QTR"] == "Q1 26-27"])
    d = pd.concat(frames)
    d = d[(d["LAUNCH_PRICE_PSF"] > 500) & (d["CURRENT_PRICE_PSF"] > 500)].copy()
    d["launch_dt"] = pd.to_datetime(d["LAUNCH_MONTH_YEAR"], errors="coerce")
    d["years"] = (pd.Timestamp("2026-06-30") - d["launch_dt"]).dt.days / 365.25
    d = d[(d["years"] > 0.5) & (d["years"] < 15)]
    d["uplift"] = d["CURRENT_PRICE_PSF"] / d["LAUNCH_PRICE_PSF"]
    d["cagr"] = d["uplift"] ** (1 / d["years"]) - 1

    print(f"sample: {len(d)} wings / {d['PROJECT_NAME'].nunique()} projects")
    for lo, hi, lab in [(0.5, 2, "0.5-2 yrs"), (2, 4, "2-4 yrs"),
                        (4, 7, "4-7 yrs"), (7, 15, "7+ yrs")]:
        s = d[(d["years"] >= lo) & (d["years"] < hi)]
        if len(s) < 3:
            continue
        print(f"  {lab:<12} n={len(s):<5} median uplift {s['uplift'].median():.2f}x "
              f"median CAGR {s['cagr'].median()*100:5.1f}%")
    print(f"  overall median CAGR {d['cagr'].median()*100:.1f}%")
    print(f"  wings below launch price today: {(d['uplift'] < 1).mean()*100:.1f}%")


if __name__ == "__main__":
    main(sys.argv[1:] or ["/root/whitefield", "/root/hinjewadi"])
