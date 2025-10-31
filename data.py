#!/usr/bin/env python3
"""
Clean & tier fbref data – BRONZE (raw) | SILVER (cleaned) | GOLD (aggregated)

Run inside the folder that contains the team folders (e.g. fbref_output_1).
"""

import shutil
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm


# --------------------------------------------------------------------------- #
# 1. CONFIG
# --------------------------------------------------------------------------- #
BASE_DIR   = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "BRONZE"
SILVER_DIR = BASE_DIR / "SILVER"
GOLD_DIR   = BASE_DIR / "GOLD"

BRONZE_DIR.mkdir(exist_ok=True)
SILVER_DIR.mkdir(exist_ok=True)
GOLD_DIR.mkdir(exist_ok=True)


# --------------------------------------------------------------------------- #
# 2. CLEANING HELPERS
# --------------------------------------------------------------------------- #
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_").replace("+", "_plus_") for c in df.columns]
    return df


def clean_scores_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    df = _norm_cols(df)

    rename = {
        "squad": "team", "opponent": "opponent", "date": "date", "time": "time",
        "round": "round", "day": "day", "venue": "venue", "result": "result",
        "gf": "gf", "ga": "ga", "xg": "xg", "xga": "xga", "possession": "poss",
        "attendance": "attendance", "captain": "captain", "formation": "formation",
        "referee": "referee", "match_report": "match_report", "notes": "notes",
    }
    df = df.rename(columns={c: v for c, v in rename.items() if c in df.columns})

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], format="%H:%M", errors="coerce").dt.time

    numeric = ["gf", "ga", "xg", "xga", "poss", "attendance"]
    for c in numeric:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if {"date", "team", "opponent"}.issubset(df.columns):
        before = len(df)
        df = df.drop_duplicates(subset=["date", "team", "opponent"], keep="first")
        if before != len(df):
            print(f"    → removed {before-len(df)} duplicate rows")

    return df


def clean_standard_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = _norm_cols(df)

    rename = {
        "player": "player", "name": "player", "nation": "nation", "pos": "pos",
        "squad": "team", "age": "age", "born": "born", "mp": "mp",
        "starts": "starts", "min": "min", "90s": "90s", "gls": "gls",
        "ast": "ast", "g_plus_a": "g_plus_a", "g_minus_pk": "g_minus_pk",
        "pk": "pk", "pkatt": "pkatt", "crdy": "crdy", "crdr": "crdr",
        "xg": "xg", "npxg": "npxg", "xag": "xag", "npxg_plus_xag": "npxg_plus_xag",
        "prgc": "prgc", "prgp": "prgp", "prgr": "prgr",
    }
    df = df.rename(columns={c: v for c, v in rename.items() if c in df.columns})

    numeric = [
        "age", "mp", "starts", "min", "90s", "gls", "ast", "g_plus_a",
        "g_minus_pk", "pk", "pkatt", "crdy", "crdr", "xg", "npxg", "xag",
        "npxg_plus_xag", "prgc", "prgp", "prgr",
    ]
    for c in numeric:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    for c in ["player", "nation", "pos", "team"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    if {"player", "team"}.issubset(df.columns):
        before = len(df)
        df = df.drop_duplicates(subset=["player", "team"], keep="first")
        if before != len(df):
            print(f"    → removed {before-len(df)} duplicate rows")

    return df


# --------------------------------------------------------------------------- #
# 3. MAIN
# --------------------------------------------------------------------------- #
def main() -> None:
    team_dirs: List[Path] = [
        p for p in BASE_DIR.iterdir()
        if p.is_dir() and p.name not in {"BRONZE", "SILVER", "GOLD"}
    ]

    scores_frames: List[pd.DataFrame] = []
    stats_frames:  List[pd.DataFrame] = []

    for team_dir in tqdm(team_dirs, desc="Team folders"):
        team_name = team_dir.name

        bronze_team = BRONZE_DIR / team_name
        silver_team = SILVER_DIR / team_name
        bronze_team.mkdir(exist_ok=True)
        silver_team.mkdir(exist_ok=True)

        for csv_path in team_dir.glob("*.csv"):
            print(f"Processing: {csv_path.relative_to(BASE_DIR)}")

            # ---- BRONZE ---------------------------------------------------- #
            bronze_dest = bronze_team / csv_path.name
            shutil.copy(csv_path, bronze_dest)

            # ---- SILVER ---------------------------------------------------- #
            raw = pd.read_csv(csv_path, low_memory=False)

            if "scores_fixtures" in csv_path.name.lower():
                clean = clean_scores_fixtures(raw)
            elif "standard_stats" in csv_path.name.lower():
                clean = clean_standard_stats(raw)
            else:
                clean = raw.drop_duplicates()
                num = clean.select_dtypes(include="number").columns
                clean[num] = clean[num].fillna(0)

            silver_dest = silver_team / csv_path.name
            clean.to_csv(silver_dest, index=False)

            # ---- GOLD ------------------------------------------------------ #
            if team_dir.name.lower() == "all_teams":
                continue

            # Ensure a team column
            if "team" not in clean.columns:
                clean = clean.copy()
                clean["team"] = team_name

            if "scores_fixtures" in csv_path.name.lower():
                scores_frames.append(clean)
                continue

            if "standard_stats" in csv_path.name.lower():
                # ---- SAFELY add to stats aggregation ----------------------- #
                if "player" not in clean.columns:
                    # try common alternatives
                    for alt in ("name", "Player", "player_name"):
                        if alt in clean.columns:
                            clean = clean.rename(columns={alt: "player"})
                            break
                    else:
                        print(f"    [WARN] No player column → skipping {csv_path.name} for GOLD stats")
                        continue

                # final guard
                if {"player", "team"}.issubset(clean.columns):
                    stats_frames.append(clean)
                else:
                    print(f"    [WARN] Missing player/team → skipping {csv_path.name} for GOLD stats")
                continue

    # --------------------------------------------------------------------- #
    # 4. GOLD – aggregated files
    # --------------------------------------------------------------------- #
    if scores_frames:
        all_scores = pd.concat(scores_frames, ignore_index=True)
        all_scores = all_scores.drop_duplicates(subset=["date", "team", "opponent"], keep="first")
        num = all_scores.select_dtypes(include="number").columns
        all_scores[num] = all_scores[num].fillna(0)
        path = GOLD_DIR / "all_scores_fixtures.csv"
        all_scores.to_csv(path, index=False)
        print(f"\nGOLD scores → {path}")

    if stats_frames:
        all_stats = pd.concat(stats_frames, ignore_index=True)
        all_stats = all_stats.drop_duplicates(subset=["player", "team"], keep="first")
        num = all_stats.select_dtypes(include="number").columns
        all_stats[num] = all_stats[num].fillna(0)
        path = GOLD_DIR / "all_standard_stats.csv"
        all_stats.to_csv(path, index=False)
        print(f"GOLD stats  → {path}")
    else:
        print("\nNo valid player-stats files were found for GOLD aggregation.")

    print("\nFinished – BRONZE / SILVER / GOLD are ready.")


if __name__ == "__main__":
    main()