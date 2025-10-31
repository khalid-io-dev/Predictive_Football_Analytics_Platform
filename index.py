# fbref_premier_league_scraper.py
# --------------------------------------------------------------
#  • Scrapes Standard Stats (base + Playing Time + Performance only)
#  • Scrapes Scores & Fixtures (all except Match Report)
#  • For all 20 Premier League teams
#  • Saves per team CSV + combined
#  • Uses local Chrome → cached Chrome → cached Edge
#  • Offline after first run
# --------------------------------------------------------------

import os
import re
import time
import random
import zipfile
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
BASE_URL = "https://fbref.com/en/comps/9/2024-2025/2024-2025-Premier-League-Stats"
OUTPUT_DIR = "fbref_output_1"
HEADLESS = True
DELAY_MIN = 1.5
DELAY_MAX = 3.5

CACHE_DIR = Path(__file__).parent / ".webdriver_cache"
CACHE_DIR.mkdir(exist_ok=True)


# ----------------------------------------------------------------------
# Helper – download once
# ----------------------------------------------------------------------
def _download_once(url: str, dest_zip: Path) -> Path:
    import urllib.request
    if dest_zip.exists():
        return dest_zip
    print(f"[i] Downloading {url} → {dest_zip.name}")
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest_zip)
    return dest_zip


# ----------------------------------------------------------------------
# DRIVER INITIALISATION
# ----------------------------------------------------------------------
def _try_local_chrome() -> Optional[webdriver.Remote]:
    try:
        opts = webdriver.ChromeOptions()
        if HEADLESS:
            opts.add_argument("--headless=new")
            opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
        )
        driver = webdriver.Chrome(options=opts)
        print("[*] LOCAL Chrome started")
        return driver
    except Exception as e:
        print(f"[!] Local Chrome failed: {e}")
        return None


# def _get_chrome_driver_path() -> Path:
#     zip_path = CACHE_DIR / "chrome-win64.zip"
#     driver_dest = CACHE_DIR / "chromedriver.exe"
#     if driver_dest.exists():
#         return driver_dest
#     _download_once(
#         "https://storage.googleapis.com/chrome-for-testing-public/129.0.6668.89/win64/chrome-win64.zip",
#         zip_path,
#     )
#     with tempfile.TemporaryDirectory() as tmp:
#         with zipfile.ZipFile(zip_path) as z:
#             z.extractall(tmp)
#         src = Path(tmp) / "chrome-win64" / "chromedriver.exe"
#         shutil.copy(src, driver_dest)
#     return driver_dest


def _start_cached_chrome() -> webdriver.Remote:
    chromedriver = _get_chrome_driver_path()
    opts = webdriver.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
    )
    driver = webdriver.Chrome(executable_path=str(chromedriver), options=opts)
    print("[*] Virtual Chrome (cached) started")
    return driver


def _get_edge_driver_path() -> Path:
    zip_path = CACHE_DIR / "edgedriver_win64.zip"
    driver_dest = CACHE_DIR / "msedgedriver.exe"
    if driver_dest.exists():
        return driver_dest
    _download_once(
        "https://msedgedriver.azureedge.net/129.0.6668.89/edgedriver_win64.zip",
        zip_path,
    )
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(tmp)
        src = Path(tmp) / "msedgedriver.exe"
        shutil.copy(src, driver_dest)
    return driver_dest


def _start_cached_edge() -> webdriver.Remote:
    edgedriver = _get_edge_driver_path()
    args = [
        "--window-size=1920,1080",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36 Edg/129.0"
    ]
    if HEADLESS:
        args[0:0] = ["--headless", "--disable-gpu"]
    caps = {"browserName": "MicrosoftEdge", "ms:edgeOptions": {"args": args}}
    driver = webdriver.Edge(executable_path=str(edgedriver), capabilities=caps)
    print("[*] Edge (cached) started")
    return driver


def init_driver() -> webdriver.Remote:
    driver = _try_local_chrome()
    if driver:
        return driver
    try:
        return _start_cached_chrome()
    except Exception as e:
        print(f"[!] Cached Chrome failed: {e}")
    return _start_cached_edge()


# ----------------------------------------------------------------------
# SCRAPING LOGIC
# ----------------------------------------------------------------------
def polite_sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def fetch_page_html(driver, url: str) -> str:
    driver.get(url)
    time.sleep(1.0)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.5)
    return driver.page_source


def find_team_links_from_competition_html(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    teams = []
    tables = soup.find_all("table")
    for com in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in com:
            tables.extend(BeautifulSoup(com, "lxml").find_all("table"))
    for table in tables:
        anchors = table.find_all("a", href=True)
        squad_links = [a for a in anchors if "/en/squads/" in a["href"]]
        if len(squad_links) >= 10:
            for a in squad_links:
                name = a.get_text(strip=True)
                href = "https://fbref.com" + a["href"] if a["href"].startswith("/") else a["href"]
                teams.append((name, href))
            break
    seen = set()
    return [(n, u) for n, u in teams if u not in seen and not seen.add(u)]


def extract_tables_from_html(html: str) -> List[pd.DataFrame]:
    dfs = []
    soup = BeautifulSoup(html, "lxml")
    for t in soup.find_all("table"):
        try:
            tables = pd.read_html(StringIO(str(t)))
            if tables:
                dfs.append(tables[0])
        except Exception:
            continue
    for com in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in com:
            try:
                cs = BeautifulSoup(com, "lxml")
                for t in cs.find_all("table"):
                    tables = pd.read_html(StringIO(str(t)))
                    if tables:
                        dfs.append(tables[0])
            except Exception:
                continue
    return dfs


def flatten_multiindex_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(c) for c in col if str(c).strip()]) for col in df.columns.values]
    return df


def select_standard_stats_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = flatten_multiindex_columns(df)
    # Keep base + Playing Time + Performance; drop Expected, Progression, Per 90 Minutes, Matches
    cols_to_keep = [c for c in df.columns if not c.startswith('Expected') and not c.startswith('Progression') and not c.startswith('Per 90 Minutes') and c != 'Matches']
    return df[cols_to_keep]


def remove_match_report_col(df: pd.DataFrame) -> pd.DataFrame:
    df = flatten_multiindex_columns(df)
    cols_to_drop = [c for c in df.columns if re.search(r"match\s*report|mr", str(c), re.I)]
    return df.drop(columns=cols_to_drop, errors="ignore")


def choose_standard_and_fixtures_tables(dfs: List[pd.DataFrame]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    standard = fixtures = None
    for df in dfs:
        df_flat = flatten_multiindex_columns(df)
        cols = " ".join(str(c).lower() for c in df_flat.columns)
        if standard is None and any(k in cols for k in ("nation", "pos", "age", "born", "gls", "ast")):
            standard = df_flat.copy()
        if fixtures is None and any(k in cols for k in ("date", "opponent", "result", "gf", "ga", "venue")):
            fixtures = df_flat.copy()
        if standard is not None and fixtures is not None:
            break
    if standard is not None:
        standard = select_standard_stats_columns(standard)
    if fixtures is not None:
        fixtures = remove_match_report_col(fixtures)
    return standard, fixtures


def process_team_page(driver, team_name: str, team_url: str):
    print(f"[+] {team_name} → {team_url}")
    html = fetch_page_html(driver, team_url)
    tables = extract_tables_from_html(html)
    if not tables:
        print("  ! No tables found")
        return None, None

    s_df, f_df = choose_standard_and_fixtures_tables(tables)

    if s_df is not None and not s_df.empty:
        s_df["team_name"] = team_name
        s_df["team_url"] = team_url
    else:
        s_df = None

    if f_df is not None and not f_df.empty:
        f_df["team_name"] = team_name
        f_df["team_url"] = team_url
    else:
        f_df = None

    return s_df, f_df


def safe_filename(s: str) -> str:
    return "".join(ch for ch in s if ch.isalnum() or ch in (" ", "-", "_")).strip().replace(" ", "_")


# ----------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    driver = init_driver()
    try:
        comp_html = fetch_page_html(driver, BASE_URL)
        teams = find_team_links_from_competition_html(comp_html)
        print(f"[+] Found {len(teams)} teams")

        all_standard = []
        all_fixtures = []

        for name, url in teams:
            try:
                s_df, f_df = process_team_page(driver, name, url)
                safe = safe_filename(name)

                if s_df is not None:
                    path = os.path.join(OUTPUT_DIR, f"{safe}_standard_stats.csv")
                    s_df.to_csv(path, index=False)
                    print(f"   • standard_stats → {path} ({len(s_df)} rows)")
                    all_standard.append(s_df)
                else:
                    print("   • no standard stats table")

                if f_df is not None:
                    path = os.path.join(OUTPUT_DIR, f"{safe}_scores_fixtures.csv")
                    f_df.to_csv(path, index=False)
                    print(f"   • scores_fixtures → {path} ({len(f_df)} rows)")
                    all_fixtures.append(f_df)
                else:
                    print("   • no scores & fixtures table")

            except Exception as exc:
                print(f"   ! error for {name}: {exc}")

            polite_sleep()

        if all_standard:
            pd.concat(all_standard, ignore_index=True).to_csv(
                os.path.join(OUTPUT_DIR, "all_teams_standard_stats.csv"), index=False
            )
            print("[+] all_teams_standard_stats.csv written")
        if all_fixtures:
            pd.concat(all_fixtures, ignore_index=True).to_csv(
                os.path.join(OUTPUT_DIR, "all_teams_scores_fixtures.csv"), index=False
            )
            print("[+] all_teams_scores_fixtures.csv written")

    finally:
        driver.quit()
        print("[*] Browser closed – done!")


if __name__ == "__main__":
    main()