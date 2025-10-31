import streamlit as st
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import altair as alt
from io import StringIO
import pymysql  # driver for mysql+pymysql

# --- Compatibility wrappers for Streamlit width API ---
# We use stdlib inspect for signature checks
# ---------- Compatibility wrappers for Streamlit width/height/use_container_width API ----------
import inspect as _inspect

def _sig_supports(func, name):
    """Return True if callable `func` accepts parameter `name`."""
    try:
        sig = _inspect.signature(func)
        return name in sig.parameters
    except Exception:
        return False

# detect supported params for altair_chart
_ALT_SUPPORTS_WIDTH = _sig_supports(st.altair_chart, "width")
_ALT_SUPPORTS_HEIGHT = _sig_supports(st.altair_chart, "height")
_ALT_SUPPORTS_USE_CONTAINER = _sig_supports(st.altair_chart, "use_container_width")

# detect supported params for dataframe
_DF_SUPPORTS_WIDTH = _sig_supports(st.dataframe, "width")
_DF_SUPPORTS_HEIGHT = _sig_supports(st.dataframe, "height")
_DF_SUPPORTS_USE_CONTAINER = _sig_supports(st.dataframe, "use_container_width")

def display_chart(chart, *, height=None):
    """
    Robust wrapper to display an Altair chart across Streamlit versions.
    - Tries to call st.altair_chart with width/height when possible.
    - If 'height' param not supported, sets chart = chart.properties(height=height).
    - If 'width' not supported, falls back to use_container_width (if available).
    """
    # ensure chart is an Altair chart object
    try:
        is_altair = hasattr(chart, "to_dict")
    except Exception:
        is_altair = False

    # If height param not supported but we have an Altair chart, set it on the chart
    chart_to_show = chart
    if height is not None and not _ALT_SUPPORTS_HEIGHT and is_altair:
        try:
            chart_to_show = chart.properties(height=height)
        except Exception:
            # if setting properties fails, ignore and display without
            chart_to_show = chart

    # Preferred call: width supported
    if _ALT_SUPPORTS_WIDTH:
        kwargs = {}
        if height is not None and _ALT_SUPPORTS_HEIGHT:
            kwargs["height"] = height
        # width param supported -> use "stretch" for modern API
        try:
            st.altair_chart(chart_to_show, width="stretch", **kwargs)
            return
        except TypeError:
            # fall through to other methods
            pass

    # Fallback: use_container_width supported?
    if _ALT_SUPPORTS_USE_CONTAINER:
        if height is not None and _ALT_SUPPORTS_HEIGHT:
            # if height supported (rare) include it
            st.altair_chart(chart_to_show, use_container_width=True, height=height)
        else:
            st.altair_chart(chart_to_show, use_container_width=True)
        return

    # Last fallback: call without special args (chart may already have properties(height=...))
    try:
        st.altair_chart(chart_to_show)
    except Exception:
        # as a last-ditch, convert to st.write
        st.write(chart_to_show)

def display_dataframe(df, *, height=None):
    """
    Robust wrapper for st.dataframe across Streamlit versions.
    - Uses width='stretch' if supported.
    - Else falls back to use_container_width if supported.
    - Height passed only if supported by st.dataframe.
    """
    # if width supported, try width + optionally height
    if _DF_SUPPORTS_WIDTH:
        kwargs = {}
        if height is not None and _DF_SUPPORTS_HEIGHT:
            kwargs["height"] = height
        try:
            st.dataframe(df, width="stretch", **kwargs)
            return
        except TypeError:
            pass

    # else if use_container_width supported
    if _DF_SUPPORTS_USE_CONTAINER:
        if height is not None and _DF_SUPPORTS_HEIGHT:
            st.dataframe(df, use_container_width=True, height=height)
        else:
            st.dataframe(df, use_container_width=True)
        return

    # last fallback
    try:
        st.dataframe(df)
    except Exception:
        st.write(df)


# ---------- CONFIG ----------
DB_USER = "root"
DB_PASS = ""
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_NAME = "football_db"

CONN_STR = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = sa.create_engine(CONN_STR, pool_pre_ping=True)

st.set_page_config(page_title="Football Analytics Dashboard", layout="wide")
st.title("Football Analytics Dashboard — Premier League 2024-2025")

# ---------- Utility helpers ----------
@st.cache_data(ttl=300)
def load_teams():
    q = "SELECT idequipe, nomequipe FROM equipe ORDER BY nomequipe"
    return pd.read_sql_query(q, engine)

@st.cache_data(ttl=300)
def load_players_count():
    q = "SELECT COUNT(*) AS cnt FROM joueur"
    df = pd.read_sql_query(q, engine)
    return int(df['cnt'].iloc[0]) if not df.empty else 0

@st.cache_data(ttl=300)
def load_nationalities():
    q = "SELECT DISTINCT nationalite FROM joueur WHERE nationalite IS NOT NULL"
    return pd.read_sql_query(q, engine)

def df_to_csv_bytes(df: pd.DataFrame):
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode('utf-8')

# ---------- Sidebar filters ----------
st.sidebar.header("Filters")
teams_df = load_teams()
team_options = teams_df['nomequipe'].tolist() if not teams_df.empty else []
selected_teams = st.sidebar.multiselect("Select team(s)", options=team_options, default=None)

nationalities_df = load_nationalities()
nationality_options = list(nationalities_df['nationalite'].dropna().astype(str).unique()) if not nationalities_df.empty else []
selected_nationalities = st.sidebar.multiselect("Nationality filter", options=nationality_options, default=None)

top_n = st.sidebar.slider("Top N (for top players)", min_value=3, max_value=50, value=10, step=1)

def team_filter_clause(alias='e'):
    if not selected_teams:
        return "", {}
    placeholders = ", ".join([f":t{i}" for i in range(len(selected_teams))])
    clause = f" AND {alias}.nomequipe IN ({placeholders}) "
    params = {f"t{i}": selected_teams[i] for i in range(len(selected_teams))}
    return clause, params

def nationality_filter_clause(alias='j'):
    if not selected_nationalities:
        return "", {}
    placeholders = ", ".join([f":n{i}" for i in range(len(selected_nationalities))])
    clause = f" AND {alias}.nationalite IN ({placeholders}) "
    params = {f"n{i}": selected_nationalities[i] for i in range(len(selected_nationalities))}
    return clause, params

# ---------- Analysis functions ----------
@st.cache_data(ttl=300)
def top_scorers(limit=10, team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT j.nomjoueur AS player, e.nomequipe AS team, SUM(COALESCE(s.buts,0)) AS goals
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY j.nomjoueur, e.nomequipe
        ORDER BY goals DESC
        LIMIT :limit
    """)
    params2 = dict(params); params2['limit'] = limit
    return pd.read_sql_query(q, engine, params=params2)

@st.cache_data(ttl=300)
def most_decisive(limit=10, team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT j.nomjoueur AS player, e.nomequipe AS team,
               SUM(COALESCE(s.buts,0)) AS goals,
               SUM(COALESCE(s.passesdecisives,0)) AS assists,
               SUM(COALESCE(s.buts,0))+SUM(COALESCE(s.passesdecisives,0)) AS influence
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY j.nomjoueur, e.nomequipe
        ORDER BY influence DESC
        LIMIT :limit
    """)
    p = dict(params); p['limit']=limit
    return pd.read_sql_query(q, engine, params=p)

@st.cache_data(ttl=300)
def most_disciplined(limit=10, team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT j.nomjoueur AS player, e.nomequipe AS team,
               SUM(COALESCE(s.cartonsjaunes,0)) AS yellows,
               SUM(COALESCE(s.cartonsrouges,0)) AS reds,
               SUM(COALESCE(s.cartonsjaunes,0)) + 3*SUM(COALESCE(s.cartonsrouges,0)) AS discipline_score
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY j.nomjoueur, e.nomequipe
        ORDER BY discipline_score DESC
        LIMIT :limit
    """)
    p = dict(params); p['limit']=limit
    return pd.read_sql_query(q, engine, params=p)

@st.cache_data(ttl=300)
def nationality_distribution(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT e.nomequipe AS team, j.nationalite AS nationality, COUNT(*) AS count
        FROM joueur j
        JOIN equipe e ON j.id_equipe = e.idequipe
        WHERE j.nationalite IS NOT NULL {team_where}
        GROUP BY e.nomequipe, j.nationalite
        ORDER BY e.nomequipe, count DESC
    """)
    return pd.read_sql_query(q, engine, params=params)

@st.cache_data(ttl=300)
def total_goals_per_team(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT e.nomequipe AS team,
               SUM(COALESCE(r.butsmarques,0)) AS total_goals_for,
               SUM(COALESCE(r.butsconcedes,0)) AS total_goals_against
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY e.nomequipe
        ORDER BY total_goals_for DESC
    """)
    return pd.read_sql_query(q, engine, params=params)

@st.cache_data(ttl=300)
def avg_goals_per_match(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT t.team, 
               (t.total_goals_for / NULLIF(t.matches_played,0)) AS avg_scored_per_match,
               (t.total_goals_against / NULLIF(t.matches_played,0)) AS avg_conceded_per_match,
               t.total_goals_for, t.total_goals_against, t.matches_played
        FROM (
            SELECT e.nomequipe AS team,
                   SUM(COALESCE(r.butsmarques,0)) AS total_goals_for,
                   SUM(COALESCE(r.butsconcedes,0)) AS total_goals_against,
                   COUNT(DISTINCT r.idmatch) AS matches_played
            FROM resultatmatch r
            JOIN equipe e ON r.idequipe = e.idequipe
            WHERE 1=1 {team_where}
            GROUP BY e.nomequipe
        ) t
        ORDER BY avg_scored_per_match DESC
    """)
    return pd.read_sql_query(q, engine, params=params)

@st.cache_data(ttl=300)
def league_table(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT e.nomequipe AS team,
               SUM(CASE WHEN r.resultat='Victoire' THEN 3 WHEN r.resultat='Nul' THEN 1 ELSE 0 END) AS points,
               SUM(COALESCE(r.butsmarques,0)) AS goals_for,
               SUM(COALESCE(r.butsconcedes,0)) AS goals_against,
               SUM(COALESCE(r.butsmarques,0)) - SUM(COALESCE(r.butsconcedes,0)) AS goal_diff,
               COUNT(DISTINCT r.idmatch) AS matches_played,
               SUM(CASE WHEN r.resultat='Victoire' THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN r.resultat='Nul' THEN 1 ELSE 0 END) AS draws,
               SUM(CASE WHEN r.resultat='Défaite' THEN 1 ELSE 0 END) AS losses
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY e.nomequipe
        ORDER BY points DESC, goal_diff DESC, goals_for DESC
    """)
    return pd.read_sql_query(q, engine, params=params)

@st.cache_data(ttl=300)
def best_defense(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT e.nomequipe AS team, SUM(COALESCE(r.butsconcedes,0)) AS goals_conceded
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY e.nomequipe
        ORDER BY goals_conceded ASC
    """)
    return pd.read_sql_query(q, engine, params=params)

# ---------- DB inspection & adaptive top-scorer ----------
# Use sa.inspect to avoid colliding with stdlib inspect
inspector = sa.inspect(engine)
all_tables = inspector.get_table_names()
if 'match_' in all_tables:
    TABLE_MATCH_NAME = 'match_'
elif 'match' in all_tables:
    TABLE_MATCH_NAME = 'match'
else:
    TABLE_MATCH_NAME = None

COL_IDTEAM_AWAY = 'idteam_away'
if TABLE_MATCH_NAME:
    cols = [c['name'] for c in inspector.get_columns(TABLE_MATCH_NAME)]
    if 'idteam__away' in cols:
        COL_IDTEAM_AWAY = 'idteam__away'
    elif 'idteam_away' in cols:
        COL_IDTEAM_AWAY = 'idteam_away'

st.sidebar.markdown(f"**Detected DB:** tables={len(all_tables)}, match_table={TABLE_MATCH_NAME}, away_col={COL_IDTEAM_AWAY}")

@st.cache_data(ttl=300)
def top_scorer_per_team(team_where="", params=None):
    params = params or {}
    try:
        q_rownum = text(f"""
            SELECT t.team, t.player, t.goals FROM (
                SELECT e.nomequipe AS team, j.nomjoueur AS player, SUM(COALESCE(s.buts,0)) AS goals,
                       ROW_NUMBER() OVER (PARTITION BY e.nomequipe ORDER BY SUM(COALESCE(s.buts,0)) DESC) AS rn
                FROM statistiquejoueur s
                JOIN joueur j ON s.idjoueur = j.idjoueur
                JOIN equipe e ON j.id_equipe = e.idequipe
                WHERE 1=1 {team_where}
                GROUP BY e.nomequipe, j.nomjoueur
            ) t
            WHERE t.rn = 1
            ORDER BY t.goals DESC
        """)
        return pd.read_sql_query(q_rownum, engine, params=params)
    except Exception:
        q_fallback = text(f"""
            SELECT agg.team, agg.player, agg.goals
            FROM (
                SELECT e.nomequipe AS team, j.nomjoueur AS player, SUM(COALESCE(s.buts,0)) AS goals
                FROM statistiquejoueur s
                JOIN joueur j ON s.idjoueur = j.idjoueur
                JOIN equipe e ON j.id_equipe = e.idequipe
                WHERE 1=1 {team_where}
                GROUP BY e.nomequipe, j.nomjoueur
            ) agg
            JOIN (
                SELECT team AS team2, MAX(goals) AS max_goals
                FROM (
                    SELECT e.nomequipe AS team, j.nomjoueur AS player, SUM(COALESCE(s.buts,0)) AS goals
                    FROM statistiquejoueur s
                    JOIN joueur j ON s.idjoueur = j.idjoueur
                    JOIN equipe e ON j.id_equipe = e.idequipe
                    WHERE 1=1 {team_where}
                    GROUP BY e.nomequipe, j.nomjoueur
                ) sub
                GROUP BY team
            ) mx ON agg.team = mx.team2 AND agg.goals = mx.max_goals
            ORDER BY agg.goals DESC
        """)
        return pd.read_sql_query(q_fallback, engine, params=params)

@st.cache_data(ttl=300)
def matches_played_per_team(team_where="", params=None):
    params = params or {}
    q = text(f"""
        SELECT e.nomequipe AS team, COUNT(DISTINCT r.idmatch) AS matches_played
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        WHERE 1=1 {team_where}
        GROUP BY e.nomequipe
        ORDER BY matches_played DESC
    """)
    return pd.read_sql_query(q, engine, params=params)

# ---------- UI: Tabs & charts ----------
tab = st.tabs(["Overview","Top Scorers","Decisive Players","Discipline","Nationalities","Team Goals","Avg Goals/Match","League Table","Defense","Top Scorer per Team","Matches Count"])

team_clause, team_params = team_filter_clause('e')
nat_clause, nat_params = nationality_filter_clause('j')
combined_params = {**team_params, **nat_params}

with tab[0]:
    st.header("Overview")
    st.write("Quick summary statistics")
    tg = total_goals_per_team(team_clause, combined_params)
    lt = league_table(team_clause, combined_params)
    col1, col2, col3 = st.columns(3)
    col1.metric("Teams in DB", len(team_options))
    col2.metric("Matches (teams with goals rows)", int(tg.shape[0] if not tg.empty else 0))
    col3.metric("Players (distinct, DB)", load_players_count())
    st.markdown("Use the sidebar to filter by team or nationality. Charts and tables update accordingly.")

with tab[1]:
    st.header(f"Top {top_n} Scorers")
    df_top = top_scorers(limit=top_n, team_where=team_clause, params=combined_params)
    if df_top.empty:
        st.info("No scorer data available for the selected filters.")
    else:
        chart = alt.Chart(df_top).mark_bar().encode(
            x=alt.X("goals:Q"),
            y=alt.Y("player:N", sort='-x'),
            tooltip=["player","team","goals"]
        ).properties(height=400)
        display_chart(chart, height=400)
        display_dataframe(df_top)
        st.download_button("Download CSV", data=df_to_csv_bytes(df_top), file_name="top_scorers.csv", mime="text/csv")

with tab[2]:
    st.header("Most Decisive Players (Goals + Assists)")
    df_dec = most_decisive(limit=top_n, team_where=team_clause, params=combined_params)
    display_dataframe(df_dec)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_dec), file_name="decisive_players.csv", mime="text/csv")

with tab[3]:
    st.header("Most Disciplined (Yellow / Red Cards)")
    df_disc = most_disciplined(limit=top_n, team_where=team_clause, params=combined_params)
    display_dataframe(df_disc)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_disc), file_name="discipline.csv", mime="text/csv")

with tab[4]:
    st.header("Nationality Distribution by Team")
    df_nat = nationality_distribution(team_clause, combined_params)
    if df_nat.empty:
        st.info("No nationality data found.")
    else:
        st.write("Select a team to see its nationality breakdown (or leave none to see all).")
        team_for_nat = st.selectbox("Team for nationality chart (optional)", ["(All)"] + team_options)
        if team_for_nat != "(All)":
            df_nat = df_nat[df_nat['team'] == team_for_nat]
        if not df_nat.empty:
            chart = alt.Chart(df_nat).mark_bar().encode(
                x=alt.X("count:Q"),
                y=alt.Y("nationality:N", sort='-x'),
                color=alt.Color("team:N"),
                tooltip=["team","nationality","count"]
            ).properties(height=400)
            display_chart(chart, height=400)
            display_dataframe(df_nat)
            st.download_button("Download CSV", data=df_to_csv_bytes(df_nat), file_name="nationalities.csv", mime="text/csv")
        else:
            st.info("No data after applying selection.")

with tab[5]:
    st.header("Total Goals by Team")
    df_goals = total_goals_per_team(team_clause, combined_params)
    chart = alt.Chart(df_goals).mark_bar().encode(
        x=alt.X("total_goals_for:Q"),
        y=alt.Y("team:N", sort='-x'),
        tooltip=["team","total_goals_for","total_goals_against"]
    ).properties(height=500)
    display_chart(chart, height=500)
    display_dataframe(df_goals)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_goals), file_name="goals_per_team.csv", mime="text/csv")

with tab[6]:
    st.header("Average Goals Scored & Conceded per Match (team)")
    df_avg = avg_goals_per_match(team_clause, combined_params)
    display_dataframe(df_avg)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_avg), file_name="avg_goals_per_match.csv", mime="text/csv")

with tab[7]:
    st.header("League Table")
    df_table = league_table(team_clause, combined_params)
    display_dataframe(df_table)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_table), file_name="league_table.csv", mime="text/csv")

with tab[8]:
    st.header("Teams with Best Defense (Fewest Goals Conceded)")
    df_def = best_defense(team_clause, combined_params)
    chart = alt.Chart(df_def).mark_bar().encode(
        x="goals_conceded:Q",
        y=alt.Y("team:N", sort='-x'),
        tooltip=["team","goals_conceded"]
    ).properties(height=450)
    display_chart(chart, height=450)
    display_dataframe(df_def)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_def), file_name="best_defense.csv", mime="text/csv")

with tab[9]:
    st.header("Top Scorer per Team")
    try:
        df_top_team = top_scorer_per_team(team_clause, combined_params)
        display_dataframe(df_top_team)
        st.download_button("Download CSV", data=df_to_csv_bytes(df_top_team), file_name="top_scorer_per_team.csv", mime="text/csv")
    except Exception as e:
        st.error("Top-scorer-per-team query failed. Error: " + str(e))

with tab[10]:
    st.header("Total Matches Played per Team")
    df_mp = matches_played_per_team(team_clause, combined_params)
    display_dataframe(df_mp)
    st.download_button("Download CSV", data=df_to_csv_bytes(df_mp), file_name="matches_per_team.csv", mime="text/csv")

st.markdown("---")
st.markdown("**Notes:**\n- This dashboard queries the MySQL DB directly. Make sure your database is running and reachable from this machine.\n- Some queries assume aggregated season totals are stored in `statistiquejoueur`. The queries sum over available rows.\n- If MySQL version < 8 (no window functions), the \"Top scorer per team\" query will fall back to a compatible method.")
