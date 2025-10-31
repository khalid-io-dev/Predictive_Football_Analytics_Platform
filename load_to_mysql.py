#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm
from datetime import datetime

# DB Config (MySQL)
DB_USER = "root"
DB_PASS = ""
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_NAME = "football_db"

# Connect to DB
url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = sa.create_engine(url)
Session = sessionmaker(bind=engine)
session = Session()

def safe_int(x):
    try:
        return int(pd.to_numeric(x, errors='coerce').fillna(0))
    except:
        return 0

def safe_time(x):
    try:
        return datetime.strptime(str(x), '%H:%M').time() if x and not pd.isna(x) else None
    except:
        return None

def safe_date(x):
    try:
        return pd.to_datetime(x, errors='coerce').date()
    except:
        return None

# Insert competition and saison if not exist
session.execute(sa.text("""
    INSERT IGNORE INTO competition (nomcompetition) VALUES ('Premier League')
"""))
session.execute(sa.text("""
    INSERT IGNORE INTO saison (annee) VALUES ('2024-2025')
"""))

# Get IDs
comp_id = session.execute(sa.text("SELECT idcompetition FROM competition WHERE nomcompetition = 'Premier League'")).scalar()
saison_id = session.execute(sa.text("SELECT id_saison FROM saison WHERE annee = '2024-2025'")).scalar()

# Process stats files
SILVER_DIR = Path('SILVER')
for file_path in tqdm(list(SILVER_DIR.rglob('*_standard_stats.csv'))):
    try:
        df = pd.read_csv(file_path, low_memory=False)
        df = df.fillna(0)
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        
        team_name = file_path.parent.name.replace('_', ' ')
        
        # Insert team
        session.execute(sa.text("""
            INSERT IGNORE INTO equipe (nomequipe, idcompetition, idsaison)
            VALUES (:nomequipe, :idcompetition, :idsaison)
        """), {'nomequipe': team_name, 'idcompetition': comp_id, 'idsaison': saison_id})
        
        equipe_id = session.execute(sa.text("""
            SELECT idequipe FROM equipe WHERE nomequipe = :nomequipe AND idcompetition = :idcompetition AND idsaison = :idsaison
        """), {'nomequipe': team_name, 'idcompetition': comp_id, 'idsaison': saison_id}).scalar()
        
        for _, row in df.iterrows():
            nomjoueur = str(row.get('unnamed:_0_level_0_player', '')).strip()
            if not nomjoueur or 'Squad' in nomjoueur or 'Opponent' in nomjoueur:
                continue
            
            position = str(row.get('unnamed:_2_level_0_pos', '')).strip()
            nationalite = str(row.get('unnamed:_1_level_0_nation', '')).strip()
            
            # Insert player
            session.execute(sa.text("""
                INSERT IGNORE INTO joueur (nomjoueur, position, nationalite, id_equipe)
                VALUES (:nomjoueur, :position, :nationalite, :id_equipe)
            """), {'nomjoueur': nomjoueur, 'position': position, 'nationalite': nationalite, 'id_equipe': equipe_id})
            
            idjoueur = session.execute(sa.text("""
                SELECT idjoueur FROM joueur WHERE nomjoueur = :nomjoueur AND id_equipe = :id_equipe
            """), {'nomjoueur': nomjoueur, 'id_equipe': equipe_id}).scalar()
            
            if idjoueur:
                buts = safe_int(row.get('performance_gls', 0))
                passesdecisives = safe_int(row.get('performance_ast', 0))
                nbmatchesplayed = safe_int(row.get('playing_time_mp', 0))
                cartonsjaunes = safe_int(row.get('performance_crdy', 0))
                cartonsrouges = safe_int(row.get('performance_crdr', 0))
                
                # Insert stats
                session.execute(sa.text("""
                    INSERT INTO statistiquejoueur (idjoueur, buts, passesdecisives, nbmatchesplayed, cartonsjaunes, cartonsrouges)
                    VALUES (:idjoueur, :buts, :passesdecisives, :nbmatchesplayed, :cartonsjaunes, :cartonsrouges)
                    ON DUPLICATE KEY UPDATE buts = :buts, passesdecisives = :passesdecisives, nbmatchesplayed = :nbmatchesplayed, cartonsjaunes = :cartonsjaunes, cartonsrouges = :cartonsrouges
                """), {'idjoueur': idjoueur, 'buts': buts, 'passesdecisives': passesdecisives, 'nbmatchesplayed': nbmatchesplayed, 'cartonsjaunes': cartonsjaunes, 'cartonsrouges': cartonsrouges})
        
        session.commit()
    except Exception as e:
        print(f"[ERROR] Failed on {file_path}: {e}")
        session.rollback()

# Process scores files
for file_path in tqdm(list(SILVER_DIR.rglob('*_scores_fixtures.csv'))):
    try:
        df = pd.read_csv(file_path, low_memory=False)
        df = df.fillna(0)
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
        
        team_name = file_path.parent.name.replace('_', ' ')
        
        # Insert team
        session.execute(sa.text("""
            INSERT IGNORE INTO equipe (nomequipe, idcompetition, idsaison)
            VALUES (:nomequipe, :idcompetition, :idsaison)
        """), {'nomequipe': team_name, 'idcompetition': comp_id, 'idsaison': saison_id})
        
        equipe_id = session.execute(sa.text("""
            SELECT idequipe FROM equipe WHERE nomequipe = :nomequipe AND idcompetition = :idcompetition AND idsaison = :idsaison
        """), {'nomequipe': team_name, 'idcompetition': comp_id, 'idsaison': saison_id}).scalar()
        
        for _, row in df.iterrows():
            date_match = safe_date(row.get('date', 0))
            if not date_match:
                continue
            
            heure = safe_time(row.get('time', 0))
            round_val = str(row.get('round', '')).strip()
            venue = str(row.get('venue', '')).strip()
            
            opponent = str(row.get('opponent', '')).strip()
            if not opponent:
                continue
            
            # Insert opponent team
            session.execute(sa.text("""
                INSERT IGNORE INTO equipe (nomequipe, idcompetition, idsaison)
                VALUES (:nomequipe, :idcompetition, :idsaison)
            """), {'nomequipe': opponent, 'idcompetition': comp_id, 'idsaison': saison_id})
            
            opponent_id = session.execute(sa.text("""
                SELECT idequipe FROM equipe WHERE nomequipe = :nomequipe AND idcompetition = :idcompetition AND idsaison = :idsaison
            """), {'nomequipe': opponent, 'idcompetition': comp_id, 'idsaison': saison_id}).scalar()
            
            butsmarques = safe_int(row.get('gf', 0))
            butsconcedes = safe_int(row.get('ga', 0))
            
            if venue == 'home':
                idteamhome = equipe_id
                idteam__away = opponent_id
            else:
                idteamhome = opponent_id
                idteam__away = equipe_id
                butsmarques, butsconcedes = butsconcedes, butsmarques
            
            # Insert match
            session.execute(sa.text("""
                INSERT IGNORE INTO `match` (date_match, heure, round, venue, idteamhome, idteam__away, id_competition, id_saison)
                VALUES (:date_match, :heure, :round, :venue, :idteamhome, :idteam__away, :id_competition, :id_saison)
            """), {'date_match': date_match, 'heure': heure, 'round': round_val, 'venue': venue, 'idteamhome': idteamhome, 'idteam__away': idteam__away, 'id_competition': comp_id, 'id_saison': saison_id})
            
            idmatch = session.execute(sa.text("""
                SELECT idmatch_ FROM `match` WHERE date_match = :date_match AND idteamhome = :idteamhome AND idteam__away = :idteam__away
            """), {'date_match': date_match, 'idteamhome': idteamhome, 'idteam__away': idteam__away}).scalar()
            
            if idmatch:
                resultat = 'Victoire' if butsmarques > butsconcedes else 'Défaite' if butsmarques < butsconcedes else 'Nul'
                
                # Insert for home team
                session.execute(sa.text("""
                    INSERT INTO resultatmatch (idmatch, idequipe, butsmarques, butsconcedes, resultat)
                    VALUES (:idmatch, :idequipe, :butsmarques, :butsconcedes, :resultat)
                    ON DUPLICATE KEY UPDATE butsmarques = :butsmarques
                """), {'idmatch': idmatch, 'idequipe': idteamhome, 'butsmarques': butsmarques, 'butsconcedes': butsconcedes, 'resultat': resultat})
                
                # Insert for away team
                away_resultat = 'Victoire' if butsconcedes > butsmarques else 'Défaite' if butsconcedes < butsmarques else 'Nul'
                session.execute(sa.text("""
                    INSERT INTO resultatmatch (idmatch, idequipe, butsmarques, butsconcedes, resultat)
                    VALUES (:idmatch, :idequipe, :butsmarques, :butsconcedes, :resultat)
                    ON DUPLICATE KEY UPDATE butsmarques = :butsmarques
                """), {'idmatch': idmatch, 'idequipe': idteam__away, 'butsmarques': butsconcedes, 'butsconcedes': butsmarques, 'resultat': away_resultat})
        
        session.commit()
    except Exception as e:
        print(f"[ERROR] Failed on {file_path}: {e}")
        session.rollback()

session.close()