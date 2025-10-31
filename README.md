# ⚽ Predictive Football Analytics Platform

## 🧠 Overview
This project aims to develop a **complete predictive analytics solution** for professional football, focused on the **Premier League 2024–2025** season.  
By leveraging data from **FBref**, the system collects, cleans, stores, analyzes, and visualizes performance data for teams and players.  
The ultimate goal is to build a **data-driven ecosystem** that can anticipate match outcomes and help optimize team strategies.

---

## 🚀 Project Objectives
- Build an **automated web scraping pipeline** to collect football data from FBref.
- Clean, standardize, and store the data in a **PostgreSQL relational database**.
- Execute analytical **SQL queries** to uncover performance insights.
- Design a **Streamlit dashboard** to visualize and explore football data interactively.
- Lay the groundwork for **predictive modeling** to forecast match results.

---

## 🧩 Architecture Overview
The solution follows a **data pipeline architecture** composed of four main stages:

1. **Data Collection (Web Scraping)**
   - Extract team, player, and match data using Selenium.
   - Collect detailed player stats and match results for the 2024–2025 Premier League season.
   - Export collected data as structured CSV files.

2. **Data Transformation**
   - Clean and standardize the data (dates, numeric formats, missing values).
   - Harmonize column names and data types.
   - Output the processed data to a dedicated `cleaned/` directory.

3. **Database Storage**
   - Store transformed data in **PostgreSQL** following a defined relational model:
     - `competition(idcompetition, nomcompetition)`
     - `saison(id_saison, annee)`
     - `equipe(idequipe, nomequipe, idcompetition, idsaison)`
     - `joueur(idjoueur, nomjoueur, position, nationalite, id_equipe)`
     - `match(idmatch, date_match, heure, round, venue, idteamhome, idteamaway, id_competition, id_saison)`
     - `resultatmatch(idresultat, idmatch, idequipe, butsmarques, butsconcedes, resultat ENUM('Victoire', 'Défaite', 'Nul'))`
     - `statistiquejoueur(idstats, idjoueur, buts, passesdecisives, nbmatchesplayed, cartonsjaunes, cartonsrouges)`
   - A UML diagram is included to illustrate the entity relationships.

4. **Data Analysis & Visualization**
   - Perform SQL-based analytics, such as:
     - Top 10 scorers
     - Most decisive players (goals + assists)
     - Team rankings and defensive performance
     - Discipline statistics (cards)
     - Nationality distribution
   - Display insights in a **Streamlit dashboard** with interactive filters and downloadable data.

---

## 📊 Dashboard Features
- **Dynamic Visualizations:** View top scorers, goals per team, rankings, and discipline data.
- **Interactive Filters:** Filter by team, player, or season.
- **Download Option:** Export filtered datasets as CSV.
- **Responsive Layout:** Optimized for desktop and tablet use.

---

## 🛠️ Tech Stack

| Layer | Technologies |
|-------|---------------|
| **Scraping** | Python, Selenium |
| **Data Cleaning** | Pandas, NumPy |
| **Database** | PostgreSQL, SQLAlchemy |
| **Analysis** | SQL, Pandas |
| **Visualization** | Streamlit, Matplotlib, Plotly |
| **Version Control** | Git, GitHub |

---

## 📁 Project Structure

predictive-football-analytics/
│
├── data/
│ ├── raw/ # Raw scraped CSVs
│ ├── cleaned/ # Cleaned & transformed data
│
├── scripts/
│ ├── scrape_fbref.py # Selenium scraper
│ ├── clean_data.py # Data cleaning and transformation
│ ├── load_to_db.py # PostgreSQL ingestion script
│
├── sql/
│ ├── schema.sql # Database schema (DDL)
│ ├── analytics_queries.sql # Key analytical SQL queries
│
├── dashboard/
│ ├── app.py # Streamlit dashboard main file
│
├── uml/
│ ├── data_model.uml # UML diagram of the database
│
├── requirements.txt # Python dependencies
├── README.md # Project documentation
└── .gitignore


---

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/predictive-football-analytics.git
cd predictive-football-analytics

2. **Create and activate a virtual environment**
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate

3. **Install dependencies**
pip install -r requirements.txt

4. **Configure PostgreSQL**

Create a database named football_db.

Update your database credentials in:

scripts/load_to_db.py

dashboard/app.py

5. **Run the data pipeline**
python scripts/scrape_fbref.py     # Collect raw data
python scripts/clean_data.py       # Clean and standardize
python scripts/load_to_db.py       # Load into PostgreSQL

6. **Launch the Streamlit dashboard**
streamlit run app.py

## 📈 Example Insights
Metric	Description
Top Scorers	Displays the top 10 players with the most goals.
Most Decisive Players	Combines goals and assists to identify key contributors.
Team Ranking	Based on total points (Win = 3, Draw = 1).
Defensive Strength	Ranks teams by goals conceded.
Discipline Analysis	Highlights players with most yellow/red cards.
Nationality Distribution	Visual breakdown of player nationalities per team.
