import requests
import csv
from typing import List
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine, text
import os


API_KEY = "29hXZTAVUBfTx8poEw2A8FlVylvbPGj3CnPPB7F9"

def main():
    # fetch_schedules((2023, 2024))
    # game_ids = open("nfl_schedule.txt", "r").read().splitlines()
    # collect_team_stats(game_ids=game_ids, api_key=API_KEY)
    # teams = get_teams()
    db_conn_str = "postgresql://postgres:tobi@localhost:5432/nfl"
    # for team in teams:
    #     print("processing team", team[1])
    #     process_team_stats(team[0], db_conn_str)
    compile_offensive_stats(db_conn_str)
    compile_defensive_stats(db_conn_str)

def fetch_schedules(season_range: tuple) -> List[str]:
    with open("nfl_schedule.txt", "w") as f:
        for season in range(season_range[0], season_range[1] + 1):
            url = f"https://api.sportradar.com/nfl/official/production/v7/en/games/{season}/REG/schedule.json?api_key={API_KEY}"
            headers = {"accept": "application/json"}
            resp = requests.get(url, headers=headers)

            if resp.status_code == 200:
                response = resp.json()
                weeks = response.get("weeks", [])
                for week in weeks:
                    for game in week.get("games", []):
                        f.write(f"{game.get('id')}\n")
            else:
                print(f"Failed to fetch schedule for season {season}")

def get_teams() -> List[str]:

    engine = create_engine("postgresql://postgres:tobi@localhost:5432/nfl")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name FROM team"))
    
    return [(row[0], row[1]) for row in result.fetchall()]


    

def collect_team_stats(game_ids: List[str], api_key: str) -> None:
    # Define the headers for both offensive and defensive stats
    offensive_headers = ["game_id", "air_yards", "avg_yards", "broken_tackles", "catchable_passes", "dropped_passes", 
                         "first_downs", "longest", "longest_touchdown", "redzone_targets", "receptions", 
                         "targets", "touchdowns", "yards", "yards_after_catch", "yards_after_contact", 
                         "ppr_points", "standard_points", "half_ppr_points"]
                         
    defensive_headers = ["game_id", "batted_passes", "blitzes", "def_comps", "def_targets", "forced_fumbles", 
                         "fourth_down_stops", "fumble_recoveries", "hurries", "interceptions", "knockdowns", 
                         "missed_tackles", "passes_defenced", "sacks", "safeties", "tackles", 
                         "three_and_outs_forced", "tloss", "tloss_yards", 
                         "ppr_allowed", "half_ppr_allowed", "standard_allowed"]

    # Loop through each game_id and collect stats
    for game_id in game_ids:
        url = f"https://api.sportradar.com/nfl/official/production/v7/en/games/{game_id}/statistics.json?api_key={api_key}"
        headers = {"accept": "application/json"}
        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            response = resp.json()

            home_team = response.get("statistics").get("home", {}).get("id")
            away_team = response.get("statistics").get("away", {}).get("id")
            
            if home_team and away_team:
                print(f"Processing game_id: {game_id} between {home_team} and {away_team}")

                # Extract offensive stats
                home_offensive_stats = response.get("statistics", {}).get("home", {}).get("receiving", {}).get("totals", {})
                away_offensive_stats = response.get("statistics", {}).get("away", {}).get("receiving", {}).get("totals", {})

                # Extract defensive stats
                home_defensive_stats = response.get("statistics", {}).get("home", {}).get("defense", {}).get("totals", {})
                away_defensive_stats = response.get("statistics", {}).get("away", {}).get("defense", {}).get("totals", {})

                # Build dictionaries for each team (including game_id)
                home_offensive = {header: home_offensive_stats.get(header, 0) for header in offensive_headers[1:]}
                away_offensive = {header: away_offensive_stats.get(header, 0) for header in offensive_headers[1:]}
                home_defensive = {header: home_defensive_stats.get(header, 0) for header in defensive_headers[1:]}
                away_defensive = {header: away_defensive_stats.get(header, 0) for header in defensive_headers[1:]}
                
                home_offensive["game_id"] = game_id
                away_offensive["game_id"] = game_id
                home_defensive["game_id"] = game_id
                away_defensive["game_id"] = game_id

                # Calculate additional scoring points
                home_offensive["ppr_points"] = home_offensive_stats.get("receptions", 0) + home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6
                home_offensive["half_ppr_points"] = home_offensive_stats.get("receptions", 0) * 0.5 + home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6
                home_offensive["standard_points"] = home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6

                away_offensive["ppr_points"] = away_offensive_stats.get("receptions", 0) + away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6
                away_offensive["half_ppr_points"] = away_offensive_stats.get("receptions", 0) * 0.5 + away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6
                away_offensive["standard_points"] = away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6

                # Add defensive scoring info
                home_defensive["ppr_allowed"] = away_offensive_stats.get("receptions", 0) + away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6
                home_defensive["half_ppr_allowed"] = away_offensive_stats.get("receptions", 0) * 0.5 + away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6
                home_defensive["standard_allowed"] = away_offensive_stats.get("yards", 0) * 0.1 + away_offensive_stats.get("touchdowns", 0) * 6

                away_defensive["ppr_allowed"] = home_offensive_stats.get("receptions", 0) + home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6
                away_defensive["half_ppr_allowed"] = home_offensive_stats.get("receptions", 0) * 0.5 + home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6
                away_defensive["standard_allowed"] = home_offensive_stats.get("yards", 0) * 0.1 + home_offensive_stats.get("touchdowns", 0) * 6

                # Append stats to the CSV files for both teams
                append_to_csv(f"{home_team}_offensive_stats.csv", offensive_headers, [home_offensive])
                append_to_csv(f"{away_team}_offensive_stats.csv", offensive_headers, [away_offensive])
                append_to_csv(f"{home_team}_defensive_stats.csv", defensive_headers, [home_defensive])
                append_to_csv(f"{away_team}_defensive_stats.csv", defensive_headers, [away_defensive])


# Helper function to append to CSV
def append_to_csv(file_name, headers, data_list):
    file_exists = os.path.isfile(file_name)
    with open(file_name, "a", newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
        if not file_exists:
            csv_writer.writeheader()  # Write header only if file doesn't exist
        csv_writer.writerows(data_list)  # Append the new rows

# Function to read offensive stats and normalize for a single team
def read_offensive_stats(team_id: str) -> pd.DataFrame:
    # Load the offensive stats CSV for the team
    try:
        offensive_df = pd.read_csv(f"{team_id}_offensive_stats.csv")
    except FileNotFoundError:
        return pd.DataFrame()
    offensive_df = offensive_df.drop(columns=["game_id"])  # Drop the game_id column
    # Normalize the offensive stats for the individual team
    scaler = MinMaxScaler()
    offensive_normalized = pd.DataFrame(scaler.fit_transform(offensive_df), columns=offensive_df.columns)
    
    return offensive_normalized

# Function to read defensive stats and normalize for a single team
def read_defensive_stats(team_id: str) -> pd.DataFrame:
    # Load the defensive stats CSV for the team
    try:
        defensive_df = pd.read_csv(f"{team_id}_defensive_stats.csv")
    except FileNotFoundError:
        return pd.DataFrame()
    defensive_df = defensive_df.drop(columns=["game_id"])  # Drop the game_id column
    # Normalize the defensive stats for the individual team
    scaler = MinMaxScaler()
    defensive_normalized = pd.DataFrame(scaler.fit_transform(defensive_df), columns=defensive_df.columns)
    
    return defensive_normalized


# Calculate the ranking score for a single team's stats
def calculate_ranking_score(stats_df: pd.DataFrame) -> pd.Series:
    # Calculate the mean of the normalized stats for ranking
    ranking_score = stats_df.mean(axis=1)
    
    return ranking_score


def save_ranking_to_db(team_id: str, ranking_scores: pd.Series, db_conn_str: str, stat_type: str) -> None:
    # Create a DataFrame with team_id and a single rating
    ranking_df = pd.DataFrame({
        "team_id": [team_id],
        f"{stat_type}_rating": ranking_scores.values
    })

    # Create a connection to PostgreSQL using SQLAlchemy engine
    engine = create_engine(db_conn_str)

    # Save to the database (assumes table already exists)
    table_name = f"team_{stat_type}_rating"
    
    with engine.connect() as conn:
        ranking_df.to_sql(table_name, con=conn, if_exists='append', index=False)




def process_team_stats(team_id: str, db_conn_str: str) -> None:
    # Read and rank offensive stats for the team
    offensive_stats = read_offensive_stats(team_id)
    if not offensive_stats.empty:
        offensive_ranking = calculate_ranking_score(offensive_stats)
        offensive_average = offensive_ranking.mean()  # Get a single average rating
        save_ranking_to_db(team_id, pd.Series([offensive_average]), db_conn_str, 'offensive')
    
    # Read and rank defensive stats for the team
    defensive_stats = read_defensive_stats(team_id)
    if not defensive_stats.empty:
        defensive_ranking = calculate_ranking_score(defensive_stats)
        defensive_average = defensive_ranking.mean()  # Get a single average rating
        save_ranking_to_db(team_id, pd.Series([defensive_average]), db_conn_str, 'defensive')


def compile_offensive_stats(db_conn_str: str):
    # Create a list to hold all data
    all_offensive_data = []

    # Iterate over each team
    teams = get_teams()
    for team in teams:
        team_id = team[0]
        try:
            # Read the offensive stats CSV for the team
            offensive_df = pd.read_csv(f"{team_id}_offensive_stats.csv")
            offensive_df['team_id'] = team_id  # Add team_id for reference
            all_offensive_data.append(offensive_df)
        except FileNotFoundError:
            print(f"No offensive stats found for team {team[1]}")

    # Concatenate all DataFrames into one
    combined_offensive_df = pd.concat(all_offensive_data, ignore_index=True)

    # Save to the database
    engine = create_engine(db_conn_str)
    with engine.connect() as conn:
        combined_offensive_df.to_sql('team_offensive_stats', con=conn, if_exists='replace', index=False)
    print("Offensive stats compiled and saved to team_offensive_stats.")


def compile_defensive_stats(db_conn_str: str):
    # Create a list to hold all data
    all_defensive_data = []

    # Iterate over each team
    teams = get_teams()
    for team in teams:
        team_id = team[0]
        try:
            # Read the defensive stats CSV for the team
            defensive_df = pd.read_csv(f"{team_id}_defensive_stats.csv")
            defensive_df['team_id'] = team_id  # Add team_id for reference
            all_defensive_data.append(defensive_df)
        except FileNotFoundError:
            print(f"No defensive stats found for team {team[1]}")

    # Concatenate all DataFrames into one
    combined_defensive_df = pd.concat(all_defensive_data, ignore_index=True)

    # Save to the database
    engine = create_engine(db_conn_str)
    with engine.connect() as conn:
        combined_defensive_df.to_sql('team_defensive_stats', con=conn, if_exists='replace', index=False)
    print("Defensive stats compiled and saved to team_defensive_stats.")




if __name__ == "__main__":
    main()
