import requests
import csv

def flatten_player_data(player):
    flattened_data = {}
    
    for key, value in player.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flattened_data[f"{key}_{nested_key}"] = nested_value
        else:
            flattened_data[key] = value
            
    return flattened_data


def contains_any_substring(string, substrings):
    return any(substring in string for substring in substrings)

def main():
    url = "https://api.sportradar.com/nfl/official/production/v7/en/seasons/2023/REG/teams/0d855753-ea21-4953-89f9-0e20aff9eb73/statistics.json?api_key=29hXZTAVUBfTx8poEw2A8FlVylvbPGj3CnPPB7F9"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    raw_data = response.json()
    raw_NO_player_data = raw_data["players"]

    all_headers = set()

    flattened_players = []
    for player in raw_NO_player_data:
        if player.get("games_played") == 0 or player.get("position") not in ["QB", "RB", "WR", "TE"]:
            continue
        flattened_player = flatten_player_data(player)
        flattened_players.append(flattened_player)
        all_headers.update(flattened_player.keys())

    # First n columns to be displayed
    preferred_order = ["id", "name", "position", "sr_id", "games_played", "games_started"]
    
    remaining_headers = sorted([header for header in all_headers if header not in preferred_order])
    offensive_headers = [header for header in remaining_headers if contains_any_substring(header, ["return", "receiving", "passing", "rushing", "penalties"])]
    all_headers = preferred_order + offensive_headers

    with open('team_1_player_data.csv', 'w', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=all_headers)
        csv_writer.writeheader()

        new_flattened_players = []

        for flattened_player in flattened_players:
            new_flattened_player = flattened_player.copy()
            for column in flattened_player.keys():
                if column not in all_headers:
                    new_flattened_player.pop(column)
            new_flattened_players.append(new_flattened_player)

        for flattened_player in new_flattened_players:
            complete_player_data = {header: flattened_player.get(header, 0) for header in all_headers}
            csv_writer.writerow(complete_player_data)

if __name__ == "__main__":
    main()
