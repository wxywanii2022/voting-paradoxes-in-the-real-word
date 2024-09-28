import csv
from collections import defaultdict

"""
Calculate Borda points for players in a specific year and league
from a given CSV file containing MVP ballots.

Parameters:
    data_file (str): Path to the CSV file with MVP ballots.
    weights (list): List of weights assigned to player rankings.
    year (int): The year for which to calculate Borda points.
    league (str): The league ('AL' or 'NL') for which to calculate Borda points.
    output_filename(str)
"""


def borda_mvp_specific(data_file, weights, year, league, output_filename):
    # Dictionary to store the total Borda points for each player
    borda_scores = defaultdict(int)

    with open(data_file, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row

        # Iterate through each voter's ballot
        for row in reader:
            row_year = row[0]
            row_league = row[1]
            
            # Filter by the specified league and year
            if row_year == str(year) and row_league == league:
                players = row[5:15]
                for i, player in enumerate(players):
                    if player:
                        borda_scores[player] += weights[i]

    # Sort players by their total Borda points
    sorted_players = sorted(borda_scores.items(), key=lambda x: x[1], reverse=True)

    year_ = year - 2000
    output_file = f'./src/baseball/Borda/results/borda_{output_filename}/{year}_{league}_{output_filename}.csv'

    # Write the results to the output CSV
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Player', 'Borda Points'])
        writer.writerows(sorted_players)

    print(f"Borda results for {league} in {year} saved to {output_file}")


"""
Calculate Borda points for all years and both leagues.

Parameters:
    data_file (str): Path to the CSV file with MVP ballots.
    weights (list): List of weights assigned to player rankings.
"""


def borda_mvp_entire(data_file, weights, output_filename):
    for year in range(2012, 2024):
        borda_mvp_specific(data_file, weights, year, "AL", output_filename)
        borda_mvp_specific(data_file, weights, year, "NL", output_filename)

"""
Debug function to compute and display the Borda points for a specific player
in a specific year and league, along with the detailed scoring breakdown.

Parameters:
    data_file (str): Path to the CSV file with MVP ballots.
    weights (list): List of weights assigned to player rankings.
    year (int): The year for which to check the player's points.
    league (str): The league ('AL' or 'NL') for which to check the player's points.
    player_name (str): The name of the player to debug.
"""


def borda_mvp_debug(data_file, weights, year, league, player_name):
    output = 0 
    points_str = "" 
    weight_count = defaultdict(int)

    with open(data_file, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  
        for row in reader:
            row_year = row[0]
            row_league = row[1]

            if row_year == str(year) and row_league == league:
                players = row[5:15]  
                
                for i, player in enumerate(players):
                    if player == player_name:
                        output += weights[i]
                        points_str += f"{weights[i]} + "
                        weight_count[weights[i]] += 1 

    points_str = points_str.rstrip(" + ")
    print(f"{points_str} = {output}")

    print("\nWeight count map:")
    for weight, count in sorted(weight_count.items(), reverse=True):
        print(f"Weight {weight}: {count} times")



data_file = './data/baseball/processed_data/mvp_ballots_all/mvp_ballots_v1.csv'
weights = [1, 1/2, 1/3, 1/4, 1/5, 1/6, 1/7, 1/8, 1/9, 1/10]  

# borda_mvp_specific(data_file, weights, 2012, "AL")

borda_mvp_entire(data_file, weights, "Dowdall")

# borda_mvp_debug(data_file, weights, 2012, "AL", "Jeter")
