import csv
from collections import defaultdict

def borda_mvp_specific(data_file, weights, year, league):
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
                choices = row[5:15]
                for i, player in enumerate(choices):
                    if player:
                        borda_scores[player] += weights[i]

    # Sort players by their total Borda points
    sorted_players = sorted(borda_scores.items(), key=lambda x: x[1], reverse=True)

    year_ = year - 2000
    output_file = f'./data/baseball/processed_data/Borda/results/{league}_{year_}.csv'

    # Write the results to the output CSV
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Player', 'Borda Points'])
        writer.writerows(sorted_players)

    print(f"Borda results for {league} in {year} saved to {output_file}")


def borda_mvp_entire(data_file, weights):
    for year in range(2012, 2024):
        borda_mvp_specific(data_file, weights, year, "AL")
        borda_mvp_specific(data_file, weights, year, "NL")


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
                choices = row[5:15]  
                
                for i, player in enumerate(choices):
                    if player == player_name:
                        output += weights[i]
                        points_str += f"{weights[i]} + "
                        weight_count[weights[i]] += 1 

    points_str = points_str.rstrip(" + ")
    print(f"{points_str} = {output}")

    print("\nWeight count map:")
    for weight, count in sorted(weight_count.items(), reverse=True):
        print(f"Weight {weight}: {count} times")



data_file = './data/baseball/processed_data/entire_data/mvp_ballots_v1.csv'
weights = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]  

# borda_mvp_specific(data_file, weights, 2012, "AL")

# borda_mvp_entire(data_file, weights)

borda_mvp_debug(data_file, weights, 2012, "AL", "Jeter")
