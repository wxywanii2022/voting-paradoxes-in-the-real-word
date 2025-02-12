import pandas as pd
from itertools import combinations

"""
This script performs pairwise comparisons of MVP nominees based on their rankings in the voting data.
It generates results that indicate how often one player was ranked above another, following a 
Condorcet method.

Input: CSV files with MVP ballot data, including player rankings ('1st' to '10th') and nominee names.

Output: 
1. Pairwise comparison results for all players, saved as CSV files by year and league.
2. Pairwise comparison results for specific players, saved as a CSV file by year, league, and player list.
"""

def pairwise_comparison(year, league):
    player_df = pd.read_csv(f"./data/baseball/processed_data/auxiliary_files/mvp_nominees_by_year/mvp_nominees_{year}_{league}.csv")
    players = player_df['Player'].tolist()

    pairwise_counts = {(min(p1, p2), max(p1, p2)): [0, 0] for p1, p2 in combinations(players, 2)}

    ballot_df = pd.read_csv(f"./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv") 

    ranking_columns = ['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th']

    # Iterate over each row of ballots
    for _, row in ballot_df.iterrows():
        rankings = row[ranking_columns].tolist()
        # Loop through all combinations in pairwise_counts
        for (p1, p2) in pairwise_counts:
            if p1 in rankings and p2 in rankings:
                if rankings.index(p1) < rankings.index(p2):
                    pairwise_counts[(p1, p2)][0] += 1
                else:
                    pairwise_counts[(p1, p2)][1] += 1
            elif p1 in rankings and p2 not in rankings:
                pairwise_counts[(p1, p2)][0] += 1
            elif p2 in rankings and p1 not in rankings:
                pairwise_counts[(p1, p2)][1] += 1

    output = []
    for (p1, p2), counts in pairwise_counts.items():
        output.append(f"{p1},{p2},{counts[0]},{counts[1]}")

    with open(f"./src/baseball/Pairwise/pairwise_results/{year} {league}.csv", 'w') as f:
        f.write("PlayerA,PlayerB,A>B,B>A\n")
        f.write("\n".join(output))

    print(f"{year} {league} Pairwise comparison results saved")


def pairwise_comparison_all():
    for year in range(2012, 2024):
        pairwise_comparison(year, "AL")
        pairwise_comparison(year, "NL")


def pairwise_comparison_specific(year, league, name_list):
    players = list(name_list)

    pairwise_counts = {(min(p1, p2), max(p1, p2)): [0, 0] for p1, p2 in combinations(players, 2)}

    ballot_df = pd.read_csv(f"./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv") 

    ranking_columns = ['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th']

    for _, row in ballot_df.iterrows():
        rankings = row[ranking_columns].tolist()
        for p1, p2 in combinations(rankings, 2):
            if (min(p1, p2), max(p1, p2)) in pairwise_counts:
                p1, p2 = sorted([p1, p2])
                if rankings.index(p1) < rankings.index(p2):
                    pairwise_counts[(p1, p2)][0] += 1
                else: 
                    pairwise_counts[(p1, p2)][1] += 1

    output = []
    for (p1, p2), counts in pairwise_counts.items():
        output.append(f"{p1},{p2},{counts[0]},{counts[1]}")

    with open(f"./src/baseball/Pairwise/pairwise_results/{year} {league} {name_list}.csv", 'w') as f:
        f.write("PlayerA,PlayerB,A>B,B>A\n")
        f.write("\n".join(output))

    print(f"{year} {league} Pairwise comparison results saved")
    

# pairwise_comparison(2012, "AL")
        
pairwise_comparison_all()
    
# pairwise_comparison_specific(2012, "AL", ["Beltre","Cabrera"])