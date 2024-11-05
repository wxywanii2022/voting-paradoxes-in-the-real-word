import pandas as pd
from itertools import combinations
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import time

"""
IMPORTANT RESTRICTION: rank_points need to be consecutive, could only work on college football data

v2 preprocess the removal to avoid repetitive work
"""

# Predefined rank points
rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]

def load_players(year, league):
    """
    Load players from a file for a given year and league.
    """
    player_path = f'./data/baseball/processed_data/auxiliary_files/mvp_nominees_by_year/mvp_nominees_{year}_{league}.csv'
    return pd.read_csv(player_path)['Player'].tolist()


def preprocess_removal_effects(ballots, player_names):
    """
    Preprocess the effect of removing each player, storing the point adjustments for all other players
    when a particular player is removed.
    """
    removal_effects = {player: {} for player in player_names}  # Initialize a dict for all players

    for removed_player in player_names:
        # Loop through each row to calculate the adjustment only if the player is found in the row
        for _, row in ballots.iterrows():
            if removed_player in row.values:
                removed_idx = row.tolist().index(removed_player)

                # Calculate the adjustment for each other player in the row if `removed_player` is removed
                adjusted_points = defaultdict(int)

                for i in range(removed_idx + 1, 10):
                    player_name = row.iloc[i]
                    adjusted_points[player_name] += 1

                # Store the adjustments in `removal_effects` for this player
                removal_effects[removed_player].update(adjusted_points)

    return removal_effects


def remove_and_recalculate_optimized(league, year, names_to_remove, borda_results, removal_effects):
    """
    Use precomputed removal effects to quickly calculate the Borda points after removing players.
    """
    # Set the DataFrame index for efficient lookup and updates
    borda_results.set_index('Player', inplace=True)

    # Initialize a total adjustment map
    total_adjustments = defaultdict(int)
    for player in names_to_remove:
        # Combine adjustments for each player being removed
        for other_player, adjustment in removal_effects[player].items():
            total_adjustments[other_player] += adjustment

    # Apply all adjustments in bulk
    for player, adjustment in total_adjustments.items():
        if player in borda_results.index:
            borda_results.loc[player, 'Borda Points'] += adjustment

    # Remove the specified players from the results
    borda_results.drop(names_to_remove, inplace=True, errors='ignore')
    # Reset index and sort by Borda Points
    borda_results.reset_index().sort_values(by='Borda Points', ascending=False)

    return borda_results


player_names = load_players(2017, "NL")
ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/2017_NL_votes.csv'
ballots = pd.read_csv(ballot_path, usecols=['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th'])
removal_effects = preprocess_removal_effects(ballots, player_names)
borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/2017_NL_14-9-8--1.csv'
borda_results = pd.read_csv(borda_path)
df = remove_and_recalculate_optimized("NL", 2017, ["Arenado", "Blackmon"], borda_results.copy(), removal_effects)
print(df)


def detect_IIA_specific(league, year, target_ranks, removal_amount, max_removed_ranking):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'

    official_borda_results = pd.read_csv(borda_path)
    # Assign ranks to the players based on their position in the DataFrame
    official_borda_results['Rank'] = range(1, len(official_borda_results) + 1)

    ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
    # Restrict the columns to read in are 1st,2nd,3rd,4th,5th,6th,7th,8th,9th,10th
    ballots = pd.read_csv(ballot_path, usecols=['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th'])

    # Extract the target players based on the specified index range, df starts at index 0
    target_players = list(official_borda_results.iloc[[rank - 1 for rank in target_ranks]]['Player'])
    # Identify players who are not within the target range and filter players based on the max_removed_ranking
    players_outside_range = list(
        official_borda_results[
            (official_borda_results['Rank'] < max_removed_ranking) &
            (~official_borda_results['Player'].isin(target_players))
        ]['Player']
    )

    player_names = load_players(year, league)
    removal_effects = preprocess_removal_effects(ballots, player_names)
    
    # List to store the output data
    output_data = []

    # Iterate over combinations of players to be removed from the outside range
    for player_combo in combinations(players_outside_range, removal_amount):
        try:
            # Remove the selected players and recalculate the Borda results
            new_borda_results = remove_and_recalculate_optimized(league, year, list(player_combo), official_borda_results.copy(), removal_effects)
            # Get the ranks of the removed players from the official results
            removed_player_ranks = [official_borda_results[official_borda_results['Player'] == player]['Rank'].iloc[0] for player in player_combo]
            
            # Dictionary to store adjustments for each rank, original rank -> new rank
            adjustments = {rank: 0 for rank in target_ranks}

            # Calculate the adjustments for each target rank based on removed players
            for rank in target_ranks:
                adjustments[rank] = rank - sum(1 for r in removed_player_ranks if r < rank)

            # Identify the new target players based on the adjusted indices
            new_target_players = []
            for rank in target_ranks:
                new_target_player = new_borda_results.iloc[adjustments[rank] - 1]['Player']
                new_target_players.append(new_target_player)
            
            # Check if the new target players differ from the original target players
            if new_target_players != target_players:
                new_borda_results['Rank'] = range(1, len(new_borda_results) + 1)
                
                """
                # Retrieve the new ranks of the target players in the recalculated results
                new_ranks_of_target_players = [new_borda_results[new_borda_results['Player'] == p]['Rank'].iloc[0] for p in target_players]
                reverse_adjustments = {v: k for k, v in adjustments.items()}
                new_ranks_of_target_players_adjusted = [reverse_adjustments.get(rank, rank) for rank in new_ranks_of_target_players]
                """

                # Retrieve the original ranks of the new target players from the official results
                original_ranks_of_new_players = [official_borda_results[official_borda_results['Player'] == p]['Rank'].iloc[0] for p in new_target_players]
                
                # Append the results to the output data
                output_data.append({
                    "Year": year,
                    "League": league,
                    "Removed-Players": player_combo,
                    "RP-Ranking": tuple(removed_player_ranks),
                    "Original-Players": tuple(target_players),
                    "Original-Rankings": tuple(target_ranks),
                    # "New-Ranking": tuple(new_ranks_of_target_players_adjusted),
                    "New-Players": tuple(new_target_players),
                    "New-Rankings": tuple(original_ranks_of_new_players)
                })
        except KeyError as e:
            print(f"Key error in detect_IIA_specific: {e}")
        except Exception as e:
            print(f"Unexpected error in detect_IIA_specific: {e} {year} {league} {player_combo}")

     # Convert the output data list into a DataFrame
    output_df = pd.DataFrame(output_data)
    return output_df



def detect_IIA_all(target_ranks, removal_amount, max_removed_ranking, sort_key):
    """
    Detects IIA violations across all years and leagues, with specified player ranges and removal amounts.
    
    Args:
        target_ranks(int): the positions of players we want to consider.
        removal_amount (int): Number of irrelevant alternatives to remove during the analysis.
        max_removed_ranking: the strict upper bound for the ranking of removed players
        sort_key: the column we want to sort the final dataframe by
    """

    all_data = []

    # Use a process pool to parallelize the workload for different year and league combinations
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(detect_IIA_specific, league, year, target_ranks, removal_amount, max_removed_ranking)
            for year in range(2012, 2024)   # 2012-2023
            for league in ["AL", "NL"]
        ]
        # Collect results from each future
        for future in futures:
            try:
                # Get the result DataFrame from the future
                result_df = future.result()
                if not result_df.empty:
                    all_data.append(result_df)
            except Exception as e:
                print(f"Error processing a year/league combo: {e}")

    if all_data:
        # Combine all DataFrames into one
        final_df = pd.concat(all_data, ignore_index=True)
        # Sort the dataframe by sort_key
        final_df.sort_values(by=sort_key, ascending=False, inplace=True)
        final_df.to_csv(f"./src/baseball/Borda/borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}.csv", index=False)
        # print(f"Data saved to borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}_sortedBy_{sort_key}.csv")
        print("Data saved.")
    else:
        print("No data to save.")



# # multiprocessing requires that the main entry point of the script be protected
# if __name__ == '__main__':

#     # target_ranks, removal_amount, max_removed_ranking, sort_key
#     detect_IIA_all([1,2,3], 2, 10, "New-Rankings")


    
