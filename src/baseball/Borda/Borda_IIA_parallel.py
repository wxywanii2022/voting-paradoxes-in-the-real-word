import pandas as pd
from itertools import combinations
# Run multiple tasks in parallel
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import time


rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]

# Precompute rank points difference for efficiency
# rank_diff = {i: rank_points[i] - rank_points[i + 1] for i in range(len(rank_points) - 1)}

def remove_and_recalculate(league, year, names_to_remove, ballots=None, borda_results=None):
    """
    Avoid Repeated I/O Operations because reading the CSV file each time is costly. 
    Our approach is to load the file once and pass it as a parameter.
    """
    if ballots is None:
        ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
        # Restrict the columns read in to be 1st, 2nd, 3rd, 4th, 5th, 6th, 7th, 8th, 9th, 10th
        ballots = pd.read_csv(ballot_path, usecols=['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th'])
    
    if borda_results is None:
        borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
        borda_results = pd.read_csv(borda_path)

    """
    When "Player" is the index, Pandas can directly access rows with loc[player_name] 
    using a highly optimized hash lookup. This makes retrieving and updating specific rows faster
    """ 
    borda_results.set_index('Player', inplace=True)

    """
    DataFrame operations are slow in large data sets, so minimizing row-level operations will help. 
    Therefore, when recalculating points for each player_name, storing the changes in a dictionary 
    and updating the DataFrame in bulk at the end to improve speed.
    """
    # Dictionary to store point adjustments, switching to collections.defaultdict(int) eliminates the need for get calls. 
    points_adjustments = defaultdict(int)

    try:
        """
        elder version:
        # Iterate through names to remove and ballots to adjust points
        for name in names_to_remove:
            for _, row in ballots.iterrows():
                if name in row.values:
                    player_idx = list(row.values).index(name)
                    # Add corresponding points for players ranked after the target, max col idx is 10
                    for i in range(player_idx + 1, 10):  
                        player_name = row.iloc[i]
                        points_to_add = rank_diff.get(i - 1, 0)
                        points_adjustments[player_name] = points_adjustments.get(player_name, 0) + points_to_add
        """
        
        """
        new version:
        We want to prevent cases like in a single row of the ballot, both 1st and 2nd are removed,
        but the the 5 points addition by removing the 1st player is added to 2nd, instead of 3rd
        """
        for _, row in ballots.iterrows():
            # Create an set to store the index of players we need to remove, allow search in O(1) time
            removed_player_idxs = {i for i, player in enumerate(row) if player in names_to_remove}

            # Iterate the each player in the row to add points based on their new index, skip the removed players 
            idx = 0 
            # idx is the new ranking and i is the original ranking
            for i in range(0, 10): 
                if i not in removed_player_idxs:
                    player_name = row.iloc[i]
                    points_adjustments[player_name] += rank_points[idx] - rank_points[i]   # new - original
                    idx += 1
    except KeyError as e:
        print(f"Key error during adjustment: {e}")
    except Exception as e:
        print(f"Unexpected error in remove_and_recalculate: {e}")
        
    # Apply the adjustments in bulk
    for player_name, points in points_adjustments.items():
        if player_name.strip() in borda_results.index:
            borda_results.loc[player_name.strip(), 'Borda Points'] += points

    # Remove players from the Borda results
    borda_results.drop(names_to_remove, inplace=True, errors='ignore')
    
    # Sort the dataframe and revert Player from index to colunm title
    borda_results = borda_results.reset_index().sort_values(by='Borda Points', ascending=False)

    return borda_results


# df2 = remove_and_recalculate("AL", 2012, ["Cabrera", "Trout", "Verlander"])
# print(df2)

# df = remove_and_recalculate("NL", 2017, ["Arenado", "Blackmon"])
# print(df)

# borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/2012_AL_14-9-8--1.csv'
# official_borda_results = pd.read_csv(borda_path)
# ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/2012_AL_votes.csv'
# ballots = pd.read_csv(ballot_path)
# official_borda_results_copy = official_borda_results.copy(deep=True)
# df = remove_and_recalculate("AL", 2012, ["Cabrera", "Trout", "Verlander"], ballots, official_borda_results_copy)
# print(df)



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
    
    # List to store the output data
    output_data = []

    # Iterate over combinations of players to be removed from the outside range
    for player_combo in combinations(players_outside_range, removal_amount):
        try:
            # Remove the selected players and recalculate the Borda results
            new_borda_results = remove_and_recalculate(league, year, list(player_combo), ballots, official_borda_results.copy())
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
        final_df.to_csv(f"./src/baseball/Borda/IIA_results/borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}.csv", index=False)
        # print(f"Data saved to borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}_sortedBy_{sort_key}.csv")
        print("Data saved.")
    else:
        print("No data to save.")



# # multiprocessing requires that the main entry point of the script be protected
# if __name__ == '__main__':

#     # target_ranks, removal_amount, max_removed_ranking, sort_key
#     detect_IIA_all([1,2,3], 1, 15, "New-Rankings")


    
# if __name__ == '__main__':
#     sort_key = "New-Rankings"

#     # Loop for removal_amount = 1, with max_removed_ranking of 15
#     removal_amount = 1
#     max_removed_ranking = 15
#     for target_count in range(3, 15 + 1):
#         target_ranks = list(range(1, target_count + 1))
        
#         start_time = time.time()
#         detect_IIA_all(target_ranks, removal_amount, max_removed_ranking, sort_key)
#         end_time = time.time()
        
#         elapsed_time = end_time - start_time
#         print(f"Calling detect_IIA_all({target_ranks}, {removal_amount}, {max_removed_ranking}) "
#               f"needs {elapsed_time:.4f} seconds")

#     # Loop for removal_amount = 2, with max_removed_ranking of 15
#     removal_amount = 2
#     max_removed_ranking = 15
#     for target_count in range(3, 15 + 1):
#         target_ranks = list(range(1, target_count + 1))
        
#         start_time = time.time()
#         detect_IIA_all(target_ranks, removal_amount, max_removed_ranking, sort_key)
#         end_time = time.time()
        
#         elapsed_time = end_time - start_time
#         print(f"Calling detect_IIA_all({target_ranks}, {removal_amount}, {max_removed_ranking}) "
#               f"needs {elapsed_time:.4f} seconds")

#     # Loop for removal_amount = 3, with max_removed_ranking of 10
#     removal_amount = 3
#     max_removed_ranking = 10
#     for target_count in range(3, 9 + 1):
#         target_ranks = list(range(1, target_count + 1))
        
#         start_time = time.time()
#         detect_IIA_all(target_ranks, removal_amount, max_removed_ranking, sort_key)
#         end_time = time.time()
        
#         elapsed_time = end_time - start_time
#         print(f"Calling detect_IIA_all({target_ranks}, {removal_amount}, {max_removed_ranking}) "
#               f"needs {elapsed_time:.4f} seconds")


"""
side notes:

1. Big O:

the big O of detect_IIA_all() is O(Y * L * C(e, r) * n), with Y being the year, L being the league, 
    e being number of non-target players, r being removal amount, n being total number of rows in ballots


2. Running time:

Data saved
Calling detect_IIA_all([1, 2, 3], 1, 15) needs 1.4466 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4], 1, 15) needs 1.4268 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5], 1, 15) needs 1.4463 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6], 1, 15) needs 1.5503 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7], 1, 15) needs 1.4480 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8], 1, 15) needs 1.5013 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9], 1, 15) needs 1.5136 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1, 15) needs 1.4339 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 1, 15) needs 1.3550 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 1, 15) needs 1.4976 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], 1, 15) needs 1.3721 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], 1, 15) needs 1.4962 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], 1, 15) needs 1.2871 seconds

Data saved
Calling detect_IIA_all([1, 2, 3], 2, 15) needs 2.9903 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4], 2, 15) needs 2.6176 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5], 2, 15) needs 2.3703 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6], 2, 15) needs 2.2257 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7], 2, 15) needs 1.9959 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8], 2, 15) needs 1.7749 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9], 2, 15) needs 1.6949 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2, 15) needs 1.5973 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 2, 15) needs 1.4037 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 2, 15) needs 1.3430 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], 2, 15) needs 1.3165 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], 2, 15) needs 1.2890 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], 2, 15) needs 1.2717 seconds

Data saved
Calling detect_IIA_all([1, 2, 3], 3, 10) needs 1.8856 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4], 3, 10) needs 1.6835 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5], 3, 10) needs 1.4270 seconds
Data saved
Calling detect_IIA_all([1, 2, 3, 4, 5, 6], 3, 10) needs 1.3485 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7], 3, 10) needs 1.2813 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8], 3, 10) needs 1.2972 seconds
No data to save.
Calling detect_IIA_all([1, 2, 3, 4, 5, 6, 7, 8, 9], 3, 10) needs 1.2526 seconds

"""