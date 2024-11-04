import pandas as pd
from itertools import combinations
# Run multiple tasks in parallel
from concurrent.futures import ProcessPoolExecutor

# Precompute rank points difference for efficiency
rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]
rank_diff = {i: rank_points[i] - rank_points[i + 1] for i in range(len(rank_points) - 1)}

def remove_and_recalculate(league, year, names_to_remove, ballots=None, borda_results=None):
    """
    Avoid Repeated I/O Operations because reading the CSV file each time is costly. 
    Our approach is to load the file once and pass it as a parameter.
    """
    if ballots is None:
        ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
        ballots = pd.read_csv(ballot_path)
    
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
    # Dictionary to store point adjustments    
    points_adjustments = {}

    try:
        # Iterate through names to remove and ballots to adjust points
        for name in names_to_remove:
            for _, row in ballots.iterrows():
                if name in row.values:
                    player_idx = list(row.values).index(name)
                    # Add corresponding points for players ranked after the target, max col idx is 14
                    for i in range(player_idx + 1, 15):  
                        player_name = row.iloc[i]
                        """
                        By using 0 as the default, the code ensures that if key isn't found in dictionary, 
                        the value retrived will be 0 and nothing is modified. 
                        """ 
                        points_to_add = rank_diff.get(i - 6, 0)
                        points_adjustments[player_name] = points_adjustments.get(player_name, 0) + points_to_add
    except KeyError as e:
        print(f"Key error during adjustment: {e}")
    except Exception as e:
        print(f"Unexpected error in remove_and_recalculate: {e}")
        
    # Apply the adjustments in bulk
    for player_name, points in points_adjustments.items():
        if player_name.strip() in borda_results.index:
            #  print(f"{player_name} is in the index.")

            #  initial_points = borda_results.loc[player_name.strip(), 'Borda Points']
            borda_results.loc[player_name.strip(), 'Borda Points'] += points
            #  updated_points = borda_results.loc[player_name.strip(), 'Borda Points']
            
            #  print(f"Initial Points: {initial_points}, Updated Points: {updated_points}")
        # else:
        #     print(f"{player_name} is not in the index.")

    # Remove players from the Borda results
    borda_results.drop(names_to_remove, inplace=True, errors='ignore')
    
    # Sort the dataframe and revert Player from index to colunm title
    borda_results = borda_results.reset_index().sort_values(by='Borda Points', ascending=False)

    return borda_results


# df = remove_and_recalculate("NL", 2017, ["Arenado", "Blackmon"])
# print(df)


# df2 = remove_and_recalculate("AL", 2012, ["Cabrera", "Trout", "Verlander"])
# print(df2)

# borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/2012_AL_14-9-8--1.csv'
# official_borda_results = pd.read_csv(borda_path)
# ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/2012_AL_votes.csv'
# ballots = pd.read_csv(ballot_path)
# official_borda_results_copy = official_borda_results.copy(deep=True)
# df = remove_and_recalculate("AL", 2012, ["Cabrera", "Trout", "Verlander"], ballots, official_borda_results_copy)
# print(df)
# print("RRRRRRRRRRRRRRRRRRRRRRR")



def detect_IIA_specific(league, year, target_ranks, removal_amount, max_removed_ranking):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'

    official_borda_results = pd.read_csv(borda_path)
    # Assign ranks to the players based on their position in the DataFrame
    official_borda_results['Rank'] = range(1, len(official_borda_results) + 1)

    ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
    ballots = pd.read_csv(ballot_path)

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
                    "Original-Ranking": tuple(target_ranks),
                    # "New-Ranking": tuple(new_ranks_of_target_players_adjusted),
                    "New-Players": tuple(new_target_players),
                    "New-Ranking": tuple(original_ranks_of_new_players)
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
        print(f"Data saved to borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}.csv")
    else:
        print("No data to save.")



# multiprocessing requires that the main entry point of the script be protected
if __name__ == '__main__':

    # target_ranks, removal_amount, max_removed_ranking, sort_key
    detect_IIA_all([3, 4, 5, 6, 7], 2, 100, "New-Ranking")



"""
side note: the efficiency can be further improved by only reading necessary columns of ballots, but current
version is good enough for baseball dataset. 
"""