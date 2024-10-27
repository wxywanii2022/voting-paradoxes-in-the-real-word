import pandas as pd
from itertools import combinations
# run multiple tasks in parallel
from concurrent.futures import ProcessPoolExecutor

# Precompute rank points difference for efficiency
rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]
rank_diff = {i: rank_points[i] - rank_points[i + 1] for i in range(len(rank_points) - 1)}

def remove_and_recalculate(league, year, names, ballots=None, borda_results=None):
    """
    Avoid Repeated I/O Operations because reading the CSV file each time is costly. 
    Our approach is to load the file once and pass it as a parameter.
    """
    if ballots is None:
        ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
        ballots = pd.read_csv(ballot_path)
    
    if borda_results is None:
        borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
        borda_results = pd.read_csv(borda_path, index_col="Player")

    """
    DataFrame operations are slow in large data sets, so minimizing row-level operations will help. 
    Therefore, when recalculating points for each player_name, storing the changes in a dictionary 
    and updating the DataFrame in bulk at the end to improve speed.
    """

    # Dictionary to store point adjustments    
    points_adjustments = {}

    try:
        for name in names:
            for _, row in ballots.iterrows():
                if name in row.values:
                    player_idx = list(row.values).index(name)
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
        if player_name in borda_results.index:
            borda_results.loc[player_name, 'Borda Points'] += points

    # Remove each player from the Borda results
    borda_results.drop(names, inplace=True, errors='ignore')
    
    return borda_results.reset_index()


def detect_IIA_specific(league, year, start_index, end_index, removal_amount):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
    official_borda_results = pd.read_csv(borda_path)
    official_borda_results['Rank'] = range(1, len(official_borda_results) + 1)

    target_players = list(official_borda_results.iloc[start_index-1:end_index]['Player'])
    players_outside_range = list(official_borda_results.loc[~official_borda_results['Player'].isin(target_players), 'Player'])
    output_data = []

    for player_combo in combinations(players_outside_range, removal_amount):
        try:
            new_borda_results = remove_and_recalculate(league, year, list(player_combo), borda_results=official_borda_results.copy())

            removed_player_ranks = [official_borda_results[official_borda_results['Player'] == player]['Rank'].iloc[0] for player in player_combo]
            adjust_start_index = start_index - sum(1 for rank in removed_player_ranks if rank < start_index)
            adjust_end_index = end_index - sum(1 for rank in removed_player_ranks if rank < end_index)

            new_target_players = list(new_borda_results.iloc[adjust_start_index-1:adjust_end_index]['Player'])
            
            if new_target_players != target_players:
                new_borda_results['Rank'] = range(1, len(new_borda_results) + 1)
                new_ranks_of_target_players = [new_borda_results[new_borda_results['Player'] == p]['Rank'].iloc[0] for p in target_players]
                original_ranks_of_new_players = [official_borda_results[official_borda_results['Player'] == p]['Rank'].iloc[0] for p in new_target_players]

                output_data.append({
                    "Year": year,
                    "League": league,
                    "Removed-Players": player_combo,
                    "RP-Ranking": tuple(removed_player_ranks),
                    f"Official-Range-{start_index}-to-{end_index}": tuple(target_players),
                    "New-Ranking": tuple(new_ranks_of_target_players),
                    f"New-Range-{adjust_start_index}-to-{adjust_end_index}": tuple(new_target_players),
                    "Original-Ranking": tuple(original_ranks_of_new_players)
                })
        except KeyError as e:
            print(f"Key error in detect_IIA_specific: {e}")
        except Exception as e:
            print(f"Unexpected error in detect_IIA_specific: {e} {year} {league} {player_combo}")

    output_df = pd.DataFrame(output_data)
    return output_df

"""
Multiprocessing on Windows requires all functions to be defined at the top level of the module 
(not within another function) so that they can be pickled and shared across processes.
"""
def process_year_league(league, year, start_index, end_index, removal_amount):
    return detect_IIA_specific(league, year, start_index, end_index, removal_amount)


def detect_IIA_all(start_index, end_index, removal_amount):
    all_data = []

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_year_league, league, year, start_index, end_index, removal_amount)
            for year in range(2012, 2024)
            for league in ["AL", "NL"]
        ]
        for future in futures:
            try:
                result_df = future.result()
                if not result_df.empty:
                    all_data.append(result_df)
            except Exception as e:
                print(f"Error processing a year/league combo: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(f"./src/baseball/Borda/borda_IIA_range_{start_index}_to_{end_index}.csv", index=False)
        print(f"Data saved to borda_IIA_range_{start_index}_to_{end_index}.csv")
    else:
        print("No data to save.")

# multiprocessing requires that the main entry point of the script be protected
if __name__ == '__main__':
    detect_IIA_all(1, 5, 2)
