import pandas as pd

def remove_and_recalculate(league, year, name):
    ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'

    ballots = pd.read_csv(ballot_path)
    borda_results = pd.read_csv(borda_path, index_col="Player")
    
    rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    
    # Iterate through each row in ballots
    try:
        for _, row in ballots.iterrows():
            if name in row.values:
                # Find the index of the player to remove
                player_idx = list(row.values).index(name)
                
                # Add points to each subsequent player
                for i in range(player_idx + 1, 15):
                    player_name = row.iloc[i]
                    points_to_add = rank_points[i - 6] - rank_points[i - 5]
                    borda_results.loc[player_name, 'Borda Points'] += points_to_add
    except KeyError as e:
        print(f"Key error during adjustment: {e}")
    except Exception as e:
        print(f"Unexpected error in remove_and_recalculate: {e}")

    # Remove the player from the Borda results
    if name in borda_results.index:
        borda_results.drop(name, inplace=True)
    
    return borda_results.reset_index()

# df = remove_and_recalculate("AL", 2012, "Cabrera")
# print(df)


def detect_IIA_specific(league, year, start_index, end_index):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
    
    official_borda_results = pd.read_csv(borda_path)
    official_borda_results['Rank'] = range(1, len(official_borda_results) + 1)

    # Select players within the specified index range
    target_players = list(official_borda_results.iloc[start_index-1:end_index]['Player'])
    
    output_data = []

    # Remove players outside the specified index range and check impact on target players
    try:
        for _, row in official_borda_results.iterrows():
            player_name = row['Player']
            player_rank = row['Rank']
            
            # Skip removal of players within the target range
            if player_name in target_players:
                continue
            
            # Get new Borda results after removing the current player
            new_borda_results = remove_and_recalculate(league, year, player_name)
            
            # Adjust the target range based on the removed player's position
            if player_rank < start_index:
                # Adjust indices if removed player is before the start of the target range
                new_target_players = list(new_borda_results.iloc[start_index-2:end_index-1]['Player'])
            else:
                # Keep the target range unchanged if the removed player is outside the target range
                new_target_players = list(new_borda_results.iloc[start_index-1:end_index]['Player'])
            
            # Check if new ranking of target players has changed
            if new_target_players != target_players:
                new_borda_results['Rank'] = range(1, len(new_borda_results) + 1)

                # Calculate the new ranks of target players after removal
                new_ranks_of_target_players = [
                    new_borda_results[new_borda_results['Player'] == p]['Rank'].iloc[0]
                    for p in new_target_players
                ]

                # Calculate the original ranks of the newly ranked players in the new target list
                original_ranks_of_new_players = [
                    official_borda_results[official_borda_results['Player'] == p]['Rank'].iloc[0]
                    for p in new_target_players
                ]

                output_data.append({
                    "Year": year,
                    "League": league,
                    "Removed-Player": player_name,
                    "RP-Ranking": player_rank,
                    f"Official-Range-{start_index}-to-{end_index}": tuple(target_players),
                    "New-Ranking": tuple(new_ranks_of_target_players),
                    f"New-Range-{start_index}-to-{end_index}": tuple(new_target_players),
                    "Original-Ranking": tuple(original_ranks_of_new_players)
                })
    except KeyError as e:
        print(f"Key error in detect_IIA_specific: {e}")
    except Exception as e:
        print(f"Unexpected error in detect_IIA_specific: {e} {year} {league} {player_name}")

    output_df = pd.DataFrame(output_data)
    return output_df



def detect_IIA_all(start_index, end_index):
    all_data = []
    for year in range(2012, 2024):
        for league in ["AL", "NL"]:
            try:
                # Pass the start and end index to detect_IIA_specific
                result_df = detect_IIA_specific(league, year, start_index, end_index)
                if not result_df.empty:
                    all_data.append(result_df)
            except Exception as e:
                print(f"Error processing {league} {year}: {e}")
    
    # Concatenate all collected DataFrames into a single DataFrame
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(f"./src/baseball/Borda/borda_IIA_range_{start_index}_to_{end_index}.csv", index=False)
        print(f"Data saved to borda_IIA_range_{start_index}_to_{end_index}.csv")
    else:
        print("No data to save.")


# detect_IIA_all(5, 9)
        



