import pandas as pd

def remove_and_recalculate(league, year, name):
    ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
    borda_path = f'./data/baseball/processed_data/mvp_official_results_by_year/{league}_{year - 2000}.csv'

    ballots = pd.read_csv(ballot_path)
    borda_results = pd.read_csv(borda_path, index_col="Player")
    
    # Points mapping for each rank
    rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    
    # Iterate through each row in ballots
    for _, row in ballots.iterrows():
        if name in row.values:
            # Find the index of the player to remove
            player_idx = list(row.values).index(name)
            
            # Add points to each subsequent player
            for i in range(player_idx + 1, 15):
                player_name = row[i]
                points_to_add = rank_points[i - 6] - rank_points[i - 5]
                borda_results.loc[player_name, 'Borda Points'] += points_to_add
    
    # Remove the player from the Borda results
    if name in borda_results.index:
        borda_results = borda_results.drop(name)
    
    return borda_results.reset_index()


