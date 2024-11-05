import pandas as pd
from itertools import combinations
from concurrent.futures import ProcessPoolExecutor

rank_points = [14, 9, 8, 7, 6, 5, 4, 3, 2, 1]
rank_diff = {i: rank_points[i] - rank_points[i + 1] for i in range(len(rank_points) - 1)}

def remove_and_recalculate(league, year, names_to_remove, ballots=None, borda_results=None):
    if ballots is None:
        ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
        ballots = pd.read_csv(ballot_path, usecols=['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th'])
    
    if borda_results is None:
        borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
        borda_results = pd.read_csv(borda_path)

    borda_results.set_index('Player', inplace=True)

    points_adjustments = {}

    for _, row in ballots.iterrows():
        removed_player_idxs = set()
        for name in names_to_remove:
            if name in row.values:
                removed_player_idxs.add(list(row.values).index(name))

        idx = 0
        for i in range(0, 10):
            if i not in removed_player_idxs:
                player_name = row.iloc[i]
                points_to_add = rank_points[idx] - rank_points[i]
                points_adjustments[player_name] = points_adjustments.get(player_name, 0) + points_to_add
                idx += 1

    for player_name, points in points_adjustments.items():
        if player_name.strip() in borda_results.index:
            borda_results.loc[player_name.strip(), 'Borda Points'] += points

    borda_results.drop(names_to_remove, inplace=True, errors='ignore')
    borda_results = borda_results.reset_index().sort_values(by='Borda Points', ascending=False)

    return borda_results


def detect_IIA_specific(league, year, target_ranks, removal_amount, max_removed_ranking):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
    official_borda_results = pd.read_csv(borda_path)
    official_borda_results['Rank'] = range(1, len(official_borda_results) + 1)

    ballot_path = f'./data/baseball/processed_data/mvp_ballots_by_year/{year}_{league}_votes.csv'
    ballots = pd.read_csv(ballot_path, usecols=['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th'])

    target_players = list(official_borda_results.iloc[[rank - 1 for rank in target_ranks]]['Player'])
    players_outside_range = list(
        official_borda_results[
            (official_borda_results['Rank'] < max_removed_ranking) &
            (~official_borda_results['Player'].isin(target_players))
        ]['Player']
    )

    output_data = []

    for player_combo in combinations(players_outside_range, removal_amount):
        new_borda_results = remove_and_recalculate(league, year, list(player_combo), ballots, official_borda_results.copy())
        removed_player_ranks = [official_borda_results[official_borda_results['Player'] == player]['Rank'].iloc[0] for player in player_combo]

        adjustments = {rank: 0 for rank in target_ranks}
        for rank in target_ranks:
            adjustments[rank] = rank - sum(1 for r in removed_player_ranks if r < rank)

        new_target_players = []
        for rank in target_ranks:
            new_target_player = new_borda_results.iloc[adjustments[rank] - 1]['Player']
            new_target_players.append(new_target_player)

        if new_target_players != target_players:
            new_borda_results['Rank'] = range(1, len(new_borda_results) + 1)
            original_ranks_of_new_players = [official_borda_results[official_borda_results['Player'] == p]['Rank'].iloc[0] for p in new_target_players]
            
            output_data.append({
                "Year": year,
                "League": league,
                "Removed-Players": player_combo,
                "RP-Ranking": tuple(removed_player_ranks),
                "Original-Players": tuple(target_players),
                "Original-Rankings": tuple(target_ranks),
                "New-Players": tuple(new_target_players),
                "New-Rankings": tuple(original_ranks_of_new_players)
            })

    output_df = pd.DataFrame(output_data)
    return output_df


def detect_IIA_all(target_ranks, removal_amount, max_removed_ranking, sort_key):
    all_data = []
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(detect_IIA_specific, league, year, target_ranks, removal_amount, max_removed_ranking)
            for year in range(2012, 2024)
            for league in ["AL", "NL"]
        ]
        for future in futures:
            result_df = future.result()
            if not result_df.empty:
                all_data.append(result_df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.sort_values(by=sort_key, ascending=False, inplace=True)
        final_df.to_csv(f"./src/baseball/Borda/IIA_results/borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}.csv", index=False)
        print(f"Data saved to borda_IIA_range_{target_ranks}_remove_{removal_amount}_maxRemoved_{max_removed_ranking}_sortedBy_{sort_key}.csv")


if __name__ == '__main__':
    detect_IIA_all([1, 2, 3], 2, 15, "New-Rankings")
