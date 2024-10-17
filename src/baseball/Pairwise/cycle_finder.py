import pandas as pd
import itertools

def preprocess_pairwise_data(pairwise_df):
    """
    Preprocess the pairwise DataFrame to create a dictionary
    for faster lookups.
    """
    pairwise_dict = {}

    for _, row in pairwise_df.iterrows():
        player_a = row['PlayerA']
        player_b = row['PlayerB']
        a_b = (row['A>B'], row['B>A'])
        pairwise_dict[(player_a, player_b)] = a_b
        pairwise_dict[(player_b, player_a)] = (a_b[1], a_b[0])  # Reverse the order for (B, A)

    return pairwise_dict


def get_player_rankings(ranking_file):
    """
    Read the player rankings from a CSV file.
    Returns a dictionary mapping players to their rank and points.
    """
    ranking_df = pd.read_csv(ranking_file)
    ranking_df['Rank'] = ranking_df.index + 1  # Rank is based on the row number (1-indexed)
    ranking_dict = {row['Player']: {'Rank': row['Rank'], 'Points': row['Borda Points']}
                    for _, row in ranking_df.iterrows()}
    return ranking_dict


def cycle_finder_specific(league, year):
    # Read the nominees name list
    data_path = f"./data/baseball/processed_data/auxiliary_files/mvp_nominees_by_year/mvp_nominees_{year}_{league}.csv"
    df = pd.read_csv(data_path)
    
    # Extract the player names from the 'Player' column
    name_list = df['Player'].tolist()

    # Generate all possible 3-name combinations, sorted
    combinations = [sorted(combo) for combo in itertools.combinations(name_list, 3)]

    # Read the pairwise data CSV file (PlayerA, PlayerB, A>B, B>A)
    pairwise_path = f"./src/baseball/Pairwise/pairwise_results/{year} {league}.csv"
    pairwise_df = pd.read_csv(pairwise_path)
    
    # Preprocess the pairwise data into a dictionary for faster lookups
    pairwise_dict = preprocess_pairwise_data(pairwise_df)

    # Read player rankings (Borda Points)
    ranking_file = f"./data/baseball/processed_data/mvp_official_results_by_year/{league}_{year - 2000}.csv"
    player_rankings = get_player_rankings(ranking_file)
    
    # Iterate through each 3-player combination
    valid_combinations = []
    for combo in combinations:
        a, b, c = combo

        # Look up the results for each pair from the preprocessed dictionary
        try:
            a_b_result = pairwise_dict[(a, b)]
            a_c_result = pairwise_dict[(a, c)]
            b_c_result = pairwise_dict[(b, c)]
        except KeyError:
            continue  # Skip this combination if any pair data is missing

        # Check the cycle conditions
        if ((a_b_result[0] > a_b_result[1] and  # A > B
            b_c_result[0] > b_c_result[1] and  # B > C
            a_c_result[1] > a_c_result[0]) or  # C > A
            (a_b_result[1] > a_b_result[0] and  # A < B
            b_c_result[1] > b_c_result[0] and  # B < C
            a_c_result[0] > a_c_result[1])):    # C < A
            
            # Get the rankings and points of the players
            a_rank = player_rankings.get(a, {'Rank': 'N/A', 'Points': 'N/A'})
            b_rank = player_rankings.get(b, {'Rank': 'N/A', 'Points': 'N/A'})
            c_rank = player_rankings.get(c, {'Rank': 'N/A', 'Points': 'N/A'})

            valid_combinations.append({
                'Year': year,
                'League': league,
                'Combo': f'{a}, {b}, {c}',
                'Rankings': f'{a_rank["Rank"]}, {b_rank["Rank"]}, {c_rank["Rank"]}',
                'ab': f'({a} {b})',
                'a>b': f'{a_b_result[0]}',
                'b>a': f'{a_b_result[1]}',
                'bc': f'({b} {c})', 
                'b>c': f'{b_c_result[0]}', 
                'c>b': f'{b_c_result[1]}', 
                'ca': f'({c} {a})', 
                'c>a': f'{a_c_result[1]}', 
                'a>c': f'{a_c_result[0]}'
                # 'RankA': a_rank['Rank'],
                # 'PointsA': a_rank['Points'],
                # 'RankB': b_rank['Rank'],
                # 'PointsB': b_rank['Points'],
                # 'RankC': c_rank['Rank'],
                # 'PointsC': c_rank['Points']
            })

    # Convert the valid combinations to a DataFrame
    valid_combos_df = pd.DataFrame(valid_combinations)
    return valid_combos_df



def cycle_finder_all():
    years = range(2012, 2024)  
    leagues = ["AL", "NL"] 

    all_results_df = pd.DataFrame()

    for year in years:
        for league in leagues:
            print(f"Processing year {year}, league {league}...")
            result_df = cycle_finder_specific(league, year)
            all_results_df = pd.concat([all_results_df, result_df], ignore_index=True)

    # Save the combined DataFrame to a CSV file with a clean format
    all_results_df.to_csv("./src/baseball/Pairwise/cycles.csv", index=False)
    print("All data has been processed and saved")


def cycle_finder_filter(cutoff):
    # Read the cycles
    data_path = f"./src/baseball/Pairwise/cycles.csv"
    df = pd.read_csv(data_path)

    # Filter the DataFrame based on the condition
    filtered_df = df[
        (df['ab-a'] + df['ab-b'] >= cutoff) &
        (df['bc-b'] + df['bc-c'] >= cutoff) &
        (df['ca-c'] + df['ca-a'] >= cutoff)
    ]

    output_path = f"./src/baseball/Pairwise/cycles_cutoff{cutoff}.csv"
    filtered_df.to_csv(output_path, index=False)

    print(f"Filtered data saved to {output_path}")


cycle_finder_all()

# cycle_finder_filter(10)
