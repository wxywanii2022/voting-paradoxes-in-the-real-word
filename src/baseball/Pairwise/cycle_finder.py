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


def cycle_finder(league, year, cycle_size):
    # Read the nominees name list
    data_path = f"./data/baseball/processed_data/auxiliary_files/mvp_nominees_by_year/mvp_nominees_{year}_{league}.csv"
    df = pd.read_csv(data_path)

    # Extract the player names from the 'Player' column
    name_list = df['Player'].tolist()

    # Generate all possible cycle-size combinations, sorted
    combinations = [sorted(combo) for combo in itertools.combinations(name_list, cycle_size)]

    # Read the pairwise data CSV file (PlayerA, PlayerB, A>B, B>A)
    pairwise_path = f"./src/baseball/Pairwise/pairwise_results/{year} {league}.csv"
    pairwise_df = pd.read_csv(pairwise_path)
    
    pairwise_dict = preprocess_pairwise_data(pairwise_df)

    # Read player rankings (Borda Points)
    ranking_file = f"./data/baseball/processed_data/mvp_official_results_by_year/{league}_{year - 2000}.csv"
    player_rankings = get_player_rankings(ranking_file)
    
    # Iterate through each combination
    valid_combinations = []
    for combo in combinations:
        # different conditions check based on cycle sizes
        if cycle_size == 3:
            a, b, c = combo
            try:
                a_b_result = pairwise_dict[(a, b)]
                a_c_result = pairwise_dict[(a, c)]
                b_c_result = pairwise_dict[(b, c)]
            except KeyError:
                continue  # Skip this combination if any pair data is missing

            # A>B, B>C, C>A OR A<B, B<C, C<A
            if ((a_b_result[0] > a_b_result[1] and  
                 b_c_result[0] > b_c_result[1] and  
                 a_c_result[1] > a_c_result[0]) or 
                (a_b_result[1] > a_b_result[0] and  
                 b_c_result[1] > b_c_result[0] and  
                 a_c_result[0] > a_c_result[1])):

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
                })
        
        elif cycle_size == 4:
            a, b, c, d = combo
            try:
                a_b_result = pairwise_dict[(a, b)]
                a_c_result = pairwise_dict[(a, c)]
                a_d_result = pairwise_dict[(a, d)]
                b_c_result = pairwise_dict[(b, c)]
                b_d_result = pairwise_dict[(b, d)]
                c_d_result = pairwise_dict[(c, d)]
            except KeyError:
                continue 

            # A>B, B>C, C>D, D>A OR A<B, B<C, C<D, D<A
            if ((a_b_result[0] > a_b_result[1] and  
                 b_c_result[0] > b_c_result[1] and  
                 c_d_result[0] > c_d_result[1] and  
                 a_d_result[1] > a_d_result[0]) or 
                (a_b_result[1] > a_b_result[0] and  
                 b_c_result[1] > b_c_result[0] and  
                 c_d_result[1] > c_d_result[0] and  
                 a_d_result[0] > a_d_result[1])):   

                a_rank = player_rankings.get(a, {'Rank': 'N/A', 'Points': 'N/A'})
                b_rank = player_rankings.get(b, {'Rank': 'N/A', 'Points': 'N/A'})
                c_rank = player_rankings.get(c, {'Rank': 'N/A', 'Points': 'N/A'})
                d_rank = player_rankings.get(d, {'Rank': 'N/A', 'Points': 'N/A'})

                valid_combinations.append({
                    'Year': year,
                    'League': league,
                    'Combo': f'{a}, {b}, {c}, {d}',
                    'Rankings': f'{a_rank["Rank"]}, {b_rank["Rank"]}, {c_rank["Rank"]}, {d_rank["Rank"]}',
                    'ab': f'({a} {b})',
                    'a>b': f'{a_b_result[0]}',
                    'b>a': f'{a_b_result[1]}',
                    'bc': f'({b} {c})', 
                    'b>c': f'{b_c_result[0]}', 
                    'c>b': f'{b_c_result[1]}',
                    'cd': f'({c} {d})',
                    'c>d': f'{c_d_result[0]}',
                    'd>c': f'{c_d_result[1]}',
                    'da': f'({d} {a})',
                    'd>a': f'{a_d_result[1]}',
                    'a>d': f'{a_d_result[0]}'
                })

        elif cycle_size == 5:
            a, b, c, d, e = combo
            try:
                a_b_result = pairwise_dict[(a, b)]
                a_c_result = pairwise_dict[(a, c)]
                a_d_result = pairwise_dict[(a, d)]
                a_e_result = pairwise_dict[(a, e)]
                b_c_result = pairwise_dict[(b, c)]
                b_d_result = pairwise_dict[(b, d)]
                b_e_result = pairwise_dict[(b, e)]
                c_d_result = pairwise_dict[(c, d)]
                c_e_result = pairwise_dict[(c, e)]
                d_e_result = pairwise_dict[(d, e)]
            except KeyError:
                continue 

            # A>B, B>C, C>D, D>E, E>A OR A<B, B<C, C<D, D<E, E<A
            if ((a_b_result[0] > a_b_result[1] and
                 b_c_result[0] > b_c_result[1] and
                 c_d_result[0] > c_d_result[1] and
                 d_e_result[0] > d_e_result[1] and
                 a_e_result[1] > a_e_result[0]) or
                (a_b_result[1] > a_b_result[0] and
                 b_c_result[1] > b_c_result[0] and
                 c_d_result[1] > c_d_result[0] and
                 d_e_result[1] > d_e_result[0] and
                 a_e_result[0] > a_e_result[1])):

                a_rank = player_rankings.get(a, {'Rank': 'N/A', 'Points': 'N/A'})
                b_rank = player_rankings.get(b, {'Rank': 'N/A', 'Points': 'N/A'})
                c_rank = player_rankings.get(c, {'Rank': 'N/A', 'Points': 'N/A'})
                d_rank = player_rankings.get(d, {'Rank': 'N/A', 'Points': 'N/A'})
                e_rank = player_rankings.get(e, {'Rank': 'N/A', 'Points': 'N/A'})

                valid_combinations.append({
                    'Year': year,
                    'League': league,
                    'Combo': f'{a}, {b}, {c}, {d}, {e}',
                    'Rankings': f'{a_rank["Rank"]}, {b_rank["Rank"]}, {c_rank["Rank"]}, {d_rank["Rank"]}, {e_rank["Rank"]}',
                    'ab': f'({a} {b})',
                    'a>b': f'{a_b_result[0]}',
                    'b>a': f'{a_b_result[1]}',
                    'bc': f'({b} {c})',
                    'b>c': f'{b_c_result[0]}',
                    'c>b': f'{b_c_result[1]}',
                    'cd': f'({c} {d})',
                    'c>d': f'{c_d_result[0]}',
                    'd>c': f'{c_d_result[1]}',
                    'de': f'({d} {e})',
                    'd>e': f'{d_e_result[0]}',
                    'e>d': f'{d_e_result[1]}',
                    'ea': f'({e} {a})',
                    'e>a': f'{a_e_result[1]}',
                    'a>e': f'{a_e_result[0]}'
                })

    # Convert the valid combinations to a DataFrame
    valid_combos_df = pd.DataFrame(valid_combinations)
    return valid_combos_df



def cycle_finder_all(cycle_size):
    years = range(2012, 2024)  
    leagues = ["AL", "NL"] 

    all_results_df = pd.DataFrame()

    for year in years:
        for league in leagues:
            print(f"Processing year {year}, league {league}...")
            result_df = cycle_finder(league, year, cycle_size)
            all_results_df = pd.concat([all_results_df, result_df], ignore_index=True)

    output_file = f"./src/baseball/Pairwise/cycles_{cycle_size}.csv"
    all_results_df.to_csv(output_file, index=False)
    print(f"All data has been processed and saved to {output_file}")



def cycle_finder_cutoff_3cycle(cutoff):
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




cycle_finder_all(5)


# cycle_finder_cutoff(10)
