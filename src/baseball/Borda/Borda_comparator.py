import pandas as pd
import os

# Function to read the top N players from a given CSV file path and return their names
def get_top_n_players(file_path, top_n):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        top_players = df['Player'].head(top_n).tolist()
        return top_players
    else:
        print(f"File {file_path} not found.")
        return []

# Function to process a specific league, year, and top number of players
def process_league_year(league, year, top_n):
    base_path = f"./src/baseball/Borda/results/"
    
    # Construct paths for each Borda system
    official_borda_path = os.path.join(base_path, f'borda_14-9-8--1/{year}_{league}_14-9-8--1.csv')
    borda_top1_path = os.path.join(base_path, f'borda_top1/{year}_{league}_top1.csv')
    borda_top3_path = os.path.join(base_path, f'borda_top3/{year}_{league}_top3.csv')
    borda_top5_path = os.path.join(base_path, f'borda_top5/{year}_{league}_top5.csv')
    borda_top10_path = os.path.join(base_path, f'borda_top10/{year}_{league}_top10.csv')
    dowdall_path = os.path.join(base_path, f'borda_Dowdall/{year}_{league}_Dowdall.csv')
    
    # Get top N players' names from each system
    official_borda_players = get_top_n_players(official_borda_path, top_n)
    borda_top1_players = get_top_n_players(borda_top1_path, top_n)
    borda_top3_players = get_top_n_players(borda_top3_path, top_n)
    borda_top5_players = get_top_n_players(borda_top5_path, top_n)
    borda_top10_players = get_top_n_players(borda_top10_path, top_n)
    dowdall_players = get_top_n_players(dowdall_path, top_n)
    
    # Return the combined results as a tuple
    return {
        'Season': year,
        'League': league,
        'Official Borda': ', '.join(official_borda_players),
        'Borda-Top1': ', '.join(borda_top1_players),
        'Borda-Top3': ', '.join(borda_top3_players),
        'Borda-Top5': ', '.join(borda_top5_players),
        'Borda-Top10': ', '.join(borda_top10_players),
        'Dowdall': ', '.join(dowdall_players)
    }

# Function to process all leagues and years, construct the output file path, and save the results
def borda_comparator(top_n):
    results = []
    leagues = ['AL', 'NL']
    years = range(2012, 2024)  # Years 2012 to 2023
    
    for year in years:
        for league in leagues:
            result = process_league_year(league, year, top_n)
            results.append(result)
    
    output_file = f"./src/baseball/Borda/borda-comparison-top{top_n}.csv"
    
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_file, index=False, columns=[
        'Season', 'League', 'Official Borda', 'Borda-Top1', 'Borda-Top3', 'Borda-Top5', 'Borda-Top10', 'Dowdall'
    ])
    print(f"Results saved to {output_file}")

borda_comparator(5)
