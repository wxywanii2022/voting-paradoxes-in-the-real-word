import pandas as pd
from itertools import combinations
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import time
import os
import pickle
import hashlib

# Predefined rank points for top 25 teams
rank_points = list(range(25, 0, -1))

def generate_cache_key(year, week, weights):
    """
    Generate a unique cache key for the preprocessed data.
    
    Args:
        year (int): Season year
        week (int): Week number
        weights (list): Weight list used for scoring
        
    Returns:
        str: Unique cache key
    """
    # Create a string containing all the parameters
    params = f"{year}_{week}_{'-'.join(map(str, weights))}"
    # Generate a hash of the parameters
    return hashlib.md5(params.encode()).hexdigest()

def load_or_preprocess_data(year, week, weights, cache_dir="./cache"):
    """
    Load preprocessed data from cache if available, otherwise process and cache it.
    
    Args:
        year (int): Season year
        week (int): Week number
        weights (list): Weight list for scoring
        cache_dir (str): Directory to store cache files
        
    Returns:
        tuple: (removal_effects, original_scores, original_teams) or None if data unavailable
    """
    try:
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        # Generate cache key and file path
        cache_key = generate_cache_key(year, week, weights)
        cache_file = os.path.join(cache_dir, f"{cache_key}.pkl")
        
        # Try to load from cache
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        
        # If not in cache, process the data
        ballot_path = f'./data/college-polls/processed_data/ballot_data_by_season_and_week/season_{year}/{year}_week{week}_top25.csv'
        rankings_path = f'./src/college-polls/Borda/results/borda_top25/season_{year}/{year}_week{week}_top25.csv'
        
        ballots = pd.read_csv(ballot_path)
        rankings_df = pd.read_csv(rankings_path)
        original_teams = rankings_df["Teams"].tolist()
        
        # Calculate original scores
        original_scores = defaultdict(int)
        for _, ballot in ballots.iterrows():
            for idx, team in enumerate(ballot.iloc[3:]):
                if idx < len(weights):
                    original_scores[team] += weights[idx]
        
        # Preprocess removal effects
        removal_effects = {team: {} for team in original_teams}
        rankings = ballots.iloc[:, 3:].values
        
        for removed_team in original_teams:
            for ballot in rankings:
                if removed_team in ballot:
                    removed_idx = list(ballot).index(removed_team)
                    
                    adjusted_points = defaultdict(int)
                    for i in range(removed_idx + 1, len(ballot)):
                        team_name = ballot[i]
                        adjusted_points[team_name] += 1
                    
                    for team, points in adjusted_points.items():
                        if team in removal_effects[removed_team]:
                            removal_effects[removed_team][team] += points
                        else:
                            removal_effects[removed_team][team] = points
        
        # Cache the results
        result = (removal_effects, original_scores, original_teams)
        with open(cache_file, 'wb') as f:
            pickle.dump(result, f)
        
        return result
        
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f'Error processing {year} week {week}: {e}')
        return None

def detect_paradox(year, week, target_rankings, removed_teams, weights=None):
    """
    Detect ranking paradoxes for specific combinations of removals.
    
    Args:
        year (int): Season year
        week (int): Week number
        target_rankings (list): Rankings to analyze
        removed_teams (list): Teams to remove
        weights (list): Optional custom weight list
    
    Returns:
        dict: Paradox information if found, None otherwise
    """
    if weights is None:
        weights = rank_points
        
    # Load or process data
    data = load_or_preprocess_data(year, week, weights)
    if data is None:
        return None
        
    removal_effects, original_scores, original_teams = data
    
    # Get target teams
    target_teams = [original_teams[i - 1] for i in target_rankings]
    
    # Recalculate scores with removals
    new_scores = original_scores.copy()
    
    # Apply all adjustments from removed teams
    for removed_team in removed_teams:
        effects = removal_effects[removed_team]
        for team, adjustment in effects.items():
            if team in new_scores and team not in removed_teams:
                new_scores[team] += adjustment
    
    # Remove scores for removed teams
    for team in removed_teams:
        new_scores.pop(team, None)
    
    # Get new rankings
    sorted_teams = sorted(new_scores.items(), key=lambda x: x[1], reverse=True)
    sorted_teams = [team for team, _ in sorted_teams]
    
    # Compare new positions for target teams
    new_target_teams = [sorted_teams[i - 1] for i in target_rankings]
    
    if new_target_teams != target_teams:
        # Get original rankings of removed teams
        removed_rankings = [original_teams.index(team) + 1 for team in removed_teams]
        # Get original rankings of new target teams
        new_rankings = [original_teams.index(team) + 1 for team in new_target_teams]
        
        return {
            'Season': str(year),
            'Week': str(week),
            'Removed-Teams': tuple(removed_teams),
            'RT-Ranking': tuple(removed_rankings),
            'Original-Teams': tuple(target_teams),
            'Original-Rankings': tuple(target_rankings),
            'New-Teams': tuple(new_target_teams),
            'New-Rankings': tuple(new_rankings)
        }
        
    return None

def process_year_week(year, week, target_rankings, remove_amount, weights):
    """Process a specific year and week for paradoxes."""
    start_time = time.time()
    
    try:
        # Load data
        data = load_or_preprocess_data(year, week, weights)
        if data is None:
            return []
            
        _, _, all_teams = data
        
        # Remove target teams from consideration
        eligible_teams = [team for i, team in enumerate(all_teams) 
                        if i + 1 not in target_rankings][:10]
        
        # Check all possible combinations of removals
        results = []
        for teams_to_remove in combinations(eligible_teams, remove_amount):
            result = detect_paradox(year, week, target_rankings, 
                                 list(teams_to_remove), weights)
            if result:
                results.append(result)
                
        elapsed_time = time.time() - start_time
        print(f"Processed year {year}, week {week} in {elapsed_time:.2f} seconds")
        
        return results
        
    except Exception as e:
        print(f'Error processing {year} week {week}: {e}')
        return []

def analyze_all_paradoxes(target_rankings, remove_amount, weights=None):
    """
    Analyze paradoxes across all seasons and weeks using parallel processing.
    
    Args:
        target_rankings (list): Rankings to analyze
        remove_amount (int): Number of teams to remove
        weights (list): Optional custom weight list
    """
    if weights is None:
        weights = rank_points
        
    all_results = []
    
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_year_week, year, week, target_rankings, remove_amount, weights)
            for year in range(2014, 2025)
            for week in range(1, 18)
        ]
        
        for future in futures:
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                print(f'Error collecting results: {e}')
    
    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df.sort_values(by='New-Rankings', ascending=False, inplace=True)
        
        output_path = f"./src/college-polls/Borda/IIA_results/temp_output_{target_rankings}_{remove_amount}.csv"
        results_df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")
    else:
        print("No paradoxes found.")

if __name__ == '__main__':
    analyze_all_paradoxes([1, 2, 3, 4, 5], 2)



"""
Total running time: 4.48s

Processed year 2014, week 1 in 0.02 seconds
Processed year 2014, week 2 in 0.02 seconds
Processed year 2014, week 3 in 0.02 seconds
Processed year 2014, week 4 in 0.03 seconds
Processed year 2014, week 5 in 0.03 seconds
Processed year 2014, week 7 in 0.02 seconds
Processed year 2014, week 6 in 0.03 seconds
Processed year 2014, week 8 in 0.03 seconds
Processed year 2014, week 9 in 0.03 seconds
Processed year 2014, week 10 in 0.04 seconds
Processed year 2014, week 11 in 0.04 seconds
Processed year 2014, week 13 in 0.04 seconds
Processed year 2014, week 12 in 0.04 seconds
Processed year 2014, week 14 in 0.04 seconds
Processed year 2014, week 15 in 0.04 seconds
Processed year 2014, week 16 in 0.05 seconds
Processed year 2014, week 17 in 0.05 seconds
Processed year 2015, week 1 in 0.05 seconds
Processed year 2015, week 4 in 0.06 seconds
Processed year 2015, week 3 in 0.06 seconds
Processed year 2015, week 5 in 0.06 seconds
Processed year 2015, week 7 in 0.06 seconds
Processed year 2015, week 6 in 0.06 seconds
Processed year 2015, week 9 in 0.06 seconds
Processed year 2015, week 8 in 0.06 seconds
Processed year 2015, week 10 in 0.06 seconds
Processed year 2015, week 11 in 0.06 seconds
Processed year 2015, week 12 in 0.06 seconds
Processed year 2015, week 13 in 0.06 seconds
Processed year 2015, week 14 in 0.06 seconds
Processed year 2015, week 15 in 0.06 seconds
Processed year 2015, week 16 in 0.06 seconds
Processed year 2016, week 1 in 0.07 seconds
Processed year 2016, week 2 in 0.06 seconds
Processed year 2016, week 3 in 0.07 seconds
Processed year 2016, week 4 in 0.06 seconds
Processed year 2016, week 5 in 0.07 seconds
Processed year 2016, week 6 in 0.07 seconds
Processed year 2016, week 7 in 0.06 seconds
Processed year 2016, week 8 in 0.07 seconds
Processed year 2016, week 9 in 0.06 seconds
Processed year 2016, week 10 in 0.07 seconds
Processed year 2016, week 12 in 0.06 seconds
Processed year 2016, week 11 in 0.06 seconds
Processed year 2016, week 13 in 0.07 seconds
Processed year 2016, week 14 in 0.07 seconds
Processed year 2016, week 15 in 0.07 seconds
Processed year 2016, week 16 in 0.06 seconds
Processed year 2017, week 1 in 0.07 seconds
Processed year 2017, week 3 in 0.07 seconds
Processed year 2017, week 2 in 0.07 seconds
Processed year 2017, week 4 in 0.06 seconds
Processed year 2017, week 5 in 0.06 seconds
Processed year 2017, week 6 in 0.06 seconds
Processed year 2017, week 7 in 0.07 seconds
Processed year 2017, week 8 in 0.06 seconds
Processed year 2017, week 9 in 0.06 seconds
Processed year 2017, week 10 in 0.06 seconds
Processed year 2017, week 11 in 0.07 seconds
Processed year 2017, week 12 in 0.07 seconds
Processed year 2017, week 13 in 0.06 seconds
Processed year 2017, week 14 in 0.07 seconds
Processed year 2017, week 15 in 0.07 seconds
Processed year 2017, week 16 in 0.07 seconds
Processed year 2018, week 1 in 0.07 seconds
Processed year 2018, week 2 in 0.06 seconds
Processed year 2018, week 3 in 0.06 seconds
Processed year 2018, week 4 in 0.07 seconds
Processed year 2018, week 6 in 0.07 secondsProcessed year 2018, week 5 in 0.08 seconds

Processed year 2018, week 7 in 0.07 seconds
Processed year 2018, week 8 in 0.07 seconds
Processed year 2018, week 9 in 0.08 secondsProcessed year 2018, week 10 in 0.08 seconds
Processed year 2018, week 12 in 0.08 seconds

Processed year 2018, week 11 in 0.08 secondsProcessed year 2018, week 13 in 0.08 seconds
Processed year 2018, week 15 in 0.06 seconds
Processed year 2018, week 14 in 0.08 seconds
Processed year 2018, week 16 in 0.06 seconds

Processed year 2019, week 4 in 0.07 seconds
Processed year 2019, week 3 in 0.08 seconds
Processed year 2019, week 6 in 0.06 seconds
Processed year 2019, week 2 in 0.09 secondsProcessed year 2019, week 1 in 0.10 seconds

Processed year 2019, week 7 in 0.07 seconds
Processed year 2019, week 9 in 0.08 seconds
Processed year 2019, week 10 in 0.08 seconds
Processed year 2019, week 8 in 0.09 seconds
Processed year 2019, week 12 in 0.07 seconds
Processed year 2019, week 11 in 0.08 seconds
Processed year 2019, week 5 in 0.11 seconds
Processed year 2019, week 13 in 0.08 seconds
Processed year 2019, week 14 in 0.07 seconds
Processed year 2019, week 15 in 0.07 seconds
Processed year 2019, week 16 in 0.08 seconds
Processed year 2019, week 17 in 0.08 seconds
Processed year 2020, week 1 in 0.07 seconds
Processed year 2020, week 2 in 0.06 seconds
Processed year 2020, week 3 in 0.06 seconds
Processed year 2020, week 4 in 0.07 seconds
Processed year 2020, week 5 in 0.07 seconds
Processed year 2020, week 6 in 0.07 seconds
Processed year 2020, week 7 in 0.06 seconds
Processed year 2020, week 8 in 0.06 seconds
Processed year 2020, week 9 in 0.05 seconds
Processed year 2020, week 11 in 0.05 seconds
Processed year 2020, week 10 in 0.05 seconds
Processed year 2020, week 12 in 0.06 seconds
Processed year 2020, week 13 in 0.06 secondsProcessed year 2020, week 14 in 0.06 seconds

Processed year 2020, week 15 in 0.06 seconds
Processed year 2020, week 16 in 0.06 seconds
Processed year 2020, week 17 in 0.06 seconds
Processed year 2021, week 2 in 0.06 seconds
Processed year 2021, week 1 in 0.06 seconds
Processed year 2021, week 4 in 0.07 seconds
Processed year 2021, week 3 in 0.07 seconds
Processed year 2021, week 5 in 0.06 seconds
Processed year 2021, week 6 in 0.06 seconds
Processed year 2021, week 7 in 0.06 seconds
Processed year 2021, week 8 in 0.06 seconds
Processed year 2021, week 9 in 0.06 seconds
Processed year 2021, week 10 in 0.06 seconds
Processed year 2021, week 11 in 0.06 seconds
Processed year 2021, week 12 in 0.06 seconds
Processed year 2021, week 13 in 0.06 seconds
Processed year 2021, week 14 in 0.06 seconds
Processed year 2021, week 15 in 0.06 seconds
Processed year 2021, week 16 in 0.06 seconds
Processed year 2022, week 1 in 0.07 seconds
Processed year 2022, week 2 in 0.07 seconds
Processed year 2022, week 3 in 0.07 seconds
Processed year 2022, week 4 in 0.07 seconds
Processed year 2022, week 5 in 0.07 secondsProcessed year 2022, week 6 in 0.07 seconds

Processed year 2022, week 7 in 0.07 seconds
Processed year 2022, week 8 in 0.07 seconds
Processed year 2022, week 9 in 0.06 seconds
Processed year 2022, week 10 in 0.07 seconds
Processed year 2022, week 11 in 0.06 seconds
Processed year 2022, week 12 in 0.06 seconds
Processed year 2022, week 13 in 0.06 seconds
Processed year 2022, week 14 in 0.06 seconds
Processed year 2022, week 15 in 0.06 seconds
Processed year 2022, week 16 in 0.06 seconds
Processed year 2023, week 1 in 0.07 seconds
Processed year 2023, week 2 in 0.06 seconds
Processed year 2023, week 4 in 0.06 seconds
Processed year 2023, week 3 in 0.06 seconds
Processed year 2023, week 5 in 0.06 seconds
Processed year 2023, week 6 in 0.06 seconds
Processed year 2023, week 8 in 0.06 secondsProcessed year 2023, week 7 in 0.06 seconds

Processed year 2023, week 9 in 0.06 seconds
Processed year 2023, week 10 in 0.06 seconds
Processed year 2023, week 11 in 0.06 seconds
Processed year 2023, week 12 in 0.07 seconds
Processed year 2023, week 13 in 0.06 seconds
Processed year 2023, week 15 in 0.06 seconds
Processed year 2023, week 14 in 0.06 seconds
Processed year 2023, week 16 in 0.06 seconds
Processed year 2024, week 1 in 0.06 seconds
Processed year 2024, week 2 in 0.06 seconds
Processed year 2024, week 3 in 0.05 seconds
Processed year 2024, week 4 in 0.05 secondsProcessed year 2024, week 5 in 0.04 seconds
"""