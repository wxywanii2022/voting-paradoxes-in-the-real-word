import pandas as pd
from itertools import combinations
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import time

# Predefined rank points for top 25 teams
rank_points = list(range(25, 0, -1))

def preprocess_removal_effects(ballots, teams):
    """
    Preprocess the effect of removing each team, storing the point adjustments for all other teams
    when a particular team is removed.
    
    Args:
        ballots (pd.DataFrame): DataFrame containing ballot data
        teams (list): List of teams to analyze
    
    Returns:
        dict: Mapping of removal effects for each team
    """
    removal_effects = {team: {} for team in teams}
    
    for removed_team in teams:
        # Get only the ranking columns
        rankings = ballots.iloc[:, 3:].values
        
        for ballot in rankings:
            if removed_team in ballot:
                removed_idx = list(ballot).index(removed_team)
                
                # Calculate adjustments for teams after the removed team
                adjusted_points = defaultdict(int)
                for i in range(removed_idx + 1, len(ballot)):
                    team_name = ballot[i]
                    adjusted_points[team_name] += 1
                
                # Update removal effects
                for team, points in adjusted_points.items():
                    if team in removal_effects[removed_team]:
                        removal_effects[removed_team][team] += points
                    else:
                        removal_effects[removed_team][team] = points
                        
    return removal_effects

def recalculate_scores(teams, removed_teams, removal_effects, original_scores):
    """
    Quickly recalculate scores after removing teams using precomputed effects.
    
    Args:
        teams (list): List of all teams
        removed_teams (list): Teams being removed
        removal_effects (dict): Precomputed removal effects
        original_scores (dict): Original Borda scores
        
    Returns:
        dict: Updated scores after removals
    """
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
        
    return new_scores

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
    try:
        ballot_path = f'./data/college-polls/processed_data/ballot_data_by_season_and_week/season_{year}/{year}_week{week}_top25.csv'
        ballots = pd.read_csv(ballot_path)
        
        original_rankings_path = f'./src/college-polls/Borda/results/borda_top25/season_{year}/{year}_week{week}_top25.csv'
        org_rankings = pd.read_csv(original_rankings_path)
        
        # Get original teams and their positions
        original_teams = org_rankings["Teams"].tolist()
        target_teams = [original_teams[i - 1] for i in target_rankings]
        
        # Precompute removal effects
        all_teams = set(original_teams)
        removal_effects = preprocess_removal_effects(ballots, all_teams)
        
        # Calculate original scores
        original_scores = defaultdict(int)
        for _, ballot in ballots.iterrows():
            for idx, team in enumerate(ballot.iloc[3:]):
                if idx < len(weights):
                    original_scores[team] += weights[idx]
        
        # Recalculate scores with removals
        new_scores = recalculate_scores(all_teams, removed_teams, removal_effects, original_scores)
        
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
        
    except FileNotFoundError:
        print(f'Data not available for year {year}, week {week}')
        return None
    except Exception as e:
        print(f'Error processing year {year}, week {week}: {e}')
        return None

def process_year_week(year, week, target_rankings, remove_amount, weights):
    start_time = time.time()  # Start the timer
    
    try:
        # Load rankings to get teams eligible for removal
        rankings_path = f'./src/college-polls/Borda/results/borda_top25/season_{year}/{year}_week{week}_top25.csv'
        rankings_df = pd.read_csv(rankings_path)
        all_teams = rankings_df["Teams"].tolist()
        
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
                
        elapsed_time = time.time() - start_time  # Stop the timer
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
    analyze_all_paradoxes([1], 2)



"""
Total running time: 30.27s


Processed year 2014, week 11 in 1.93 seconds
Processed year 2014, week 10 in 1.94 seconds
Processed year 2014, week 9 in 1.95 seconds
Processed year 2014, week 12 in 1.99 seconds
Processed year 2014, week 6 in 2.08 seconds
Processed year 2014, week 7 in 2.14 seconds
Processed year 2014, week 8 in 2.16 seconds
Error processing 2015 week 2: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2015/2015_week2_top25.csv'
Processed year 2014, week 3 in 2.22 seconds
Processed year 2014, week 5 in 2.21 seconds
Processed year 2014, week 2 in 2.25 seconds
Processed year 2014, week 4 in 2.22 seconds
Processed year 2014, week 1 in 2.32 seconds
Processed year 2014, week 14 in 1.96 seconds
Processed year 2014, week 15 in 1.96 seconds
Processed year 2014, week 13 in 2.00 seconds
Processed year 2014, week 16 in 1.98 seconds
Processed year 2014, week 17 in 2.02 seconds
Processed year 2015, week 8 in 2.01 seconds
Processed year 2015, week 7 in 2.11 seconds
Processed year 2015, week 6 in 2.14 seconds
Processed year 2015, week 1 in 2.23 seconds
Error processing 2015 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2015/2015_week17_top25.csv'
Processed year 2015, week 5 in 2.22 seconds
Processed year 2015, week 4 in 2.32 seconds
Processed year 2015, week 3 in 2.37 seconds
Processed year 2015, week 11 in 1.97 seconds
Processed year 2015, week 9 in 2.01 seconds
Processed year 2015, week 10 in 2.17 seconds
Processed year 2015, week 12 in 2.14 seconds
Processed year 2015, week 13 in 2.22 seconds
Processed year 2015, week 16 in 2.08 seconds
Processed year 2015, week 15 in 2.14 seconds
Processed year 2015, week 14 in 2.22 seconds
Processed year 2016, week 2 in 2.16 seconds
Processed year 2016, week 1 in 2.27 seconds
Processed year 2016, week 3 in 2.17 seconds
Processed year 2016, week 4 in 2.31 seconds
Processed year 2016, week 5 in 2.27 seconds
Error processing 2016 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2016/2016_week17_top25.csv'
Processed year 2016, week 6 in 2.29 seconds
Processed year 2016, week 7 in 2.26 seconds
Processed year 2016, week 8 in 2.23 seconds
Processed year 2016, week 9 in 2.16 seconds
Processed year 2016, week 10 in 2.05 seconds
Processed year 2016, week 11 in 2.10 seconds
Processed year 2016, week 12 in 2.19 seconds
Processed year 2016, week 13 in 2.16 seconds
Processed year 2016, week 14 in 2.20 seconds
Processed year 2016, week 15 in 2.22 seconds
Processed year 2016, week 16 in 2.19 seconds
Processed year 2017, week 1 in 2.28 seconds
Processed year 2017, week 6 in 2.11 seconds
Processed year 2017, week 3 in 2.24 seconds
Processed year 2017, week 7 in 2.07 seconds
Processed year 2017, week 2 in 2.41 seconds
Error processing 2017 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2017/2017_week17_top25.csv'
Processed year 2017, week 5 in 2.25 seconds
Processed year 2017, week 4 in 2.35 seconds
Processed year 2017, week 9 in 2.09 seconds
Processed year 2017, week 10 in 2.05 seconds
Processed year 2017, week 8 in 2.29 seconds
Processed year 2017, week 11 in 2.10 seconds
Processed year 2017, week 12 in 2.10 seconds
Processed year 2017, week 13 in 2.04 seconds
Processed year 2017, week 16 in 2.05 seconds
Processed year 2017, week 15 in 2.10 seconds
Processed year 2017, week 14 in 2.16 seconds
Processed year 2018, week 3 in 2.29 seconds
Processed year 2018, week 1 in 2.38 seconds
Processed year 2018, week 2 in 2.39 seconds
Processed year 2018, week 7 in 2.13 seconds
Processed year 2018, week 4 in 2.34 seconds
Error processing 2018 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2018/2018_week17_top25.csv'
Processed year 2018, week 8 in 2.19 seconds
Processed year 2018, week 6 in 2.37 seconds
Processed year 2018, week 5 in 2.46 seconds
Processed year 2018, week 9 in 2.18 seconds
Processed year 2018, week 10 in 2.25 seconds
Processed year 2018, week 11 in 2.28 seconds
Processed year 2018, week 12 in 2.28 seconds
Processed year 2018, week 14 in 2.14 seconds
Processed year 2018, week 13 in 2.18 seconds
Processed year 2018, week 15 in 2.14 seconds
Processed year 2018, week 16 in 2.25 seconds
Processed year 2019, week 4 in 2.25 seconds
Processed year 2019, week 1 in 2.46 seconds
Processed year 2019, week 2 in 2.32 seconds
Processed year 2019, week 3 in 2.36 seconds
Processed year 2019, week 5 in 2.34 seconds
Processed year 2019, week 6 in 2.27 seconds
Processed year 2019, week 7 in 2.25 seconds
Processed year 2019, week 8 in 2.23 seconds
Processed year 2019, week 9 in 2.20 seconds
Processed year 2019, week 10 in 2.24 seconds
Processed year 2019, week 11 in 2.25 seconds
Processed year 2019, week 14 in 2.13 seconds
Processed year 2019, week 12 in 2.38 seconds
Processed year 2019, week 15 in 2.16 seconds
Processed year 2019, week 16 in 2.15 seconds
Processed year 2019, week 13 in 2.25 seconds
Processed year 2019, week 17 in 2.24 seconds
Processed year 2020, week 3 in 2.30 seconds
Processed year 2020, week 2 in 2.39 seconds
Processed year 2020, week 1 in 2.49 seconds
Processed year 2020, week 4 in 2.39 seconds
Processed year 2020, week 5 in 2.57 seconds
Processed year 2020, week 10 in 2.25 seconds
Processed year 2020, week 6 in 2.60 seconds
Processed year 2020, week 11 in 2.32 seconds
Processed year 2020, week 8 in 2.46 seconds
Processed year 2020, week 9 in 2.45 seconds
Processed year 2020, week 7 in 2.51 seconds
Processed year 2020, week 12 in 2.22 seconds
Processed year 2020, week 13 in 2.23 seconds
Processed year 2020, week 15 in 2.10 seconds
Processed year 2020, week 14 in 2.28 seconds
Processed year 2020, week 16 in 2.22 seconds
Processed year 2020, week 17 in 2.24 seconds
Processed year 2021, week 1 in 2.47 seconds
Processed year 2021, week 2 in 2.51 seconds
Processed year 2021, week 5 in 2.38 seconds
Processed year 2021, week 6 in 2.37 seconds
Processed year 2021, week 4 in 2.45 seconds
Error processing 2021 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2021/2021_week17_top25.csv'
Processed year 2021, week 3 in 2.61 seconds
Processed year 2021, week 7 in 2.24 seconds
Processed year 2021, week 10 in 2.21 seconds
Processed year 2021, week 9 in 2.33 seconds
Processed year 2021, week 8 in 2.36 seconds
Processed year 2021, week 11 in 2.25 seconds
Processed year 2021, week 12 in 2.20 seconds
Processed year 2021, week 13 in 2.06 seconds
Processed year 2021, week 14 in 2.20 seconds
Processed year 2021, week 16 in 2.21 seconds
Processed year 2021, week 15 in 2.24 seconds
Processed year 2022, week 2 in 2.49 seconds
Processed year 2022, week 1 in 2.61 seconds
Processed year 2022, week 3 in 2.48 seconds
Processed year 2022, week 4 in 2.25 seconds
Processed year 2022, week 5 in 2.31 seconds
Error processing 2022 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2022/2022_week17_top25.csv'
Processed year 2022, week 6 in 2.39 seconds
Processed year 2022, week 7 in 2.40 seconds
Processed year 2022, week 8 in 2.32 seconds
Processed year 2022, week 9 in 2.23 seconds
Processed year 2022, week 10 in 2.37 seconds
Processed year 2022, week 12 in 2.34 seconds
Processed year 2022, week 11 in 2.52 seconds
Processed year 2022, week 13 in 2.33 seconds
Processed year 2022, week 14 in 2.35 seconds
Processed year 2022, week 15 in 2.29 seconds
Processed year 2022, week 16 in 2.39 seconds
Processed year 2023, week 3 in 2.36 seconds
Processed year 2023, week 1 in 2.61 seconds
Processed year 2023, week 2 in 2.58 seconds
Processed year 2023, week 4 in 2.38 seconds
Processed year 2023, week 5 in 2.31 seconds
Error processing 2023 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2023/2023_week17_top25.csv'
Processed year 2023, week 6 in 2.24 seconds
Processed year 2023, week 7 in 2.25 seconds
Processed year 2023, week 8 in 2.38 seconds
Processed year 2023, week 10 in 2.14 seconds
Processed year 2023, week 9 in 2.27 seconds
Error processing 2024 week 6: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week6_top25.csv'
Error processing 2024 week 7: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week7_top25.csv'
Error processing 2024 week 8: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week8_top25.csv'
Error processing 2024 week 9: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week9_top25.csv'
Error processing 2024 week 10: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week10_top25.csv'
Error processing 2024 week 11: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week11_top25.csv'
Error processing 2024 week 12: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week12_top25.csv'
Error processing 2024 week 13: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week13_top25.csv'
Error processing 2024 week 14: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week14_top25.csv'
Error processing 2024 week 15: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week15_top25.csv'
Error processing 2024 week 16: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week16_top25.csv'
Error processing 2024 week 17: [Errno 2] No such file or directory: './src/college-polls/Borda/results/borda_top25/season_2024/2024_week17_top25.csv'
Processed year 2023, week 11 in 2.26 seconds
Processed year 2023, week 12 in 2.12 seconds
Processed year 2023, week 15 in 1.99 seconds
Processed year 2023, week 13 in 2.16 seconds
Processed year 2023, week 14 in 2.09 seconds
Processed year 2023, week 16 in 2.12 seconds
Processed year 2024, week 1 in 2.15 seconds
Processed year 2024, week 2 in 2.12 seconds
Processed year 2024, week 3 in 2.09 seconds
Processed year 2024, week 5 in 1.85 seconds
Processed year 2024, week 4 in 1.99 seconds
Results saved to ./src/college-polls/Borda/IIA_results/temp_output.csv
    
    
"""