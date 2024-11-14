import pandas as pd
import itertools

def paradox_finder(weights, year, week, removed_teams, target_rankings):
    #Reads in csv file that contains ballot data for the provided week and year if it exists
    try:
        df = pd.read_csv(f'../../../../data/college-polls/processed_data/ballot_data_by_season_and_week/season_{year}/{year}_week{week}_top25.csv')
    except:
        print(f'Data for this combination of week and year (year: {year}, week: {week}) was not available in the local ballot_data_by_season_and_week directory')
        return

    borda_scores = dict()

    #Iterates through rows of top25 data to create borda count result that excludes teams in 'removed_teams'
    for index, row in df.iterrows():
        team_rankings = row[3:].tolist()
        for removed_team in removed_teams: 
            if removed_team in team_rankings: team_rankings.remove(removed_team)
        for idx in range(len(team_rankings)):
            if team_rankings[idx] not in borda_scores: borda_scores[team_rankings[idx]] = 0
            if idx < len(weights):
                borda_scores[team_rankings[idx]] += weights[idx]
    
    org_borda_df = pd.read_csv(f'../results/borda_top25/season_{year}/{year}_week{week}_top25.csv')

    org_borda_teams = org_borda_df["Teams"].tolist()
    sorted_teams = sorted(borda_scores.items(), key=lambda x: x[1], reverse=True)
    sorted_teams = [tup[0] for tup in sorted_teams]
    rt_ranking = [org_borda_teams.index(removed_team) + 1 for removed_team in removed_teams]

    target_teams = [org_borda_teams[i - 1] for i in target_rankings]
    comp_to_target = [sorted_teams[i - 1] for i in target_rankings]
    new_ranking = [org_borda_teams.index(team) + 1 for team in comp_to_target]

    paradox_find_results = []

    #Records the following data in the situation rankings of the target teams changes
    if target_teams != comp_to_target:
        paradox_find_results.append({
            'Season': f'{year}',
            'Week': f'{week}',
            'Removed-Teams': tuple(removed_teams),
            'RT-Ranking': tuple(rt_ranking),
            'Original-Teams': tuple(target_teams),
            'Original-Rankings': tuple(target_rankings),
            'New-Teams': tuple(comp_to_target),
            'New-Rankings': tuple(new_ranking)
        })

    return pd.DataFrame(paradox_find_results)

def paradox_file_maker(week, year, target_rankings, remove_amount):
    all_results_df = pd.DataFrame()

    #Reads in csv file that contains standard borda-count data for the provided week and year if it exists
    try:
        top25_df = pd.read_csv(f"../results/borda_top25/season_{year}/{year}_week{week}_top25.csv")
    except:
        print(f'Data for this combination of week and year (year: {year}, week: {week}) was not available in the local ballot_data_by_season_and_week directory')
        return
    
    global_ranked_teams = top25_df["Teams"].tolist()
    weights = [i for i in range(25, 0, -1)]

    #Selects top 30 teams that are eligible to be removed from ballots
    for idx in target_rankings:
        global_ranked_teams.pop(idx - 1)

    global_ranked_teams = global_ranked_teams[:30]

    #Generate a list of all combinations of unique teams of length 'remove_amount' and iterate through the list to
    #construct dataframe containing paradox findings
    combinations = list(itertools.combinations(global_ranked_teams, remove_amount))
    for combo in combinations:
        result_df = paradox_finder(weights, year, week, list(combo), target_rankings)
        if result_df is not None:
            all_results_df = pd.concat([all_results_df, result_df], ignore_index=True)    
    
    return all_results_df

def IIA_per_target_rank(target_rankings, remove_amount):
    target_df = pd.DataFrame()
    #Iterate through all combinations of years and weeks in order find any paradoxes and records all the discovered paradoxes
    #corresponding with specified 'target_rankings' and 'remove_amount'
    for year in range(2014, 2025):
        for week in range(1, 18):
            res_df = paradox_file_maker(str(week), str(year), target_rankings, remove_amount)
            if res_df is not None:
                target_df = pd.concat([target_df, res_df], ignore_index=True)   
    
    #Writes paradox findings to a csv and sorts output by descending order of 'New-Rankings',
    #which essentially ranks findings by the highest position where rank changed
    csv_name = "borda_IIA_range_" + str(target_rankings) + "_remove_" + str(remove_amount) + "_maxRemoved_30.csv"
    target_df = target_df.sort_values(by='New-Rankings', ascending=False)
    target_df.to_csv(csv_name, index=False)     

IIA_per_target_rank([1], 1)