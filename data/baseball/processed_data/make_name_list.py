import pandas as pd
import os

"""
This script processes MVP voting data by grouping the data by 'Year' and 'League'. It extracts the 
names of voters and players (from the top 10 ranks) and saves the results as separate CSV files.

Input: A CSV file with MVP voting data, including columns for voter names and player rankings 
('1st' to '10th').

Output: 
1. A CSV file containing all unique nominees and another for all voters.
2. Separate CSV files for each year and league, containing sorted lists of unique voters and nominees.
"""


df = pd.read_csv('./data/baseball/processed_data/entire_data/mvp_ballots_v1.csv')

# make a list of all names in col 1st to 10th
columns_1_to_10 = ['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th']
columns_name = 'Name'
names = []
names_voters = []

# Iterate over the rows and append names from relevant columns
for col in columns_1_to_10:
    names.extend(df[col].tolist())
names_voters.extend(df['Name'].tolist())

# Remove duplicates by converting the list to a set, then sort alphabetically
unique_names = sorted(set(names))
unique_names_voters = sorted(set(names_voters))

output_df = pd.DataFrame(unique_names, columns=["Name"])
output_df.to_csv('./data/baseball/processed_data/all_names/all_nominees.csv', index=False)
output_df = pd.DataFrame(unique_names_voters, columns=["Name"])
output_df.to_csv('./data/baseball/processed_data/all_names/all_voters.csv', index=False)


# create auxiliary file
output_dir_names = './data/baseball/processed_data/separate_names'
output_dir_players = './data/baseball/processed_data/separate_names'
os.makedirs(output_dir_names, exist_ok=True)
os.makedirs(output_dir_players, exist_ok=True)

grouped = df.groupby(['Year', 'League'])

for (year, league), group in grouped:
    # Process and store the "Name" column
    names_list = group[columns_name].unique() 
    sorted_names = sorted(names_list) 
    output_name_file = os.path.join(output_dir_names, f'mvp_voters_{year}_{league}.csv')
    pd.DataFrame(sorted_names, columns=[columns_name]).to_csv(output_name_file, index=False)
    
    # Process and store players from the '1st' to '10th' columns
    players = pd.melt(group[columns_1_to_10], value_name='Player')['Player']
    unique_players = sorted(players.dropna().unique())  
    output_players_file = os.path.join(output_dir_players, f'mvp_nominees_{year}_{league}.csv')
    pd.DataFrame(unique_players, columns=['Player']).to_csv(output_players_file, index=False)
    
