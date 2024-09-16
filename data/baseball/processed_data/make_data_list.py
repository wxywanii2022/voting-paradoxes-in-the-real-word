import pandas as pd
import os

df = pd.read_csv('./data/baseball/processed_data/entire_data/mvp_ballots_v1.csv')

grouped = df.groupby(['Year', 'League'])

for (year, league), group in grouped:
    file_name = f"./data/baseball/processed_data/separate_data/{year}_{league}_votes.csv"
    group.to_csv(file_name, index=False)
    print(f"Saved {file_name}")
