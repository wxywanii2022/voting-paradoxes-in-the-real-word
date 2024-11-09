import pandas as pd

def find_condorcet_winner(league, year, top_n):
    borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
    borda_results = pd.read_csv(borda_path)
    
    top_players = borda_results['Player'].head(top_n).tolist()
    
    pairwise_path = f'./src/baseball/Pairwise/pairwise_results/{year} {league}.csv'
    pairwise_results = pd.read_csv(pairwise_path)
    
    # Check each top player to see if they win against all other top players
    for player in top_players:
        is_condorcet_winner = True
        
        # Loop through each row in pairwise results and check outcomes for the current player
        for _, row in pairwise_results.iterrows():
            if row['PlayerA'] == player:
                # Current player is PlayerA, check if they win over PlayerB
                if row['A>B'] <= row['B>A']:  # If PlayerA doesn't have more votes than PlayerB
                    is_condorcet_winner = False
                    break
            elif row['PlayerB'] == player:
                # Current player is PlayerB, check if they win over PlayerA
                if row['B>A'] <= row['A>B']:  # If PlayerB doesn't have more votes than PlayerA
                    is_condorcet_winner = False
                    break

        # If player wins against all others in pairwise, return as the Condorcet winner
        if is_condorcet_winner:
            return player
    
    # If no Condorcet winner found, return None
    return None


# for i in range (2012, 2024):
    print(find_condorcet_winner("AL", i, 3))
    print(find_condorcet_winner("NL", i, 3))


def borda_condorcet():
    results = []

    for year in range(2012, 2024):
        for league in ["AL", "NL"]:
            # Find Borda winner
            borda_path = f'./src/baseball/Borda/results/borda_14-9-8--1/{year}_{league}_14-9-8--1.csv'
            borda_results = pd.read_csv(borda_path)
            borda_winner = borda_results['Player'].iloc[0]  

            condorcet_winner = find_condorcet_winner(league, year, top_n=3)

            same_winner = borda_winner == condorcet_winner

            results.append({
                "Year": year,
                "League": league,
                "Borda Winner": borda_winner,
                "Condorcet Winner": condorcet_winner,
                "Same?": same_winner
            })

    results_df = pd.DataFrame(results)
    results_df.to_csv("./src/baseball/Pairwise/borda_condorcet_results.csv", index=False)


borda_condorcet()
        
