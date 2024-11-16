import pandas as pd

def find_condorcet_winner(week, year, top_n):
    try:
        borda_path = f'./src/college-polls/Borda/results/borda_top25/season_{year}/{year}_week{week}_top25.csv'
        borda_results = pd.read_csv(borda_path)
    except Exception as e:
        print(f"Error reading Borda results for week {week}, year {year}: {e}")
        return None

    try:
        top_players = borda_results['Teams'].head(top_n).tolist()
    except Exception as e:
        print(f"Error processing top players for week {week}, year {year}: {e}")
        return None

    try:
        pairwise_path = f'./src/college-polls/Pairwise/results/season_{year}/{year}_week{week}_condorcet.csv'
        pairwise_results = pd.read_csv(pairwise_path)
    except Exception as e:
        print(f"Error reading pairwise results for week {week}, year {year}: {e}")
        return None

    # Check each top player to see if they win against all other top players
    for player in top_players:
        is_condorcet_winner = True
        try:
            # Loop through each row in pairwise results and check outcomes for the current player
            for _, row in pairwise_results.iterrows():
                if row['TeamA'] == player:
                    if row['A>B'] <= row['B>A']:
                        is_condorcet_winner = False
                        break
                elif row['TeamB'] == player:
                    if row['B>A'] <= row['A>B']:
                        is_condorcet_winner = False
                        break
        except Exception as e:
            print(f"Error evaluating player {player} in week {week}, year {year}: {e}")
            continue

        # If player wins against all others in pairwise, return as the Condorcet winner
        if is_condorcet_winner:
            return player

    # If no Condorcet winner found, return None
    return None


def borda_condorcet():
    results = []

    for year in range(2014, 2025):
        for week in range(1, 18):
            try:
                # Find Borda winner
                borda_path = f'./src/college-polls/Borda/results/borda_top25/season_{year}/{year}_week{week}_top25.csv'
                borda_results = pd.read_csv(borda_path)
                borda_winner = borda_results['Teams'].iloc[0]
            except Exception as e:
                print(f"Error finding Borda winner for week {week}, year {year}: {e}")
                borda_winner = None

            try:
                condorcet_winner = find_condorcet_winner(week, year, top_n=3)
            except Exception as e:
                print(f"Error finding Condorcet winner for week {week}, year {year}: {e}")
                condorcet_winner = None

            if condorcet_winner is None:
                indicator = 1  # Condorcet winner does not exist
            elif borda_winner == condorcet_winner:
                indicator = 0  # Condorcet winner exists and is the same as Borda winner
            else:
                indicator = 2  # Condorcet winner exists but is not the same as Borda winner

            results.append({
                "Year": year,
                "Week": week,
                "Borda Winner": borda_winner,
                "Condorcet Winner": condorcet_winner,
                "Paradox": indicator
            })

    try:
        results_df = pd.DataFrame(results)
        results_df.to_csv("./src/college-polls/Pairwise/borda_condorcet_results_cf.csv", index=False)
    except Exception as e:
        print(f"Error saving results to CSV: {e}")


# Call the function
borda_condorcet()
