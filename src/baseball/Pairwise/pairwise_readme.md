# pairwise_results: 
  - Contains the output comparison results.

# cycle_finder.py
  - This script is used to detect voting cycles, which indicate the presence of voting paradoxes (i.e.where player A is preferred over player B, player B over player C, but player C is preferred over player A).
  - The script identifies and outputs any cycles found in the pairwise comparison data.

# cycles3, 4, 5.csv
  - Contains the results of the cycle detection, listing any voting paradoxes (cycles) that were found in the data. Each row details the players involved in the cycle and the number of voters that created the paradox.

# pairwise.py
  - pairwise_comparison(year, league): 
    - Outputs the pairwise comparison result for a certain year and league into a CSV file, sorted.
  - pairwise_comparison_all(): 
    - Outputs the pairwise comparison results for all years and leagues.
  - pairwise_comparison_specific(year, league, name_list): 
    - Outputs the pairwise comparison result between the people in `name_list`.