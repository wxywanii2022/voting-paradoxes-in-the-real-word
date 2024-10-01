# Borda

## results: 
  - Contains calculated Borda points for every possible/valid combination of season and week.

## Borda_count.ipynb
  - borda_count_ap_polls(weights, year, week, output_filename):: 
    - Takes in a data file from a specific week in a some year, along with the weights array. It calculates the Borda points for each team for the corresponding week in that season. It then outputs the Borda points for each team for each that week  and year as a CSV file.

## original_borda_count.ipynb
  - org_borda_count_dictionary(Week, url, short_szn=True): 
    - Gets data about a team and its Borda Count from the website, storing the output as a dictionary
  - csv_data_writer_by_year_and_week(year, Week, num_week):
    - Calls `org_borda_count_dictionary` on all valid combos of week and season 
    - Takes the output of `org_borda_count_dictionary` and writes it to the CSV file corresponding with that week and season
  - csv_creation():
    - Passes values of week and season to `csv_data_writer_by_year_and_week`, allowing it to create all the files we want
    - Currently is hard-coded to not include any weeks after week 5 in the 2024 season