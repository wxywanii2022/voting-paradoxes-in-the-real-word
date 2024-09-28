# processed_data

- ## auxiliary_files
- Houses all auxiliary files related to the overall data

  - ### every_voter_and_voted_team:
    
    - #### all_voted_teams.csv:
    - List all teams that appeared in at least one voters ballot.

    - #### all_voters.csv
    - List all voters that casted at least one ballot.

    - #### retrieve_every_voter_and_voted_team.ipynb
    - Python script that pulls the voter and voted teams list from the entire data CSV
    - Ensures that both lists are sorted and implemented as sets so there are no repetitions
    - Fills the CSVs with voters and voted teams

  - ### voted_teams_by_season_and_week
  - Lists all teams that received a vote in each week of a season, separated by season and week (i.e. 1000 ballots)
    
    - #### retrieve_voters_by_season_and_year.ipynb
    - Python script that generates an ordered set of teams that appeared on at least one ballot for each season and week
  
  - ### voters_by_season_and_week
  - Lists all voters that voted in at least one ballot for each season and week

    - #### retrieve_voters_by_season_and_year
    - Python script that uses CSV to generate voter list for each week/season

- ## ballot_data_by_season_and_week
- Separated ballot data that displays the full ballot for each week/election of voting

  - ### ballot_scraper_by_year_and_week
  - Scrapes ballot data from the entire data, and separates it by season and week creating a separate csv for each season/week

- ## entire_ballot_data
- Lists the entire ballot data for every season and week in one CSV, i.e. generating a 10,000 line CSV
  
  - ### college_poll_data_scrape.ipynb
  - Python script that web scrapes from the collegepolltracker.com site using Beautiful Soup
  - Generates a dictionary dataset which all work is done through
  - Generates all csvs through dictionaries and dataframes