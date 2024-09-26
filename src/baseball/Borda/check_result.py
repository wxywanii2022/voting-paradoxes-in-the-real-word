import requests
from bs4 import BeautifulSoup
import pandas as pd

"""
This script scrapes MVP voting data from the BBWAA website, loads corresponding CSV data, and 
compares the two sets of data 
to identify discrepancies in the Borda point totals.

Input:
1. URLs for MVP data by year and league.
2. CSV files containing previously calculated Borda points for comparison.

Output:
Messages indicating whether the data from the website and the CSV files match for each league and year.

Functions:
- scrape_mvp_data(url): Scrapes player names and points from a specified webpage.
- load_csv_data(csv_file): Loads CSV data and converts it into a dictionary.
- compare_data(website_data, csv_data, league, year): Compares points from the website and CSV data.
- check_by_league(league): Iterates through the years, scraping and comparing data for a specified league.
"""


def scrape_mvp_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if table is None:
        raise ValueError("Table not found in the webpage. Check the HTML structure or the URL.")

    # Create a dictionary to store the data
    website_data = {}

    # Loop through each row in the table
    for row in table.find_all('tr')[1:]:  # Skip the header row
        columns = row.find_all('td')

        # Extract player data and points from the specified columns
        player_info = columns[0].text.strip()   # <td class="column-1">Mike Trout, Angels</td>
        points = int(columns[11].text.strip())  # <td class="column-12">385</td>

        website_data[player_info] = points

    return website_data


def load_csv_data(csv_file):
    df = pd.read_csv(csv_file)
    # Convert the CSV file data into a dictionary for easy comparison
    return dict(zip(df['Player'], df['Borda Points']))


def compare_data(website_data, csv_data, league, year):
    # Convert the points from website_data and csv_data to lists for sequential comparison
    website_points_list = list(website_data.values())
    csv_points_list = list(csv_data.values())
    
    if len(website_points_list) != len(csv_points_list):
        print(f'{league} {year} PROBLEM!!! Length mismatch between website and CSV data')
        return
    
    for i, (csv_points, website_points) in enumerate(zip(csv_points_list, website_points_list)):
        if csv_points != website_points:
            print(f'{league} {year} PROBLEM!!!')
            return
        
    print(f'{league} {year} good')


def check_by_league(league):
    for year in range(12, 24):
        url = f'https://bbwaa.com/{year}-{league}-mvp/'
        website_data = scrape_mvp_data(url)
        csv_file = f'./data/baseball/processed_data/Borda/results/{league.upper()}_{year}.csv'
        csv_data = load_csv_data(csv_file)
        compare_data(website_data, csv_data, league, year)


check_by_league("al")
check_by_league("nl")
