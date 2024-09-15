import requests
from bs4 import BeautifulSoup
import pandas as pd

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
    for i, (csv_player, csv_points) in enumerate(csv_data):
        website_player, website_points = website_data[i]

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
