import requests
from bs4 import BeautifulSoup
import csv
import os

# define the function for scraping data with a given url
def scrape_data(url):
    response = requests.get(url)
    if response.status_code == 200:  # request successful
        soup = BeautifulSoup(response.content, 'html.parser')  # parse the html content of website using parser
        table = soup.find('table')  # get the <table> element
        
        if table:
            headers = [th.text.strip() for th in table.find_all('th')]  # extract headers <th>
            rows = []
            for tr in table.find_all('tr')[1:]:  # go over each row, excluding the first one (header)
                row = [td.text.strip() for td in tr.find_all('td')]  # extracting the cells <td>
                rows.append(row)
            return headers, rows
        else:
            print(f"No table found on {url}")
            return None, None
    else:
        print(f"Failed to retrieve {url}. Status code: {response.status_code}")
        return None, None

years = range(12, 24)

directory = './data/baseball/raw_data/separate'  # the directory to store the data separately
directory2 = './data/baseball/raw_data'  # the directory to store the merged data

os.makedirs(directory, exist_ok=True)  # check if this directory exists

merged_rows = []  # list to store all data for the merged CSV
merged_headers = None  # variable to store the merged headers

for year in years:
    al_url = f'https://bbwaa.com/{year}-al-mvp-ballots/'
    nl_url = f'https://bbwaa.com/{year}-nl-mvp-ballots/'

    # Scraping AL data
    al_headers, al_rows = scrape_data(al_url)
    if al_headers and al_rows:
        al_file_name = os.path.join(directory, f'al_mvp_ballots_{year}.csv')  # construct the output file path
        with open(al_file_name, mode='w', newline='', encoding='utf-8') as al_file:  # set the mode to write
            al_writer = csv.writer(al_file)
            al_writer.writerow(al_headers)
            al_writer.writerows(al_rows)
        print(f"AL data for {year} saved to {al_file_name}")

        # Prepare for merging
        if not merged_headers:  # Initialize the merged headers once
            merged_headers = ['Year', 'League'] + al_headers
        for row in al_rows:
            merged_rows.append([f"20{year}", 'AL'] + row)  

    # Scraping NL data
    nl_headers, nl_rows = scrape_data(nl_url)
    if nl_headers and nl_rows:
        nl_file_name = os.path.join(directory, f'nl_mvp_ballots_{year}.csv')
        with open(nl_file_name, mode='w', newline='', encoding='utf-8') as nl_file:
            nl_writer = csv.writer(nl_file)
            nl_writer.writerow(nl_headers)
            nl_writer.writerows(nl_rows)
        print(f"NL data for {year} saved to {nl_file_name}")

        # Prepare for merging
        for row in nl_rows:
            merged_rows.append([f"20{year}", 'NL'] + row) 

# Write the merged CSV file
merged_file_name = os.path.join(directory2, 'mvp_ballots_original.csv')
with open(merged_file_name, mode='w', newline='', encoding='utf-8') as merged_file:
    merged_writer = csv.writer(merged_file)
    merged_writer.writerow(merged_headers)  # Write the headers
    merged_writer.writerows(merged_rows)  # Write all the rows
print(f"Merged data saved to {merged_file_name}")
