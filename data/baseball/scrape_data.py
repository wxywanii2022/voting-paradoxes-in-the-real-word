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
            for tr in table.find_all('tr')[1:]:  # go over each row, excluding the first one(header)
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

directory = './data/baseball'  # the directory to store the data

os.makedirs(directory, exist_ok=True)   # check if this directory exists

for year in years:
    al_url = f'https://bbwaa.com/{year}-al-mvp-ballots/'
    nl_url = f'https://bbwaa.com/{year}-nl-mvp-ballots/'
    
    al_headers, al_rows = scrape_data(al_url)
    if al_headers and al_rows:
        al_file_name = os.path.join(directory, f'al_mvp_ballots_{year}.csv')   # construct the output file path
        with open(al_file_name, mode='w', newline='', encoding='utf-8') as al_file:   # set the mode to write
            al_writer = csv.writer(al_file)
            al_writer.writerow(al_headers) 
            al_writer.writerows(al_rows) 
        print(f"AL data for {year} saved to {al_file_name}")
    
    nl_headers, nl_rows = scrape_data(nl_url)
    if nl_headers and nl_rows:
        nl_file_name = os.path.join(directory, f'nl_mvp_ballots_{year}.csv')
        with open(nl_file_name, mode='w', newline='', encoding='utf-8') as nl_file:
            nl_writer = csv.writer(nl_file)
            nl_writer.writerow(nl_headers)  
            nl_writer.writerows(nl_rows) 
        print(f"NL data for {year} saved to {nl_file_name}")
