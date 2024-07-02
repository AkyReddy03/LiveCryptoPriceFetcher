# Importing necessary libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time


# Define the base URL and headers for web scraping
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://finance.yahoo.com/',
    'Connection': 'keep-alive'
}

baseurl = "https://finance.yahoo.com/crypto/?count=25&offset="

# Define the initial offset and increment for pagination 
offset = 0
increment = 25
max_offset = 225  # We want to scrape crypto_data for max of 10 pages 

# List to store all tables from different pages
all_tables = []

# Loop through the pages to scrape data
while offset <= max_offset:
    url = baseurl + str(offset)
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = pd.read_html(str(soup))

        # Append the first table on the page to the list
        if tables:
            all_tables.append(tables[0])
            print(f"Page {offset // increment + 1} scraped successfully.")
        else:
            print(f"No tables found on page {offset // increment + 1}.")
    else:
        print(f"Web scraping failed, status code {response.status_code}")

    # Increment the offset for the next page and sleep to avoid overloading the server
    offset += increment
    time.sleep(1)

# Concatenate all the scraped tables into one DataFrame
data = pd.concat(all_tables, ignore_index=True)

# Drop unnecessary columns
columns_to_drop = [
    'Change', 'Volume in Currency (Since 0:00 UTC)',
    'Total Volume All Currencies (24Hr)', '52 Week Range', 'Day Chart'
]
data = data.drop(columns=columns_to_drop)

# Remove 'USD' at the end of each cryptocurrency name
data['Name'] = data['Name'].str.replace(r'\s*USD$', '', regex=True)

# Rename columns for better readability
data = data.rename(columns={
    'Price (Intraday)': 'Price',
    'Volume in Currency (24Hr)': 'Volume'
})


# Save the cleaned DataFrame to a CSV file
data.to_csv("Crypto_Currencies_cleaned.csv", index=False)


