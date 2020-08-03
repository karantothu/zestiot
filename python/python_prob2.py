
__author__  = 'Vinod Karantothu'
__date___   = '31-07-2020'

"""
Write a script to scrap data for every 30 mins from https://www.airportia.com/india/rajiv-gandhi-airport/arrivals/ and same data to database without duplications.

Instructions:
    Please install all dependencies from requiments.txt
    Make sure that you have the Chrome Web driver('chromedriver.exe') file in the same current working directory.
    This script is written for Windows OS with Chrome browser installed. If want to run the script on Linux or Mac pls replace the web driver with the compitable one.  
"""

# import the libraries
import os
import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def scrape(browser, url):
    """Function to scrape data from web page."""
    browser.get(url)
    html_content = browser.page_source
    time.sleep(5) # Let the page load and run JavaScript content!
    # browser.quit()

    # Parse the html content
    soup = BeautifulSoup(html_content, "lxml")
    # Get the table content
    table = soup.find("table", attrs={"class": "flightsTable flightsTable--airportArrivals"})

    data = {}

    # read all table headers
    for th in table.find_all("th"):
        data[th.text.replace('\n', ' ').strip()] = []

    # iterate over the table rows
    for tr in table.find_all("tr"):
        
        # iterate over cells in a row
        for td, key in zip(tr.find_all("td"), data.keys()):
            
            if key=='Flight' and td.text.replace('\n', '').strip()=='':
                continue
            
            data[key].append(td.text.replace('\n', '').strip())

    # load data to a pandas.DataFrame
    df = pd.DataFrame(data)

    return df.iloc[:, :-1]

def write_data_to_db(conn, df):
    """Function to write data to the database."""
    df.to_sql('arrivals', con=engine, if_exists='replace')


if __name__ == '__main__':
    # webpage url to scrape
    URL_= 'https://www.airportia.com/india/rajiv-gandhi-airport/arrivals/'
    # path to webdriver
    chrome_driver_path= os.path.join(os.getcwd(), 'chromedriver.exe')

    # browser options
    Options = webdriver.ChromeOptions()
    Options.add_argument("headless")

    browser = webdriver.Chrome(chrome_driver_path, options=Options)
    
    
    #engine = create_engine('mysql://root:password@localhost:3306/flights')
    engine = create_engine('sqlite:///flights.db', echo = False) # set echo=True to see sqlalchemy actions
    # conn = engine.connect()

    while(True):
        try:
            df = scrape(browser, URL_)
        except Exception as err:
            print(err.__str__)
        else:
            write_data_to_db(engine, df)
            time.sleep(5)                
        finally:
            results = engine.execute("SELECT * FROM arrivals").fetchall()
            print(results)
            time.sleep(1800) #sleep for 30mins=1800secs
            
