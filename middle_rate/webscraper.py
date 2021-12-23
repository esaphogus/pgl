import os
from time import sleep
import json
from pytz import timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import urllib as ul
import requests as rq
import os
import pandas as pd
import datetime

TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND


# !/usr/bin/env python
# coding: utf-8

# Need to download Chrome Driver here:https://chromedriver.chromium.org/
def set_chrome_options():
    """Sets chrome options for Selenium.
    Chrome options for headless browser is enabled.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    return chrome_options


def get_rates():

    url = "https://www.bi.go.id/id/statistik/indikator/bi-7day-rr.aspx"
    # create a new Chrome session
    chrome_options = set_chrome_options()
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(30)
    driver.get(url)

    dataRow = []

    # Number of pages to parse
    num_pages = 7

    #  Parsing HTML of each Web Page
    for i in range(1, num_pages):
        resourceHtml = driver.page_source
        soup = BeautifulSoup(resourceHtml, "html.parser")
        pageContainer = soup.find_all('div', id='ctl00_ctl44_g_4dbaa7f5_3c71_4542_af75_185912ff0c39')
        tableMidRate = pageContainer[0].find_all('table')[0]
        rowsMidRate = list(list(tableMidRate.children)[3].children)

        # Modification from Raw HTML into list and dataframe
        for rows in rowsMidRate:
            if rows != '\n': # Excludes blank lines in page
                row = list(rows.children)
                data_line = []
                for line in row:
                    if line != '\n': # Excludes blank lines in page
                        text_line = line.get_text()
                        if '\n' not in text_line: # To get only Tanggal and Middle Rate
                            data_line.append(text_line)
                dataRow.append(data_line)

        # Click next page button to parse next page
        name = 'ctl00$ctl44$g_4dbaa7f5_3c71_4542_af75_185912ff0c39$ctl00$DataPagerBI7DRR$ctl02$ctl00'
        next_page_button = driver.find_element_by_name(name)
        driver.execute_script("arguments[0].click();", next_page_button)

    # Modification list into dataframe
    dr = pd.DataFrame(dataRow, columns=['Tanggal', 'Middle Rate'])
    dr['json'] = dr.apply(lambda x: x.to_json(), axis=1)

    driver.quit()
    return dr['json'].values

# Add here to create flow for dataframe to be inserted to your tables


if __name__ == '__main__':
    # producer = KafkaProducer(
    #     bootstrap_servers=KAFKA_BROKER_URL,
    #     # Encode all values as JSON
    #     value_serializer=lambda value: json.dumps(value).encode(),
    # )

    start_date = datetime.datetime.now() - datetime.timedelta(days=1)
    values = get_rates()

    while True:
        for val in values:
            # producer.send(TRANSACTIONS_TOPIC, value=val)
            print(val)  # DEBUG
        # sleep(3600)
