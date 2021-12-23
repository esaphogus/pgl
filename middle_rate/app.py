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
#!/usr/bin/env python
# coding: utf-8

#Need to download Chrome Driver here:https://chromedriver.chromium.org/
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
    url = "https://www.bi.go.id/id/statistik/informasi-kurs/transaksi-bi/default.aspx"
    # create a new Chrome session
    chrome_options = set_chrome_options()
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(30)
    driver.get(url)

    #Kindly create modification for this time to be used as you need
    date = datetime.datetime.now().replace(tzinfo=timezone('UTC'))
    dateUsed = str(date.strftime("%D:%b:%Y"))

    inputTanggal = driver.find_element_by_name('ctl00$PlaceHolderMain$g_6c89d4ad_107f_437d_bd54_8fda17b556bf$ctl00$txtTanggal')
    inputTanggal.clear()
    inputTanggal.send_keys(dateUsed)
    inputTanggal.send_keys(Keys.RETURN)

    cariBtn = driver.find_element_by_name("ctl00$PlaceHolderMain$g_6c89d4ad_107f_437d_bd54_8fda17b556bf$ctl00$btnSearch2")
    cariBtn.click()

    resourceHtml = driver.page_source

    #Parsing HTML from Web Page
    soup = BeautifulSoup(resourceHtml, "html.parser")
    divMidRate = soup.find_all('div', id ='ctl00_PlaceHolderMain_g_6c89d4ad_107f_437d_bd54_8fda17b556bf_ctl00_kursTable1')

    tanggalMidRate = divMidRate[0].find_all('span')[1].get_text()

    tableMidRate = divMidRate[0].find_all('table')[0]
    rowMidRate = list(list(tableMidRate.children)[1].children)

    # Modification from Raw HTML into list and dataframe 
    dataRow = []

    for row in rowMidRate:
        if row != '\n': #to cancel last children
            col = list(row.children)
            if list(col[3].children)[0]!='Kurs Jual': #to ignore row with column name
                
                tmp1 = list(col[3].children)[0].replace('.','')
                tmp2 = tmp1.replace(',','.')
                kursJual = float(tmp2)

                tmp1 = list(col[4].children)[0].replace('.','')
                tmp2 = tmp1.replace(',','.')
                kursBeli = float(tmp2)

                dataRow.append([list(col[1].children)[0].strip(), list(col[2].children)[0], kursJual, kursBeli, (kursJual+kursBeli)/2, str(date.strftime("%Y-%m-%d %H:%M:%S"))])
        
    print(dataRow)

    #modification list into dataframe
    dr = pd.DataFrame(dataRow,columns=['Mata Uang','Nilai','Kurs Jual','Kurs Beli','Middle Rate','Tanggal'])
    dr['json'] = dr.apply(lambda x: x.to_json(), axis=1)
    
    driver.quit()
    return dr['json'].values

#Add here to create flow for dataframe to be inserted to your tables



if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    start_date = datetime.datetime.now() - datetime.timedelta(days=1)
    values = get_rates()

    while True:
        for val in values:
            producer.send(TRANSACTIONS_TOPIC, value=val)
            print(val)  # DEBUG
        sleep(3600)

    