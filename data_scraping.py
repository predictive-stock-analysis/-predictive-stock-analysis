import os
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pyautogui
import re
from datetime import datetime
import csv

# stock = "RELIANCE"
# stock = "BSE"
# stock = "INFY"
# stock = "WIPRO"
# stock = "AXISBANK"
# stock = "TATASTEEL"
# stock = "ICICIBANK"
# stock = "MARUTI"
# stock = "ITC"
stock = "SUNPHARMA"

# date = "2023-10-17"
date = "2023-10-18"

os.environ["PATH"] += r"C:/SeleniumDrivers"
driver = webdriver.Chrome()
driver.implicitly_wait(30)

# url = 'https://www.google.com/finance/quote/RELIANCE:NSE'
# url = 'https://www.google.com/finance/quote/BSE:NSE'
# url = 'https://www.google.com/finance/quote/INFY:NSE'
# url = 'https://www.google.com/finance/quote/WIPRO:NSE'
# url = 'https://www.google.com/finance/quote/AXISBANK:NSE'
# url = 'https://www.google.com/finance/quote/TATASTEEL:NSE'
# url = 'https://www.google.com/finance/quote/ICICIBANK:NSE'
# url = 'https://www.google.com/finance/quote/MARUTI:NSE'
# url = 'https://www.google.com/finance/quote/ITC:NSE'
url = 'https://www.google.com/finance/quote/SUNPHARMA:NSE'

driver.get(url)

driver.implicitly_wait(30)

data_store = dict()
data_store[date] = {}

data_rows = []

csv_file_path = stock+" "+date+" data.csv"

pyautogui.moveTo(100, 900)

with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Stock', 'Date', 'Time', 'Price'])
    mov = 0
    while mov<900 and len(data_store[date])<375:
        mov+=1
        pyautogui.moveTo(100+mov, 900)
        # print(len(data_store[date]))
        data = driver.find_elements(By.CLASS_NAME, 'hSGhwc')
        try:
            data = data[0].text
        except:
            data = ""

        if data:
            data = re.split(" |\n|, |\u202f", data)

            date = data[2] + " " + data[3] + " " + data[4]
            input_format = "%b %d %Y"
            parsed_date = datetime.strptime(date, input_format)
            output_format = "%Y-%m-%d"
            date = parsed_date.strftime(output_format)

            time1 = data[5] + " " + data[6]
            input_format = "%I:%M %p"
            parsed_time = datetime.strptime(time1, input_format)
            output_format = "%H:%M"
            time1 = parsed_time.strftime(output_format)

            price = float(data[1][1:].replace(",", ""))

            if date not in data_store:
                data_store[date] = {}
            if time1 not in data_store[date]:
                data_store[date][time1] = price
                # print(i)
                print("Date:", date)
                print("Time:", time1)
                print("Price:", price)
                print(len(data_store[date]))
                # writer.writerow(data_row)
    
    # assuming data_store has data for a single date at a time
    data_store = data_store[date]
    data_store = list(data_store.items())
    data_store.sort()

    for i in data_store:
        writer.writerow([stock, date, i[0], i[1]])