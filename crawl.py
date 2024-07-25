import numpy as np
from time import sleep
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementNotSelectableException, TimeoutException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random

def collect_data_from_page(driver):
    # lấy link và title
    elems = driver.find_elements(By.CSS_SELECTOR, '.RfADt [href]')
    title = [elem.text for elem in elems]
    links = [elem.get_attribute('href') for elem in elems]

    elems_price = driver.find_elements(By.CSS_SELECTOR , ".aBrP0")
    price = [elem_price.text for elem_price in elems_price]

    df1 = pd.DataFrame(list(zip(title, price, links)), columns=['title', 'price', 'link_item'])
    df1['index_'] = np.arange(1, len(df1) + 1)

    # Lấy giảm giá, lượt bán, đánh giá, địa chỉ
    discount_list, discount_idx = [], []
    sold_list, evaluate_list, location_list = [], [], []

    for i in range(1, len(title) + 1):
        try:
            # discount
            discounts = driver.find_elements(By.XPATH, f"/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{i}]/div/div/div[2]/div[4]/span[1]")                                                                                                 
            if discounts:
                for discount in discounts:
                    discount_list.append(discount.text)
                    discount_idx.append(i)
            else:
                discount_list.append(None)
                discount_idx.append(i)

            # solds
            solds = driver.find_elements(By.XPATH, f"/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{i}]/div/div/div[2]/div[5]/span[1]/span[1]")
            if solds:
                for sold in solds:
                    sold_list.append(sold.text)
            else: 
                sold_list.append(None)
                
            # evaluates
            evaluates = driver.find_elements(By.XPATH, f"/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{i}]/div/div/div[2]/div[5]/div/span")
            if evaluates:
                for evaluate in evaluates:
                    evaluate_list.append(evaluate.text)
            else:
                evaluate_list.append(None)                                       
            
            # locations
            locations = driver.find_elements(By.XPATH, f"/html/body/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/div[{i}]/div/div/div[2]/div[5]/span[2]")   
            if locations:
                for location in locations:
                    location_list.append(location.text)
                else: 
                    location_list.append(None)       

        except NoSuchElementException:
            print("No Such Element Exception " + str(i))

    df2 = pd.DataFrame(list(zip(discount_idx, discount_list, sold_list, evaluate_list, location_list)), 
                       columns=('discount_id', 'discount', 'sold', 'evaluate', 'location'))

    df3 = df1.merge(df2, how='left', left_on='index_', right_on='discount_id')
    df3 = df3.drop(columns=['discount_id', 'index_'])

    return df3

def collect_data_from_pages(num_pages):
    chrome_driver_path = 'C:/Program Files/Google/Chrome/Application/chromedriver.exe'
    service = Service(chrome_driver_path)
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:/Program Files/Google/Chrome/Application/chrome.exe'
    driver = webdriver.Chrome(service=service, options=options)

    all_data = []

    for page in range(1, num_pages + 1):
        url = f'https://www.lazada.vn/trang-phuc-nam/?spm=a2o4n.searchlist.breadcrumb.3.2d8072872Fq1QG&page={page}'
        driver.get(url)
        sleep(random.randint(3, 5))

        # Collect data from the current page
        page_data = collect_data_from_page(driver)
        if page_data is not None and not page_data.empty:
            all_data.append(page_data)

    driver.quit()

    if all_data:
        df4 = pd.concat(all_data, ignore_index=True)
        return df4
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data was collected

# Số trang muốn thu thập dữ liệu
num_pages = 25
df5 = collect_data_from_pages(num_pages)

# Thêm thời gian 
current_time = datetime.now()

df5['timestamp'] = current_time

# Kiểm tra dữ liệu
#print(df5)

# Đường dẫn 
file_path = 'C:/Users/chihi/OneDrive/Desktop/report/data/lazada_data.csv'

# Lưu DataFrame dưới dạng CSV
df5.to_csv(file_path, index=False)

