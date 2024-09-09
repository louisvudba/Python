from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def solution(url):
    chrome_options = Options()
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(options = chrome_options)    
    driver.get(url)

    detail = ""
    properties = ""

    time.sleep(3)
    elements = driver.find_elements(By.XPATH, "//div[@class='detail']//span")
    for e in elements:    
        detail += e.get_attribute('innerText')

    elements = driver.find_elements(By.XPATH, "//div[@class='property']//div[@class='options']//span")
    for e in elements:    
        properties += e.get_attribute('innerText') + " "

    driver.quit()
    return detail, properties


