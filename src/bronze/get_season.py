from selenium import webdriver # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.support.ui import Select, WebDriverWait # type: ignore
from selenium.webdriver.support import expected_conditions as EC # type: ignore
import os
import sys
from pathlib import Path
from utils.config import load_variables
from utils.logger import config_log
import logging

BASE_URL = 'https://wutools.com/es/tiempo/calendario-estaciones'

def open_chrome() -> webdriver.Chrome:
    driver = webdriver.Remote(
        command_executor='http://chrome:4444/wd/hub',
        options=webdriver.ChromeOptions()
    )

    return driver

def close_chrome(driver: webdriver.Chrome):
    driver.quit()

def get_season(logger: logging.Logger) -> str:
    season = ''

    driver = open_chrome()
    driver.get(BASE_URL)

    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, 'inputYear'))
    )

    for year in range(1900, 2100):
        logger.info('\tExtracting season data for year: ' + str(year))

        season = season + str(year) + '\n'

        input_year = driver.find_element(By.ID, 'inputYear')
        input_year.clear()
        input_year.send_keys(year)

        select_hemisphere = Select(driver.find_element(By.ID, "inputHemisphere"))
        select_hemisphere.select_by_value('southern')

        driver.find_element(By.XPATH, '//button[text()=" Generar Calendario"]').click()

        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, 'springEquinox'))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "div.info-row")
        for r in rows:
            season = season + r.text + '\n'

        driver.find_element(By.XPATH, '//button[text()=" Restablecer"]').click()

        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.ID, 'springEquinox'))
        )

    close_chrome(driver)

    CONFIG_VARS = load_variables()

    path_txt = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'season.txt')
    with open(path_txt, 'w') as f:
        f.write(season)

    return path_txt
