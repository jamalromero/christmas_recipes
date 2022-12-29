import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import timeit

print('Reading Json file ################')

recipes = pd.read_json("data/recipes.json.zip", lines=True)

recipes["num_ingredients"] = recipes['Ingredients'].map(len)

recipes["num_instructions"] = recipes['Method'].map(len)


def scrap_data(url):
    """Grab extra data using the url provided.
    Returns a dict having extra info like
    data_holder = {
        'kcal': np.nan,
        'fat': np.nan,
        'saturates': np.nan,
        'carbs': np.nan,
        'sugars': np.nan,
        'fibre': np.nan,
        'protein': np.nan,
        'salt': np.nan
        } 
    """
    data_holder = {
        'kcal': np.nan,
        'fat': np.nan,
        'saturates': np.nan,
        'carbs': np.nan,
        'sugars': np.nan,
        'fibre': np.nan,
        'protein': np.nan,
        'salt': np.nan}
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find(
            'table', {'class': 'key-value-blocks hidden-print mt-xxs'})
        elements = results.find_all(
            'tbody', {'class': 'key-value-blocks__batch body-copy-extra-small'})
        for element in elements:
            items = element.find_all('tr', {'class': 'key-value-blocks__item'})
            for item in items:
                key = item.find(
                    'td', {'class': 'key-value-blocks__key'}).text.strip()
                value = item.find(
                    'td', {'class': 'key-value-blocks__value'}).text.strip()
                value = re.sub(r'[^\d.]', '', value)
                if key == 'kcal':
                    data_holder['kcal'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'fat':
                    data_holder['fat'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'saturates':
                    data_holder['saturates'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'carbs':
                    data_holder['carbs'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'sugars':
                    data_holder['sugars'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'fibre':
                    data_holder['fibre'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'protein':
                    data_holder['protein'] = float(
                        value) if float(value) != 0 else np.nan
                elif key == 'salt':
                    data_holder['salt'] = float(
                        value) if float(value) != 0 else np.nan
    except:
        print('Exception happened in URL: ', url)
    return data_holder


def scrap_all():
    """Scrapping all by iterating through each row .
    Returns a new Dataframe where it will be merge with original frame
    """
    scrapped_data = []
    for _, row in recipes.iterrows():
        data_holder = scrap_data(row['url'])
        scrapped_data.append(data_holder)
    return pd.DataFrame(scrapped_data)


print('Start scrapping data')


scrapped_data = scrap_all()


pd.concat([recipes, scrapped_data], axis=1).to_csv("final_data.csv")
