
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

def calculate_annee_civile (row):
    if int(row['mois_soutenance']) < 8 :
        return str(int(row['annee_civile_soutenance']) - 1) + "/" + str(row['annee_civile_soutenance'])
    else:
        return str(row['annee_civile_soutenance']) + "/" + str(int(row['annee_civile_soutenance']) + 1)

def days_between(row):
    if pd.notnull(row["embargo"]):
        d1 = datetime.strptime(str(row["date_soutenance"]), "%Y-%m-%d")
        d2 = datetime.strptime(row["embargo"], "%Y-%m-%d")
        return abs((d2 - d1).days)

def  scrapping_oai_sets_dewey():
    url = "https://www.theses.fr/schemas/tef/recommandation/oai_sets.html"
    resp = requests.get(url).text  # ou f = http.request('GET', url).data
    soup = BeautifulSoup(resp, features="lxml")
    oai_list = []
    for row in soup.findAll("table")[0].findAll("tr"):
        label = re.sub('<!--.*-->|\r|\n', '', str(row.findAll("td")[0].get_text(strip=True)), flags=re.DOTALL)
        label = re.sub('\s{2,}|&nbsp;', ' ', label)
        oai_list.append(
            {
                "label": label,
                "code": row.findAll("td")[1].get_text(strip=True),
            }
        )
    return oai_list