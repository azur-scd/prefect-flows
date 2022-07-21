import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

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
    df = pd.DataFrame(oai_list[1:])
    df['main_domain'] = df['code'].apply(lambda x: 'Sciences, Technologies, Santé' if ((str(x[4]) == "5") | (str(x[4]) == "6") | (str(x[4:7]) == "004")) else 'Lettres, sciences Humaines et Sociales')
    return df