#!/usr/bin/env python
# -*- coding: utf-8 -*-
import prefect
from prefect import task, Flow, Parameter, Task
from prefect.run_configs import LocalRun
from prefect.storage import Local, GitHub
from prefect.executors import DaskExecutor
from prefect.engine import state
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

FLOW_NAME = "flow-de-test"
storage = Local()
storage_github = GitHub(
    repo="azur-scd/prefect-flows",                            # name of repo
    path="flows/flow_de_test/flow.py",                    # location of flow file in repo
    #access_token_secret="prefect"   # name of personal access token secret
)

FLOW_PATH = "flows/flow_de_test"

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

def send_notification(obj, old_state, new_state):  
    now = datetime.now()
    msg = "{0} : new_state {1} starting at {2}".format(obj,new_state,now.strftime("%Y-%m-%d %H:%M:%S"))
    #requests.post("http://localhost:5000", json={"data": str(obj) +" : "+str(new_state.message)})
    requests.post("http://localhost:5000", json.dumps({'data': msg}))
    return new_state

#@task(log_stdout=True, name="generate_data", state_handlers=[send_notification])

@task(log_stdout=True, name="load_data")
def load_data() -> pd.DataFrame :
    df = pd.read_csv(f"{FLOW_PATH}/data/theses_test_processed.csv",sep=",", encoding="utf-8")
    print(df.shape)
    return df

@task(log_stdout=True, name="save_data")
def save_data(df) -> pd.DataFrame:
    df.to_csv(f"{FLOW_PATH}/data/theses_test_processed_2.csv", index=False, encoding='utf8')
    return df

@task(log_stdout=True, name="load_oai_data")
def load_oai_data() -> pd.DataFrame :
    df = scrapping_oai_sets_dewey()
    df.to_csv(f"{FLOW_PATH}/data/oai_sets.csv", index=False, encoding='utf8')
    return df

with Flow(name=FLOW_NAME) as flow:
    result = load_oai_data()


flow.register(project_name="projet_de_test")
flow.storage=storage_github
flow.run_config=LocalRun(labels=["dev"])
flow.run()

# Prefect cloud
## prefect backend cloud
## prefect auth login --key pcu_lcwhwE2WmL4tPYNaww47GDUuYWWkUZ3pQPKc
## prefect agent local start
# Pour enregister le flow dans Prefect Cloud : prefect register -p flows/my_flow.py --project MyProject

# prefect agent start -p ~/Developer/prefect/examples/tutorial