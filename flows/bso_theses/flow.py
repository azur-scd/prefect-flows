#!/usr/bin/env python
# -*- coding: utf-8 -*-
import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import Local, GitHub
from prefect.executors import DaskExecutor
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import functions as fn

FLOW_NAME = "flow-bso-theses"
#storage = GitHub(repo="azur-scd/prefect-flows", path="flows/barometre_theses/flow.py")
storage = Local()

def send_notification(obj, old_state, new_state):
    """Sends a POST request with the error message if task fails"""
    if new_state.is_failed():
        requests.post("http://example.com", json={"error_msg": str(new_state.result)})
    return new_state

#@task(log_stdout=True, name="load_data", state_handlers=[send_notification])
@task(log_stdout=True, name="load_data")
def load_data() -> pd.DataFrame :
    df = pd.read_csv("https://www.data.gouv.fr/fr/datasets/r/eb06a4f5-a9f1-4775-8226-33425c933272",sep=",", encoding="utf-8")
    print(df.shape)
    return df

#@task(log_stdout=True, name="extract_data", state_handlers=[send_notification])
@task(log_stdout=True, name="extract_data")
def extract_data(df,etabs) -> pd.DataFrame:
    appended_data = []
    for i in etabs:
        data = df[df["code_etab"] == str(i)]
        appended_data.append(data)
    appended_data = pd.concat(appended_data)
    return appended_data.convert_dtypes()


#@task(log_stdout=True, name="scrap_oai_data", state_handlers=[send_notification])
@task(log_stdout=True, name="scrap_oai_data")
def scrap_oai_data() -> list:
    oai_list = fn.scrapping_oai_sets_dewey()
    #with open('flows/barometre_theses/data/oai_sets_dewey.json', 'w+', 'utf-8') as fp:
    #    json.dump(oai_list, fp)
    return oai_list[1:]


#@task(log_stdout=True, name="transform_data", state_handlers=[send_notification])
@task(log_stdout=True, name="transform_data", state_handlers=[send_notification])
def transform_data(df_load_extract,oai) -> pd.DataFrame:
    #keep only relevant columns and rename
    df_tmp = df_load_extract[['accessible', 'auteurs.0.idref', 'auteurs.0.nom', 'auteurs.0.prenom', 'cas', 'code_etab', 'date_soutenance', 'directeurs_these.0.idref', 'directeurs_these.0.nom', 'directeurs_these.0.prenom', 'directeurs_these.1.idref', 'directeurs_these.1.nom', 'directeurs_these.1.prenom', 'discipline.fr', 'ecoles_doctorales.0.nom', 'embargo', 'etablissements_soutenance.0.idref', 'etablissements_soutenance.1.idref', 'etablissements_soutenance.0.nom', 'etablissements_soutenance.1.nom', 'iddoc', 'langue', 'nnt', 'oai_set_specs', 'partenaires_recherche.0.idref', 'partenaires_recherche.0.nom', 'partenaires_recherche.0.type', 'partenaires_recherche.1.idref', 'partenaires_recherche.1.nom', 'partenaires_recherche.1.type', 'partenaires_recherche.2.idref', 'partenaires_recherche.2.nom', 'partenaires_recherche.2.type', 'resumes.fr', 'source', 'these_sur_travaux', 'titres.fr', 'status']]
    df = df_tmp.set_axis([w.replace('.', '_') for w in df_tmp.columns], axis=1, inplace=False)
    df = df.rename(columns={'accessible': 'open_access'})
    #new col année civile de soutenance
    df["annee_civile_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[0])
    #new col mois de soutenance
    df["mois_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[1])
    #new col année universitaire de soutenance)
    df["annee_univ_soutenance"] = df.apply(lambda row: fn.calculate_annee_civile(row), axis=1)
    #clean ecoles doctorales variable with fillna
    df[['ecoles_doctorales_0_nom']] = df[['ecoles_doctorales_0_nom']].fillna('non renseignée')
    #new col calcul de la durée (en jours) d'embargo
    df["embargo_duree"] = df.apply(lambda row: fn.days_between(row), axis=1)
    df[['embargo_duree']] = df[['embargo_duree']].fillna(0)
    #new col variable binaire is_embargo oui/non
    df['has_exist_embargo'] = df['embargo']
    tmp_condition = df['has_exist_embargo'].isna()
    df.loc[tmp_condition, 'has_exist_embargo'] = 'non'
    df.loc[~tmp_condition, 'has_exist_embargo'] = 'oui'
    #split oai_sets_specs in two columns for the multivalues
    split_df = df['oai_set_specs'].str.split('\|\|', expand=True)
    split_df.columns = ['oai_set_specs' + f"_{id_}" for id_ in range(len(split_df.columns))]
    df = pd.merge(df, split_df, how="left", left_index=True, right_index=True)
    df = df.drop(columns=['oai_set_specs_2', 'oai_set_specs_3'])
    #new cols with disc label 
    df['oai_set_specs_0_label'] = df['oai_set_specs_0'].apply(lambda x: [y['label'] for y in oai if y['code'] == str(x)][0])
    df['oai_set_specs_1_label'] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x: [y['label'] for y in oai if y['code'] == str(x)][0])
    #new cols with disc regroup in main dewey classes (dcc codes and labels)
    df["oai_set_specs_0_regroup"] = df['oai_set_specs_0'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_0_regroup_label'] = df['oai_set_specs_0_regroup'].apply(lambda x: [y['label'] for y in oai if y['code'] == str(x)][0])
    df["oai_set_specs_1_regroup"] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_1_regroup_label'] = df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_1_regroup'].apply(lambda x: [y['label'] for y in oai if y['code'] == str(x)][0])
    #new col with regroup disc label concaténés
    #df['oai_set_specs_all_regroup_label'] = ""
    #df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_all_regroup_label'] = df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_0_regroup_label'] +"|"+ df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_1_regroup_label']
    #df[df['oai_set_specs_1_regroup'].isna()]['oai_set_specs_all_regroup_label'] = df[df['oai_set_specs_1_regroup'].isna()]['oai_set_specs_0_regroup_label']
    #manage cols type
    for column_name in df.columns:
        df[column_name] = df[column_name].astype('string')
    df['embargo_duree'] = pd.to_numeric(df['embargo_duree'], downcast='integer', errors='coerce')
    #manage all nan values
    df = df.fillna('')
    df.to_csv("data/03_primary/theses_uca_processed.csv", index=False, encoding='utf8')
    return df

with Flow(name=FLOW_NAME) as flow:
    etabs = Parameter('etabs',default = ['NICE','AZUR','COAZ'])
    load_result = load_data()
    extract_result = extract_data(load_result,etabs)
    scrap_result = scrap_oai_data()
    result = transform_data(extract_result,scrap_result)

flow.register(project_name="barometre")
flow.storage=storage
flow.run_config=LocalRun()
flow.run(etabs=['NICE','AZUR','COAZ'])

#https://docs.prefect.io/core/advanced_tutorials/task-guide.html#state-handlers
