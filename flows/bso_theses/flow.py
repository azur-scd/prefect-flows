#!/usr/bin/env python
# -*- coding: utf-8 -*-
import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import Local, GitHub
from prefect.executors import DaskExecutor
from prefect.engine import state
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import functions as fn

FLOW_NAME = "flow-bso-theses"
#storage = GitHub(repo="azur-scd/prefect-flows", path="flows/barometre_theses/flow.py")
storage = Local()

FLOW_PATH = "flows/bso_theses"

def send_notification(obj, old_state, new_state):  
    msg = "Calling my custom state handler on {0} : {1} to {2}"
    #requests.post("http://localhost:5000", json={"data": str(obj) +" : "+str(new_state.message)})
    requests.post("http://localhost:5000", json.dumps({'data': msg.format(obj, old_state, new_state)}))
    return new_state

#@task(log_stdout=True, name="load_data", state_handlers=[send_notification])
@task(log_stdout=True, name="load_data", state_handlers=[send_notification])
def load_data() -> pd.DataFrame :
    df = pd.read_csv("https://www.data.gouv.fr/fr/datasets/r/eb06a4f5-a9f1-4775-8226-33425c933272",sep=",", encoding="utf-8")
    print(df.shape)
    return df

#@task(log_stdout=True, name="extract_data", state_handlers=[send_notification])
@task(log_stdout=True, name="extract_uca_data", state_handlers=[send_notification])
def extract_uca_data(df,etabs_uca) -> pd.DataFrame:
    df_tmp = df[df.source == "star"][['accessible', 'auteurs.0.idref', 'auteurs.0.nom', 'auteurs.0.prenom', 'cas', 'code_etab', 'date_soutenance', 'directeurs_these.0.idref', 'directeurs_these.0.nom', 'directeurs_these.0.prenom', 'directeurs_these.1.idref', 'directeurs_these.1.nom', 'directeurs_these.1.prenom', 'discipline.fr', 'ecoles_doctorales.0.nom', 'embargo', 'etablissements_soutenance.0.idref', 'etablissements_soutenance.1.idref', 'etablissements_soutenance.0.nom', 'etablissements_soutenance.1.nom', 'iddoc', 'langue', 'nnt', 'oai_set_specs', 'partenaires_recherche.0.idref', 'partenaires_recherche.0.nom', 'partenaires_recherche.0.type', 'partenaires_recherche.1.idref', 'partenaires_recherche.1.nom', 'partenaires_recherche.1.type', 'partenaires_recherche.2.idref', 'partenaires_recherche.2.nom', 'partenaires_recherche.2.type','titres.fr']]
    appended_data = []
    for i in etabs_uca:
        data = df_tmp[df_tmp["code_etab"] == str(i)]
        appended_data.append(data)
    appended_data = pd.concat(appended_data)
    return appended_data.convert_dtypes()

@task(log_stdout=True, name="extract_fr_data", state_handlers=[send_notification])
def extract_fr_data(df) -> pd.DataFrame:
    df_tmp = df[df.source == "star"][['accessible', 'cas', 'code_etab', 'date_soutenance','discipline.fr','etablissements_soutenance.1.nom','embargo','langue', 'nnt', 'oai_set_specs']]
    return df_tmp.convert_dtypes()

@task(log_stdout=True, name="extract_udice_data", state_handlers=[send_notification])
def extract_udice_data(df,etabs_udice) -> pd.DataFrame:
    appended_data = []
    for i in etabs_udice:
        data = df[df["code_etab"] == str(i)]
        appended_data.append(data)
    appended_data = pd.concat(appended_data)
    return appended_data.convert_dtypes()


@task(log_stdout=True, name="read_oai_data", state_handlers=[send_notification])
def read_oai_data() -> list:
    df = pd.read_csv(f"{FLOW_PATH}/data/02_intermediate/oai_set_specs_dewey_labels.csv", sep=",", encoding='utf8')
    return df.to_dict('records')

@task(log_stdout=True, name="scrap_oai_data", state_handlers=[send_notification])
def scrap_oai_data() -> list:
    df = fn.scrapping_oai_sets_dewey()
    df.to_csv(f"{FLOW_PATH}/data/02_intermediate/oai_set_specs_dewey_labels.csv", index=False, encoding='utf8')
    return df.to_dict('records')

@task(log_stdout=True, name="clean_column_names", state_handlers=[send_notification])
def clean_column_names(df) -> pd.DataFrame:
    df = df.set_axis([w.replace('.', '_') for w in df.columns], axis=1, inplace=False)
    return df

@task(log_stdout=True, name="clean_ending_data", state_handlers=[send_notification])
def clean_ending_data(df) -> pd.DataFrame:
    for column_name in df.columns:
        df[column_name] = df[column_name].astype('string')
        if column_name == "ecoles_doctorales_0_nom":
            df[['ecoles_doctorales_0_nom']] = df[['ecoles_doctorales_0_nom']].fillna('non renseigné')
    df['embargo_duree'] = pd.to_numeric(df['embargo_duree'], downcast='integer', errors='coerce')
    df = df.fillna('')
    return df

@task(log_stdout=True, name="create_oa_variables", state_handlers=[send_notification])
def create_oa_variables(df) -> pd.DataFrame:
    df["is_oa_normalized"] = df["accessible"]
    df['is_oa_normalized'] = df['is_oa_normalized'].replace(['oui','non'],['Accès ouvert','Accès fermé'])
    return df

@task(log_stdout=True, name="create_date_variables", state_handlers=[send_notification])
def create_date_variables(df) -> pd.DataFrame:
    df["annee_civile_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[0])
    df["mois_soutenance"] = df['date_soutenance'].apply(lambda x: x.split("-")[1])
    df["annee_univ_soutenance"] = df.apply(lambda row: fn.calculate_annee_civile(row), axis=1)
    return df

@task(log_stdout=True, name="create_embargo_variables", state_handlers=[send_notification])
def create_embargo_variables(df) -> pd.DataFrame:
    df["embargo_duree"] = df.apply(lambda row: fn.days_between(row), axis=1)
    df[['embargo_duree']] = df[['embargo_duree']].fillna(0)
    df['has_exist_embargo'] = df['embargo']
    tmp_condition = df['has_exist_embargo'].isna()
    df.loc[tmp_condition, 'has_exist_embargo'] = 'non'
    df.loc[~tmp_condition, 'has_exist_embargo'] = 'oui'
    return df

@task(log_stdout=True, name="create_discipline_variables", state_handlers=[send_notification])
def create_discipline_variables(df,oai_data) -> pd.DataFrame:
    #split oai_sets_specs in two columns for the multivalues
    split_df = df['oai_set_specs'].str.split('\|\|', expand=True)
    split_df.columns = ['oai_set_specs' + f"_{id_}" for id_ in range(len(split_df.columns))]
    df = pd.merge(df, split_df, how="left", left_index=True, right_index=True)
    df = df.drop(columns=['oai_set_specs_2', 'oai_set_specs_3'])
    #new cols with disc label 
    df['oai_set_specs_0_label'] = df['oai_set_specs_0'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    df['oai_set_specs_1_label'] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    #new cols (dcc-codes and labels) with disc regroup in main dewey classes 0XX, 1XX, 2XX etc...
    df["oai_set_specs_0_regroup"] = df['oai_set_specs_0'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_0_regroup_label'] = df['oai_set_specs_0_regroup'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    df["oai_set_specs_1_regroup"] = df[df['oai_set_specs_1'].notna()]['oai_set_specs_1'].apply(lambda x : x[0:5]+"00")
    df['oai_set_specs_1_regroup_label'] = df[df['oai_set_specs_1_regroup'].notna()]['oai_set_specs_1_regroup'].apply(lambda x: [y['label'] for y in oai_data if y['code'] == str(x)][0])
    #new col (only based on oai_set_specs_0) with disc regroup in main domains 'Sciences, Technologies, Santé' and 'Lettres, sciences Humaines et Sociales'
    df['oai_set_specs_0_main_domain'] = df['oai_set_specs_0'].apply(lambda x: [y['main_domain'] for y in oai_data if y['code'] == str(x)][0])
    return df

#@task(log_stdout=True, name="transform_data", state_handlers=[send_notification])
@task(log_stdout=True, name="transform_data", state_handlers=[send_notification])
def transform_data(df_load_extract,oai) -> pd.DataFrame:
    #keep only relevant columns and rename
    df_tmp = df_load_extract[['accessible', 'auteurs.0.idref', 'auteurs.0.nom', 'auteurs.0.prenom', 'cas', 'code_etab', 'date_soutenance', 'directeurs_these.0.idref', 'directeurs_these.0.nom', 'directeurs_these.0.prenom', 'directeurs_these.1.idref', 'directeurs_these.1.nom', 'directeurs_these.1.prenom', 'discipline.fr', 'ecoles_doctorales.0.nom', 'embargo', 'etablissements_soutenance.0.idref', 'etablissements_soutenance.1.idref', 'etablissements_soutenance.0.nom', 'etablissements_soutenance.1.nom', 'iddoc', 'langue', 'nnt', 'oai_set_specs', 'partenaires_recherche.0.idref', 'partenaires_recherche.0.nom', 'partenaires_recherche.0.type', 'partenaires_recherche.1.idref', 'partenaires_recherche.1.nom', 'partenaires_recherche.1.type', 'partenaires_recherche.2.idref', 'partenaires_recherche.2.nom', 'partenaires_recherche.2.type','source', 'these_sur_travaux', 'titres.fr', 'status']]
    df = df_tmp.set_axis([w.replace('.', '_') for w in df_tmp.columns], axis=1, inplace=False)
    df["is_oa_normalized"] = df["accessible"]
    df['is_oa_normalized'] = df['is_oa_normalized'].replace(['oui','non'],['Accès ouvert','Accès fermé'])
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
    return df

@task(log_stdout=True, name="save_data", state_handlers=[send_notification])
def save_data(df, observation_date, perimetre_save) -> pd.DataFrame:
    #df.to_csv(f"data/03_primary/{observation_date}/theses_{perimetre_save}_processed_v2.csv", index=False, encoding='utf8')
    df.to_csv(f"flows/bso_theses/data/03_primary/{observation_date}/theses_{perimetre_save}_processed.csv", index=False, encoding='utf8')
    return df

with Flow(name=FLOW_NAME) as flow:
    etabs_uca = Parameter('etabs_uca',default = ['NICE','AZUR','COAZ'])
    observation_date = Parameter('observation_date',default = "2022-05-30")
    perimetre_save = Parameter('perimetre_save',default = "uca")
    load_oai_data = read_oai_data()
    # or load_oai_data = scrap_oai_data()
    load_result = load_data()
    # UCA part
    extract_clean_uca_result = clean_column_names(extract_uca_data(load_result,etabs_uca))
    load_uca_oa_variables = create_oa_variables(extract_clean_uca_result)
    load_uca_date_variables = create_date_variables(load_uca_oa_variables)
    load_uca_embargo_variables = create_embargo_variables(load_uca_date_variables)
    load_uca_discipline_variables = create_discipline_variables(load_uca_embargo_variables,load_oai_data)
    load_uca_ending_data = clean_ending_data(load_uca_discipline_variables)
    result_uca = save_data(load_uca_ending_data,observation_date,"uca")
    #transform_result = transform_data(extract_result,scrap_result)
    # all thèses fr part
    extract_clean_fr_result = clean_column_names(extract_fr_data(load_result))
    load_fr_oa_variables = create_oa_variables(extract_clean_fr_result)
    load_fr_date_variables = create_date_variables(load_fr_oa_variables)
    load_fr_embargo_variables = create_embargo_variables(load_fr_date_variables)
    load_fr_discipline_variables = create_discipline_variables(load_fr_embargo_variables,load_oai_data)
    load_fr_ending_data = clean_ending_data(load_fr_discipline_variables)
    result_fr = save_data(load_fr_ending_data,observation_date,"fr")


flow.register(project_name="barometre")
flow.storage=storage
flow.run_config=LocalRun()
flow.run(etabs=['NICE','AZUR','COAZ'],observation_date="2022-05-30",perimetre_save="uca")

#https://docs.prefect.io/core/advanced_tutorials/task-guide.html#state-handlers
