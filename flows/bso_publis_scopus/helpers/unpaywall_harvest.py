# Ensemble de fonctions pour requêter les différentes API Scopus et parser les résultats #
import numpy as np
import pandas as pd
import requests
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

upw_base_url = "https://api.unpaywall.org/v2/"
observation_date = "2021-09-14"

def upw_metadata(doi,email):
    """Get all Unpaywall metadata from a single doi"""     
    if doi is None:
        raise ValueError('DOI cannot be None')
    df_temp = pd.DataFrame()
    params = {'email': email}    
    try:
        requests.get(upw_base_url+str(doi),params=params)
        if requests.get(upw_base_url+str(doi),params=params).status_code == 200:
            response = requests.get(upw_base_url+str(doi),params=params).text
            df_temp = pd.json_normalize(json.loads(response),max_level=6,errors='ignore')
            df_temp["source_doi"] = doi
    except requests.exceptions.RequestException:
        pass
    return df_temp

def upw_retrieval(doi_list,email):
    """Request function upw_metadata from a list of doi,filter result on a few fields and compile in a dataframe"""
    processes = []
    df_collection = []
    fields = ["source_doi","genre","title","published_date","year","publisher","journal_name",
              "journal_issn_l","journal_is_oa","journal_is_in_doaj","is_oa",
              "oa_status","oa_locations"]
    with ThreadPoolExecutor(max_workers=10) as executor:
        processes = {executor.submit(upw_metadata, doi, email): str(doi) for doi in doi_list}
    for task in as_completed(processes):
        worker_result = task.result()
        df_collection.append(worker_result)
    single_df = pd.concat(df_collection)[fields]
    single_df["is_oa_normalized"] = single_df.apply (lambda row: is_oa_normalize(row), axis=1)
    single_df["oa_status_normalized"] = single_df.apply (lambda row: capitalize(row["oa_status"]), axis=1)
    for i in ["host_type","url","license","version"]:
        single_df["oa_locations_"+i] = single_df.apply (lambda row:','.join(map(str,[loc[i] for loc in row["oa_locations"]])), axis=1)
    single_df["oa_host_type_normalized"] = single_df.apply (lambda row: oa_host_type_normalize(row), axis=1)
    single_df["oa_host_domain"] = single_df.apply (lambda row: oa_url2domain(row), axis=1)
    single_df["oa_hostdomain_count"] = single_df.apply (lambda row: oa_hostdomain_count(row), axis=1)
    single_df["oa_repo_normalized"] = single_df.apply (lambda row: oa_repo_normalize(row), axis=1)
    #to resume (optional)
    #size_source = len(doi_list)
    #size_result = len(single_df["source_doi"].to_list())
    #print("Pourcentage de doi reconnus par Unpaywall : "+ "{:.2f}".format(((size_source - size_result) / size_doi) * 100) + "%")
    print("DOI non reconnus : " + ",".join(set(single_df["source_doi"].to_list()) - set(doi_list)))
    # save unrecognized doi 
    #pd.DataFrame(set(single_df["source_doi"].to_list()) - set(doi_list)).to_csv("data/03_primary/{0}/publis_non_traitees/unpaywall_unrecognized_doi.csv".format(observation_date),index = False,encoding='utf8')
    return single_df.rename(columns={"year": "year_upw"}).drop(columns=['is_oa','oa_locations_url'])
        
"""Utils functions to parse and transform the data from unpaywall API in a custom way"""   
def is_oa_normalize(row):
    if row['is_oa'] is False :
        return 'Accès fermé'
    if row['is_oa'] is True :
        return 'Accès ouvert'
    
def capitalize(row):
    return row.capitalize() 
    
def oa_host_type_normalize(row):
    if ("publisher" in row['oa_locations_host_type']) and ("repository" in row['oa_locations_host_type'])  :
        return 'Editeur et archive ouverte'
    if ("publisher" in row['oa_locations_host_type']) and ("repository" not in row['oa_locations_host_type'])  :
        return 'Editeur'
    if ("publisher" not in row['oa_locations_host_type']) and ("repository" in row['oa_locations_host_type'])  :
        return 'Archive ouverte'
    if row['is_oa_normalized'] == "Accès fermé" :
        return 'Accès fermé'
    
def oa_url2domain(row):
    list = row['oa_locations_url'].split(",")
    l = []
    for k in list:
        o = urlparse(k).netloc
        l.append(o)
    return ",".join(l)

def oa_hostdomain_count (row):
    list = row['oa_host_domain'].split(",")
    return {"hal":sum('hal' in s for s in list),"arXiv":sum('arxiv' in s for s in list),"Autres":sum(('hal' not in s) and ('arxiv' not in s) for s in list)}

def oa_repo_normalize (row):
    r = row['oa_hostdomain_count']
    s = ""
    list = []
    if (r["hal"] != 0):
        list.append("Hal")
    if (r["arXiv"] != 0):
        list.append("arXiv")
    if (r["Autres"] != 0):
        list.append("Autres")
    return ",".join(list)

def doi_prefix(row):
    return row["doi"].str.partition("/")[0]