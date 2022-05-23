# Ensemble de fonctions pour requêter les différentes API Scopus et parser les résultats #
import numpy as np
import pandas as pd
import requests
import json
import time
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
#[Unused] crossref driver package crossref-commons
#import crossref_commons.retrieval

crf_base_url = "https://api.crossref.org/v1/works/"
crfprefix_base_url = "https://api.crossref.org/v1/prefixes/"

"""Publications Metadata"""

def crf_metadata(doi,email):
    """Get all Crossref metadata from a single doi"""  
    if doi is None:
        raise ValueError('DOI cannot be None')
    params = {'mailto': email}
    result = {}
    try:
        requests.get(crf_base_url+str(doi),params=params)  
        if requests.get(crf_base_url+str(doi),params=params).status_code == 200:
            response = requests.get(crf_base_url+str(doi),params=params).text
            d = json.loads(response).get("message")
            result["source_doi"] = doi
            try:
                d["published-online"]
                result["published-online-date"] = d["published-online"]["date-parts"][0][0]
            except:
                pass
            try:
                d["published-print"]
                result["published-print-date"] = d["published-print"]["date-parts"][0][0]
            except:
                pass
            try:
                d["journal-issue"]
                result["journal-published-print-date"] = d["journal-issue"]["published-print"]["date-parts"][0][0]
            except:
                pass
            try:
                d["is-referenced-by-count"]
                result["is-referenced-by-count"] = d["is-referenced-by-count"]
            except:
                pass
            try:
                d["funder"]
                result["funder"] = ','.join(map(str,[l["name"] for l in d["funder"]]))               
            except:
                pass
    except ValueError:
        pass
    time.sleep(1)
    return result

def crf_retrieval(doi_list,email):
    """Request function crf_metadata from a list of doi,filter result on a few fields and compile in a dataframe"""
    df_result = pd.DataFrame(crf_metadata(i,email) for i in doi_list)
    return df_result

"""Normalized publisher's names"""

def crf_publisher_metadata(prefix):
    """Get the homogeneous publisher's name from a prefix doi"""
    if prefix is None:
        raise ValueError('prefix cannot be None')
    result = {}
    result["prefix"] = prefix
    try:
        requests.get(crfprefix_base_url+str(prefix))
        if requests.get(crfprefix_base_url+str(prefix)).status_code == 200:
            response = requests.get(crfprefix_base_url+str(prefix)).text
            result["publisher_by_doiprefix"] = json.loads(response).get("message")["name"]
        else:
            pass
    except:
        pass
    return result

def crf_publisher_retrieval(doiprefix_list):
    """Request function crf_publisher_metadata from a list of doi prefixs and compile in a dataframe"""
    df_result = pd.DataFrame(crf_publisher_metadata(i) for i in doiprefix_list)
    return df_result[df_result["prefix"].notna()]

"""[Unused] Get all Crossref metadata from a single doi
def crf_metadata2(doi):
    if doi is None:
        raise ValueError('DOI cannot be None')
    df_temp = pd.DataFrame()
    result_error = []
    try:
        crossref_commons.retrieval.get_publication_as_json(str(doi))
        df_temp = pd.json_normalize(crossref_commons.retrieval.get_publication_as_json(doi),max_level=3,errors='ignore')
        df_temp["source_doi"] = doi
    except ValueError:
        result_error.append(doi)
        pass
    return df_temp,result_error

[Unused] Request function crf_metadata from a list of doi,filter result on a few fields and compile in a dataframe
def crf_retrieval2(doi_list):
    processes = []
    df_collection = []
    fields = ["source_doi","is-referenced-by-count","subject","funder"]
    with ThreadPoolExecutor(max_workers=10) as executor:
        processes = {executor.submit(crf_metadata, doi): doi for doi in doi_list}
    for task in as_completed(processes):
        worker_result,worker_error = task.result()
        df_collection.append(worker_result)
    single_df = pd.concat(df_collection)
    for x in ["subject","funder"]:
        if x in single_df.columns:
            single_df[x] = [try_join(l) for l in single_df[x]]
    print("Liste des doi non reconnus par Crossref : " + ",".join(worker_error))
    print("Pourcentage de doi non reconnus par Crossref : "+ "{:.2f}".format(len(worker_error)/len(doi_list) * 100) + "%")
    return single_df[single_df.columns & fields]
    """
