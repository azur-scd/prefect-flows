# Ensemble de fonctions pour requêter les différentes API Scopus et parser les résultats #
import numpy as np
import pandas as pd
import requests
import json
import time
from urllib.parse import urlparse

dsm_base_url = "https://dissem.in/api/"
 
def dsm_metadata(doi):
    """Get some choosed Dissemin metadata from a single doi"""  
    if doi is None:
        raise ValueError('DOI cannot be None')
    result = {}
    try:
        requests.get(dsm_base_url+str(doi))  
        if requests.get(dsm_base_url+str(doi)).status_code == 200:
            response = requests.get(dsm_base_url+str(doi)).text
            d = json.loads(response)
            result["source_doi"] = doi
            result["dsm_oa_classification"] = d.get("paper")["classification"]
            records = d.get("paper")["records"]
            result["dsm_splash_url"] = ','.join(list(set([urlparse(i["splash_url"]).netloc for i in d.get("paper")["records"]])))
            result["dsm_policy_preprint"] = next((loc["policy"]["preprint"] for loc in records if "policy" in loc), None)
            result["dsm_policy_postprint"] = next((loc["policy"]["postprint"] for loc in records if "policy" in loc), None)
            result["dsm_policy_published"] = next((loc["policy"]["published"] for loc in records if "policy" in loc), None)
            result["dsm_romeo_id"] = next((loc["policy"]["romeo_id"] for loc in records if "policy" in loc), None)
    except:
        pass
    time.sleep(1)
    return result

def dsm_retrieval(doi_list):
    """Request function dsm_metadata from a list of doi and compile result in a dataframe"""
    df_result = pd.DataFrame(dsm_metadata(doi) for doi in doi_list[0:10])
    return df_result

