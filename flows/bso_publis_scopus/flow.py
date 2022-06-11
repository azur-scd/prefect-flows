#!/usr/bin/env python
# -*- coding: utf-8 -*-
import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import Local, GitHub
from prefect.executors import LocalDaskExecutor
from prefect.engine.results import LocalResult
from prefect.engine import signals
import pandas as pd
import numpy as np
import requests
import json
import glob
import matplotlib.pyplot as plt
import helpers.unpaywall_harvest as upw
import helpers.crossref_harvest as crf
import helpers.dissemin_harvest as dsm
import helpers.bso_classification_harvest as bso

FLOW_NAME = "flow-bso-publis-scopus"
storage = Local()
#result = LocalResult(dir="flows/bso_publis_scopus/data/")

def keep_duplicate (row):
    return row["author_name"] +"|" + row["affiliation-name"]

@task(log_stdout=True, name="extract_reference_data")
def extract_reference_data(observation_date) -> pd.DataFrame:
    # fetch reference data
    df = pd.read_json('data/01_raw/{0}/exportDonnees_barometre_complet_{0}.json'.format(observation_date))
    print("Initial export shape : {0}".format(df.shape))
    # keep columns
    df = df[["dc:identifiers","prism:doi","reference","annee_pub","@afids","mentionAffil_reconstruct","@auid","ce:indexed-name","corresponding_author","Is_dc:creator"]]
    # rename columns
    df.columns = ['source_id', 'doi',"scopus_title",'year', 'aff_scopus_id','aff_source_text','author_id','author_name','corresponding_author','creator_author']
    # concatene authors
    df_authors = df.groupby('source_id')['author_name'].apply(list).reset_index(name='all_authors')
    df_authors['all_authors'] = df_authors["all_authors"].apply('|'.join)
    reference_data = pd.merge(df,df_authors, left_on='source_id', right_on='source_id')
    return reference_data

@task(log_stdout=True, name="update_referentiel_data")
def update_referentiel_data(reference_data) -> pd.DataFrame:
    # fetch live data
    affiliations = pd.read_json('data/03_primary/referentiel_structures.json')
    # rename and save the file in his last state
    affiliations.to_json('data/03_primary/referentiel_structures_old.json',orient="records",indent=3,force_ascii=False)
    # compare with current data df to see if new affiliations must be added ton the referential
    set_affiliations_id = set(affiliations['affiliation_id'].tolist())
    set_data_affiliations_id = set(reference_data['aff_scopus_id'].unique().tolist())
    diff = set_affiliations_id.union(set_data_affiliations_id)  - set_affiliations_id.intersection(set_data_affiliations_id) 
    if len(diff) == 1:
        # update records sums columns
        #affiliations =affiliations.drop(['document-count-period'])
        df_count = reference_data.groupby("aff_internal_id")['doi'].nunique().reset_index().rename(columns={'doi':'counts'}).convert_dtypes()
        affiliations = pd.merge(affiliations,df_count, left_on='id', right_on='aff_internal_id',how="left").drop(columns=['aff_internal_id','documents_count']).rename(columns={'counts':'documents_count'})
        affiliations['documents_count'] = affiliations['documents_count'].fillna(0)
        affiliations.to_json('data/03_primary/referentiel_structures.json',orient="records",indent=3,force_ascii=False)
        return affiliations
    else:
        print("stop flow")
        raise signals.FAIL()

@task(log_stdout=True, name="get_publis_with_affiliations_data")
def get_publis_with_affiliations_data(reference_data,affiliations,observation_date) -> pd.DataFrame:
    # merge all publis with affiliations
    affiliations["affiliation_id"] = affiliations["affiliation_id"].astype('str')
    reference_data["aff_scopus_id"] = reference_data["aff_scopus_id"].astype('str')
    publis_all_with_affiliations_data = pd.merge(reference_data,affiliations[affiliations["affiliation_id"].notna()], left_on='aff_scopus_id', right_on='affiliation_id',how="left").drop(columns=['affiliation_id','documents_count','ppn_valide','affcourt_valide','RNSR','VIAF','ISNI','BNF','HAL'])
    publis_all_with_affiliations_data = publis_all_with_affiliations_data.rename(columns={'id': 'aff_internal_id', 'parent_id': 'aff_parent_id'})
    # identify corresponding author if UCA
    publis_all_with_affiliations_data["corresponding"] = publis_all_with_affiliations_data[publis_all_with_affiliations_data["corresponding_author"] == "oui"].apply (lambda row: keep_duplicate(row), axis=1)
    publis_all_with_affiliations_data.to_csv("data/03_primary/{0}/publis_all_with_affiliations_data.csv".format(observation_date),index = False,encoding='utf8')
    return publis_all_with_affiliations_data

@task(log_stdout=True, name="control_intermediate_data")
def control_intermediate_data(publis_all_with_affiliations_data,observation_date):
    # % missing DOI
    df_unique = publis_all_with_affiliations_data.drop_duplicates(subset=['source_id'], keep='last')
    # save missing doi publs
    df_unique["doi"].isna().to_csv("data/03_primary/{0}/publis_non_traitees/without_doi.csv".format(observation_date),index = False,encoding='utf8')
    # charts
    plt.pie(np.array([df_unique["doi"].isna().sum(), df_unique["doi"].notna().sum()]), autopct='%1.1f%%')
    plt.savefig("data/08_reporting/missing_doi.png")
    # Values corresponding author
    publis_all_with_affiliations_data["corresponding_author"].value_counts().plot(kind='barh').get_figure().savefig("data/08_reporting/corresponding_control.png")


@task(log_stdout=True, name="get_publis_uniques_doi_data")
def get_publis_uniques_doi_data(publis_all_with_affiliations_data,corpus_end_year) -> pd.DataFrame:
    # Deduplicate
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].astype('category')
    publis_all_with_affiliations_data["corresponding_author"] = publis_all_with_affiliations_data["corresponding_author"].cat.set_categories(['oui', 'non', 'corr absent pour cette publi'], ordered=True)
    publis_all_with_affiliations_data.sort_values(by=['doi', 'corresponding_author'])
    publis_uniques_doi_data = publis_all_with_affiliations_data[publis_all_with_affiliations_data.doi.notna()].drop_duplicates(subset=['doi'], keep='first')[["source_id","doi","year","corresponding","all_authors"]]
    publis_uniques_doi_data = publis_uniques_doi_data[publis_uniques_doi_data.year < corpus_end_year]
    print(publis_uniques_doi_data.shape)
    publis_uniques_doi_data.to_csv("data/02_intermediate/publis_uniques_doi_data.csv",index = False,encoding='utf8')
    return publis_uniques_doi_data

@task(log_stdout=True, name="update_publiher_doiprefix_data")
def update_publiher_doiprefix_data(publis_uniques_doi_data) -> pd.DataFrame:
    new_prefix_list = list(set([item.partition("/")[0] for item in publis_uniques_doi_data["doi"].to_list()]))
    old_prefix_df = pd.read_csv("data/03_primary/mapping_doiprefixes_publisher.csv", sep=",",encoding='utf8')
    old_prefix_list = old_prefix_df["prefix"].astype(str).to_list()
    diff_prefix_list = list(set(new_prefix_list) - set(old_prefix_list))
    print("Nombre de préfixes à ajouter : " + str(len(diff_prefix_list)))
    df_new_prefix_result = crf.crf_publisher_retrieval(diff_prefix_list)
    publishers_doi_prefix = old_prefix_df.append(df_new_prefix_result)
    publishers_doi_prefix.drop_duplicates(subset=['prefix'], keep='last').to_csv("data/03_primary/mapping_doiprefixes_publisher.csv", index = False,encoding='utf8')
    return publishers_doi_prefix

@task(log_stdout=True, name="get_unpaywall_data")
def get_unpaywall_data(publis_uniques_doi_data) -> pd.DataFrame:
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        print("DOI traités par unpaywall : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        upw.upw_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv("data/02_intermediate/temp_upw/upw_"+str(i)+".csv",index = False,encoding='utf8')
    #concaténation des cvs
    extension = 'csv'
    all_filenames = [i for i in glob.glob('data/02_intermediate/temp_upw/*.{}'.format(extension))]
    unpaywall_data = pd.concat([pd.read_csv(f).drop(columns=['oa_locations']) for f in all_filenames ]).drop_duplicates(subset=['source_doi'], keep='last')
    unpaywall_data.to_csv("data/02_intermediate/unpaywall_data.csv",index = False,encoding='utf8')
    return unpaywall_data

@task(log_stdout=True, name="get_crossref_data")
def get_crossref_data(publis_uniques_doi_data) -> pd.DataFrame:
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 100
    for i in range(0, len(l), n):
        print("DOI traités par crossref : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        crf.crf_retrieval(l[i:i+n],"geraldine.geoffroy@univ-cotedazur.fr").to_csv("data/02_intermediate/temp_crf/crf_"+str(i)+".csv",index = False,encoding='utf8')
    # concaténation des csv
    extension = 'csv'
    all_filenames = [i for i in glob.glob('data/02_intermediate/temp_crf/*.{}'.format(extension))]
    combined_crf = pd.concat([pd.read_csv(f) for f in all_filenames ])
    crossref_data = combined_crf[combined_crf.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    crossref_data.to_csv("data/02_intermediate/crossref_data.csv",index = False,encoding='utf8')
    return crossref_data

@task(log_stdout=True, name="get_dissemin_data")
def get_dissemin_data(publis_uniques_doi_data) -> pd.DataFrame:
    l = publis_uniques_doi_data.drop_duplicates(subset=['doi'], keep='last')["doi"].to_list()
    n = 10
    for i in range(0, len(l), n):
        print("DOI traités par dissemin : de "+ str(i) + " à " + str(i+n))
        #sauvegarde intermédiaire en csv
        dsm.dsm_retrieval(l[i:i+n]).to_csv("data/02_intermediate/temp_dsm/dsm_"+str(i)+".csv",index = False,encoding='utf8')
    #concaténation des cvs
    extension = 'csv'
    all_filenames = [i for i in glob.glob('data/02_intermediate/temp_dsm/*.{}'.format(extension))]
    combined_dsm = pd.concat([pd.read_csv(f) for f in all_filenames ])
    dissemin_data = combined_dsm[combined_dsm.source_doi.notna()].drop_duplicates(subset=['source_doi'], keep='last')
    dissemin_data.to_csv("data/02_intermediate/dissemin_data.csv",index = False,encoding='utf8')
    return dissemin_data

@task(log_stdout=True, name="merge_all_data")
def merge_all_data(publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data,crossref_data,dissemin_data,observation_date) -> pd.DataFrame:
    # unpaywall data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_data,unpaywall_data, left_on='doi', right_on='source_doi',how="right").drop(columns=['source_doi','year_upw'])
    # publishers doi prefix
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data.apply (lambda row: str(row["doi"].partition("/")[0]), axis=1) 
    publis_uniques_doi_oa_data["doi_prefix"] = publis_uniques_doi_oa_data["doi_prefix"].astype(str)
    publishers_doi_prefix["prefix"] = publishers_doi_prefix["prefix"].astype(str)
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(publishers_doi_prefix, left_on='doi_prefix', right_on='prefix',how='left').drop(columns=['prefix'])
    # crossref data
    publis_uniques_doi_oa_data = publis_uniques_doi_oa_data.merge(crossref_data, left_on='doi', right_on='source_doi',how='left').drop(columns=['source_doi','published-online-date','journal-published-print-date','published-print-date'])
    # dissemin data
    publis_uniques_doi_oa_data = pd.merge(publis_uniques_doi_oa_data,dissemin_data, left_on='doi', right_on='source_doi',how="left").drop(columns=['source_doi'])
    publis_uniques_doi_oa_data.to_csv("data/03_primary/{0}/publis_uniques_doi_oa_data.csv".format(observation_date), index= False,encoding='utf8')
    return publis_uniques_doi_oa_data

@task(log_stdout=True, name="recup_data_for_flow_failed",nout=5)
def recup_data_for_flow_failed(observation_date) -> pd.DataFrame:
    publis_uniques_doi_oa_data = pd.read_csv("data/03_primary/{0}/publis_uniques_doi_oa_data.csv".format(observation_date), sep=",",encoding='utf8')
    return publis_uniques_doi_oa_data

@task(log_stdout=True, name="get_bso_classification_data")
def get_bso_classification_data(publis_uniques_doi_oa_data,observation_date) -> pd.DataFrame:
    # get primary class in bso mesri export
    df_bso = pd.read_csv("data/05_model_input/bso_export_dataset_mesri_20220518.csv", sep=",",encoding='utf8').drop_duplicates(subset=['doi'], keep='first')
    temp = publis_uniques_doi_oa_data[publis_uniques_doi_oa_data.title.notna() & publis_uniques_doi_oa_data.journal_name.notna()]
    temp["bso_classification"] = np.nan
    col = 'doi'
    cols_to_replace = ['bso_classification']
    temp.loc[temp[col].isin(df_bso[col]), cols_to_replace] = df_bso.loc[df_bso[col].isin(temp[col]),cols_to_replace].values
    #temp.to_csv("data/07_model_output/temp.csv", index= False,encoding='utf8')
    # prapare dataset for ML model to be applied on local doi not presents in bso
    df_ml_prepared = temp[temp.bso_classification.isna()][["doi","title","journal_name"]]
    df_ml_prepared["title_s"] = df_ml_prepared["title"].str.replace('\d+', '')
    df_ml_prepared = bso.text_process(df_ml_prepared,"title")
    df_ml_prepared["journal_name_s"] = df_ml_prepared["journal_name"].str.replace('\d+', '')
    df_ml_prepared = bso.text_process(df_ml_prepared,"journal_name")
    df_ml_ready = df_ml_prepared[["doi","title_cleaned","journal_name_cleaned"]]
    df_ml_ready["features_union_titre_journal"] = df_ml_ready["title_cleaned"] + ' '  +  df_ml_ready["journal_name_cleaned"]
    df_ml_ready["bso_classification"] = df_ml_ready.apply(lambda row: bso.to_bso_class_with_ml(row),axis=1)
    df_ml_ready.to_csv("data/07_model_output/doi_ml_result.csv", index=False,encoding="utf-8")
    temp.loc[temp[col].isin(df_ml_ready[col]), cols_to_replace] = df_ml_ready.loc[df_ml_ready[col].isin(temp[col]),cols_to_replace].values
    #merge with main oa file
    publis_uniques_doi_oa_data["bso_classification"] = np.nan
    publis_uniques_doi_oa_data.loc[publis_uniques_doi_oa_data[col].isin(temp[col]), cols_to_replace] = temp.loc[temp[col].isin(publis_uniques_doi_oa_data[col]),cols_to_replace].values
    publis_uniques_doi_oa_data[cols_to_replace] = publis_uniques_doi_oa_data[cols_to_replace].fillna("unknown").replace(r'\r\n', '', regex=True)
    # bricolage à finaliser
    df_bso_classification = pd.read_json("../data/03_primary/bso_classification.json")
    df=pd.merge(publis_uniques_doi_oa_data,df_bso_classification, left_on='bso_classification', right_on='name_en',how="left").drop(columns=['name_en']).rename(columns={"name_fr": "bso_classification_fr"})
    # erase the previous file
    df.to_csv("data/07_model_output/{0}/publis_uniques_doi_oa_data_with_bsoclasses.csv".format(observation_date), index= False,encoding='utf8')
    return df

with Flow(name=FLOW_NAME) as flow:
    observation_date = Parameter('observation_date',default = "2022-05-04")
    corpus_end_year = Parameter('corpus_end_year',default = 2022)
    reference_data = extract_reference_data(observation_date)
    affiliations = update_referentiel_data(reference_data)
    publis_all_with_affiliations_data = get_publis_with_affiliations_data(reference_data,affiliations,observation_date)
    publis_uniques_doi_data = get_publis_uniques_doi_data(publis_all_with_affiliations_data,corpus_end_year)
    publishers_doi_prefix = update_publiher_doiprefix_data(publis_uniques_doi_data)
    unpaywall_data = get_unpaywall_data(publis_uniques_doi_data)
    crossref_data = get_crossref_data(publis_uniques_doi_data)
    dissemin_data = get_dissemin_data(publis_uniques_doi_data)
    publis_uniques_doi_oa_data = merge_all_data(publis_uniques_doi_data,publishers_doi_prefix,unpaywall_data,crossref_data,dissemin_data,observation_date)
    result = get_bso_classification_data(publis_uniques_doi_oa_data,observation_date)
    control_intermediate_data = control_intermediate_data(publis_all_with_affiliations_data,observation_date)

"""with Flow(name=FLOW_NAME) as flow:
    observation_date = Parameter('observation_date',default = "2022-05-04")
    publis_uniques_doi_oa_data = recup_data_for_flow_failed(observation_date)
    result = get_bso_classification_data(publis_uniques_doi_oa_data,observation_date)"""


flow.register(project_name="barometre")
flow.storage=storage
flow.run_config=LocalRun()
#flow.run(observation_date="2022-05-04", corpus_end_year=2022)
flow.run(observation_date="2022-05-04")
#flow.visualize()