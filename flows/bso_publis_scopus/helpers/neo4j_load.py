import pandas as pd
import requests
import json
from dotenv import dotenv_values
from neomodel import (config, db, StructuredNode, StructuredRel,StringProperty, IntegerProperty, UniqueIdProperty, DateProperty, DateTimeProperty, RelationshipTo, RelationshipFrom, Q, Traversal, OUTGOING, INCOMING, EITHER)
from models import *

def create_complete_subgraph(row,obs_date):
    Publication(scopus_id=row['source_id'],doi=row['doi'],title=json.dumps(row['title']),genre=row["genre"]).save()
    db.cypher_query("MERGE (p:PublicationDate {value:'"+str(row['year'])+"'}) return p")
    db.cypher_query("MERGE (p:Publisher {doi_prefix:'"+str(row['doi_prefix'])+"',name:"+str(json.dumps(row['publisher_by_doiprefix']))+"}) return p")
    db.cypher_query("MERGE (p:PublicationOA {is_oa_value:'"+str(row['is_oa_normalized'])+"',host_type:'"+str(row['oa_host_type_normalized'])+"',oa_status:'"+str(row['oa_status_normalized'])+"',obs_date:'"+str(obs_date)+"'}) return p")
    Publication.nodes.get(scopus_id = row['source_id']).publis_has_date.connect(PublicationDate.nodes.get(value = str(row['year'])))
    Publication.nodes.get(scopus_id = row['source_id']).publis_has_publisher.connect(Publisher.nodes.get(doi_prefix = row['doi_prefix']))
    Publication.nodes.get(scopus_id = row['source_id']).publis_is_oa.connect(PublicationOA.nodes.filter(Q(is_oa_value__exact=row['is_oa_normalized']) & Q(host_type__exact=row['oa_host_type_normalized']) & Q(oa_status__exact=row['oa_status_normalized']) & Q(obs_date__exact=str(obs_date)))[0])