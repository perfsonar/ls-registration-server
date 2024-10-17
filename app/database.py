from ipaddress import ip_address, IPv6Address, IPv4Address
from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import json
from fastapi import HTTPException
import os

def map_dbspec(record):

    ip_version = set()

    for ip in record['addresses']:
        if type(ip_address(ip)) is IPv6Address:
            ip_version.add(6)
        elif type(ip_address(ip)) is IPv4Address:
            ip_version.add(4)
        
    record['ip_versions'] = list(ip_version)
    record['@timestamp'] = datetime.now(timezone.utc)
    record['expires'] = record['@timestamp'] + timedelta(hours=24)

    return record

def post_to_elastic(record):
    # Create the client instance
    esclient = Elasticsearch(
        os.environ['ELASTIC_HOST'],
        ca_certs=os.environ.get('ELASTIC_CA_CERT'),
        basic_auth=(os.environ['ELASTIC_USER'], os.environ['ELASTIC_PASS'])
    )
    record_id = record['host']['client_uuid']
    for ip in record['addresses']:
        record_id += '-' + str(ip)
    try:
        if not esclient.indices.exists(index=os.environ['ELASTIC_INDEX']):
            with open('app/mapping/es_mapping.json') as file:
                es_mapping = json.load(file)
            
            with open('app/mapping/es_settings.json') as file:
                es_settings = json.load(file)
            
            settings = {
                "settings": es_settings,
                    "mappings": es_mapping
                }
            
            esclient.indices.create(index=os.environ['ELASTIC_INDEX'], ignore=400, body=settings)
        resp = esclient.index(index=os.environ['ELASTIC_INDEX'], document=record, id=record_id)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error handling elasticsearch connection - " + str(e))

    
    return resp