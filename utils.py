# -*- coding: utf-8 -*-
"""
Created on Wed May 22 20:30:41 2019

@author: benja
"""
import requests
import shutil
from zipfile import ZipFile

def generate_query_expression(data):
    # This function generates a JSON query expression that is used in a POST
    # request to the GBIF Occurrence API. The variable 'data' is a list of 
    # GBIF species keys.
    expression = {
      'creator': 'bfeinsilver',
      'format': 'SIMPLE_CSV',
      'predicate': {
        'type': 'and',
        'predicates': [
          {
            'type': 'in',
            'key': 'SPECIES_KEY',
            'values': data
          },
          {
            'type': 'equals',
            'key': 'HAS_GEOSPATIAL_ISSUE',
            'value': 'false'
          },
          {
            'type': 'equals',
            'key': 'HAS_COORDINATE',
            'value': 'true'
          },
          {
            'type':'not',
            'predicate': {
                        'type':'equals',
                        'key':'BASIS_OF_RECORD',
                        'value':'FOSSIL_SPECIMEN'
                        }
          }                  
        ]
      }
    }            
    return expression

def prep_esummary_req(query_key, webenv, retstart, retmax, db, api_key):
    # This function generates a query string to be sent along with a GET
    # request to the Entrez ESummary utility.    
    payload = {'query_key':query_key,
               'webenv':webenv,
               'version':'2.0',
               'retmode':'json',
               'retmax':retmax,
               'retstart':retstart,
               'db':db,
               'api_key':api_key
               }
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
    req = requests.Request('GET', url, params=payload)
    prepped = req.prepare()
    prepped.headers['Accept-Encoding'] = 'identity' # Chunked encoding error fix.
    return prepped

def validate_and_filter(coord_uncertainty, x, y, limit, bounds):
    # This function validates and filters an occurrence record based on its
    # coordinate uncertainty and whether or not it falls within the bounds of
	# the raster data.
    if not coord_uncertainty:
        coord_uncertainty = 0
    try:
        coord_uncertainty = float(coord_uncertainty)
        coords = {'x':float(x), 'y':float(y)}
    except ValueError:
        pass
    else:
        if (coord_uncertainty <= limit
            and bounds['xmin'] < coords['x'] < bounds['xmax'] 
            and bounds['ymin'] < coords['y'] < bounds['ymax']):
            return coords
                
def get_download_link(session, url, timeout):
    # This function checks the status of a GBIF occurrence download request.
    r = session.get(url, stream=False, timeout=timeout)
    if r.status_code == requests.codes.ok:
        result = r.json()
        status = result['status']
        download_link = result['downloadLink']
        if status in ['PREPARING', 'RUNNING', 'SUSPENDED']:
            pass
        elif status == 'SUCCEEDED':
            return download_link
        else:
            raise Exception('Download Request Failed: ' + status)
    
def copy_stream(stream, target_archive):
    # This function copies the contents of a zipped GBIF occurrence download to
    # a consolidated archive.
    with ZipFile(stream) as source_archive:
        zip_info = source_archive.infolist()[0]
        with source_archive.open(zip_info) as source_file:
            with target_archive.open(zip_info, 'w') as target_file:
                shutil.copyfileobj(source_file, target_file)