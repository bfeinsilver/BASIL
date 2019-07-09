# -*- coding: utf-8 -*-
"""
Created on Sat May  4 21:48:16 2019

@author: benja
"""

import requests
import luigi
import json
import xml.etree.ElementTree as ET
import utils
import time
import math
import csv
import io
import os
import zipfile
import rasterio
import pickle
import pandas as pd
import numpy as np
from zipfile import ZipFile
from contextlib import ExitStack
from urllib3.util.retry import Retry
from luigi.parameter import ParameterVisibility

class EntrezTask(luigi.Task):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
    retries = Retry(backoff_factor=0.1)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    timeout = 30
    retmax = 50 # Limit of 500 for JSON response.
    db = luigi.Parameter(default='protein')
    api_key = luigi.Parameter(default='beac95a908b21daf251667ee6eb138a05608',
        visibility=ParameterVisibility.PRIVATE)

    
class GBIFTask(luigi.Task):
    url = 'http://api.gbif.org/v1/'
    retries = Retry(backoff_factor=0.1)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    timeout = 30
    user = luigi.Parameter(default='bfeinsilver')
    pwd = luigi.Parameter(default='f5Rga5RPGgg4GNEa',
        visibility=ParameterVisibility.PRIVATE)


class SearchDB(EntrezTask):
    
    # This task searches an NCBI database and, using the Entrez History Server
	# and ESearch utility, returns a web environment and query key.
    
    search_term = luigi.Parameter(default='plants[Filter] AND nbs lrr[Title]')
    
    def output(self):
        return luigi.LocalTarget('data/{}-esearch-params.json'.format(self.db))

    def run(self):
        with ExitStack() as stack:
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            payload = {'db':self.db,
                       'retmode':'json',
                       'usehistory':'y',
                       'term':self.search_term
                       }
            s.mount(self.url, self.adapter)
            r = s.get(self.url + 'esearch.fcgi', params=payload,
                timeout=self.timeout)
            if r.status_code == requests.codes.ok:
                outfile.write(r.text)
        

class GetDocSummaries(EntrezTask):
    
    # This task returns a list of UIDs and Taxonomy IDs corresponding to the
    # previous web environment and query key using the ESummary utility.
    
    def requires(self):
        return SearchDB()
        
    def output(self):
        return luigi.LocalTarget('data/{}-docsummaries.txt'.format(self.db))
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)            
            result = json.load(infile)['esearchresult']
            query_key = result['querykey']
            webenv = result['webenv']
            count = int(result['count'])        
            for retstart in range(0, count, self.retmax):
                time.sleep(0.1) # 10 requests per second limit.
                message = 'Progress: {0:.0%}'.format(retstart / count)
                self.set_status_message(message)
                print(message)
                args = [query_key, webenv, retstart,
                        self.retmax, self.db, self.api_key]
                prepped = utils.prep_esummary_req(*args)
                r = s.send(prepped, timeout=self.timeout, stream=False)
                if r.status_code == requests.codes.ok:
                    for key, value in r.json()['result'].items():
                        # Skips list of UIDs included in result.
                        if key != 'uids':
                            data = {'uid':value['uid'],
                                    'taxid':value['taxid']
                                    }
                            outfile.write('{uid},{taxid}\n'.format(**data))
                                

class RemoveDuplicateTaxIDs(luigi.Task):
    
    # This task returns a list of unique Taxonomy IDs.
    
    def requires(self):
        return GetDocSummaries()
        
    def output(self):
        return luigi.LocalTarget('data/unique-taxids.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))        
            lines = [line.split(',') for line in infile.read().splitlines()]
            taxids = set([taxid for uid,taxid in lines])
            for taxid in taxids:
                outfile.write(taxid + '\n')
                
                                
class PostTaxIDs(EntrezTask):
    
    # This task posts the previous list of unique Taxonomy IDs to the NCBI
    # Taxonomy database and, using the Entrez History Server and EPost
    # utility, returns a web environment and query key.

    def requires(self):
        return RemoveDuplicateTaxIDs()
        
    def output(self):
        # Entrez EPost does not support JSON formatted output.
        return luigi.LocalTarget('data/taxonomy-epost-params.xml')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            taxids = ','.join(infile.read().splitlines())
            payload = {'db':'taxonomy', 'id':taxids}
            s.mount(self.url, self.adapter)        
            r = s.post(self.url + 'epost.fcgi', data=payload,
                timeout=self.timeout)
            if r.status_code == requests.codes.ok:
                outfile.write(r.text)
                    
            
class GetTaxonomySummaries(EntrezTask):
    
    # This task returns a list of Taxonomy IDs and scientific names
    # corresponding to the previous web environment and query key using the
    # ESummary utility. 
    
    def requires(self):
        return [PostTaxIDs(), RemoveDuplicateTaxIDs()]
        
    def output(self):
        return luigi.LocalTarget('data/taxonomy-docsummaries.txt')
        
    def run(self):
        with ExitStack() as stack:
            infiles = [stack.enter_context(f.open('r')) for f in self.input()]
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)
            # Using ElementTree to parse XML response content.
			tree = ET.parse(infiles[0])
            root = tree.getroot()
            query_key = root[0].text
            webenv = root[1].text
            count = len(infiles[1].read().splitlines())
            for retstart in range(0, count, self.retmax):
                time.sleep(0.1) # 10 requests per second limit.
                message = 'Progress: {0:.0%}'.format(retstart / count)
                self.set_status_message(message)
                print(message)
                args = [query_key, webenv, retstart,
                    self.retmax, 'taxonomy', self.api_key]
                prepped = utils.prep_esummary_req(*args)
                r = s.send(prepped, timeout=self.timeout, stream=False)
                if r.status_code == requests.codes.ok:
                    for key, value in r.json()['result'].items():
                        # Skips list of UIDs included in result.
                        if key != 'uids':
                            data = {'taxid':value['taxid'],
                                    'sname':value['scientificname']
                                    }                        
                            outfile.write('{taxid},{sname}\n'.format(**data))
                                    

class GBIFSpeciesMatch(GBIFTask):

    # This task uses the GBIF Species API to map the previous list of scientific
    # names from the NCBI Taxonomy Database to a list of GBIF species keys.
    
    def requires(self):
        return GetTaxonomySummaries()
        
    def output(self):
        return luigi.LocalTarget('data/gbif-species-matches.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)
            entrez_data = [line.split(',', maxsplit=1) for line 
                           in infile.read().splitlines()]
            ranks = ['SPECIES',
                    'SUBSPECIES',
                    'VARIETY',
                    'SUBVARIETY',
                    'FORM',
                    'SUBFORM',
                    'CULTIVAR_GROUP',
                    'CULTIVAR'
                    ]
            for i, line in enumerate(entrez_data):
                taxid, sname = line
                message = 'Progress: {0:.0%}'.format(i / len(entrez_data))
                self.set_status_message(message)
                if i % 10 == 0: print(message)
                payload = {'name': sname, 'kingdom':'plantae', 'strict':'true'}
                r = s.get(self.url + 'species/match', params=payload,
                          stream=False, timeout=self.timeout)
                if r.status_code == requests.codes.ok:
                    result = r.json()
                    if result['matchType'] != 'NONE' and result['rank'] in ranks:
                        data = [taxid,
                                result['speciesKey'],
                                result['phylum'],
                                result['order'],
                                result['family'],
                                result['genus'],
                                result['species']
                                ]
                        outfile.write(','.join([str(i) for i in data]) + '\n')
                                    
                        
class RemoveDuplicateSpeciesKeys(luigi.Task):

    # This task returns a list of unique species keys.

    def requires(self):
        return GBIFSpeciesMatch()
        
    def output(self):
        return luigi.LocalTarget('data/unique-species-keys.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            lines = [line.split(',') for line in infile.read().splitlines()]
            species_keys = set([line[1] for line in lines])
            for species_key in species_keys:
                outfile.write(species_key + '\n')
                
                
class PostUsageKeys(GBIFTask):

    # This task posts the previous list of unique species keys to the GBIF
    # Occurrence Store in chunk sizes of no larger than 300 and returns a list
    # of download IDs.

    chunk_size = 75 # Must be no greater than 300 per GBIF limits.
    retries = Retry(backoff_factor=4, status_forcelist=[503, 420], total=None,
        connect=10, read=10, redirect=10, status=250, method_whitelist=['POST'])
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)

    def requires(self):
        return RemoveDuplicateSpeciesKeys()
    
    def output(self):
        return luigi.LocalTarget('data/download-IDs.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())    
            species_keys = infile.read().splitlines()
            length = len(species_keys)
            number_of_chunks = math.ceil(length / self.chunk_size)
            s.mount(self.url, self.adapter)
            s.auth = (self.user, self.pwd)
            for i, start in enumerate(range(0, length, self.chunk_size)):
                message = 'Progress: {0:.0%}'.format(i / number_of_chunks)
                self.set_status_message(message)
                print(message)
                end = start + self.chunk_size
                if end < length:
                    chunk = species_keys[start:end]
                else:
                    chunk = species_keys[start:]
                payload = utils.generate_query_expression(chunk)
                r = s.post(self.url + 'occurrence/download/request',
                            json=payload, timeout=self.timeout)
                if r.status_code == 201:
                    download_id = r.text
                    outfile.write(download_id + '\n')
                        
                
class GetDOIs(GBIFTask):

    # This task returns a list of Digital Object Identifiers (DOIs)
    # corresponding to the previous list of download IDs.

    def requires(self):
        return PostUsageKeys()
    
    def output(self):
        return luigi.LocalTarget('data/DOIs.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            download_ids = infile.read().splitlines()
            s.mount(self.url, self.adapter)
            for download_id in download_ids:
                r = s.get(self.url + 'occurrence/download/' + download_id)
                if r.status_code == requests.codes.ok:
                    doi = r.json()['doi']
                    outfile.write(doi + '\n')
                        
                
class GetDownloadLinks(GBIFTask):
    
    # This task checks the status of the previously submitted download requests
    # and returns a list of download links.
    
    def requires(self):
        return PostUsageKeys()
    
    def output(self):
        return luigi.LocalTarget('data/download-links.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            download_ids = infile.read().splitlines()
            s.mount(self.url, self.adapter)
            for i, download_id in enumerate(download_ids):
                message = 'Progress: {0:.0%}'.format(i / len(download_ids))
                self.set_status_message(message)
                print(message)
                url = self.url + 'occurrence/download/' + download_id
                retry_max = 250
                for j in range(retry_max):
                    download_link = utils.get_download_link(s, url, self.timeout)
                    if download_link:
                        outfile.write(download_link + '\n')
                        break
                    else:
                        time.sleep(30)
                        continue
                else:
                    err_msg = ('Exceeded max retries while attempting to fetch '
                                'download link.')
                    raise Exception(err_msg)
                            
            
class DownloadOccurrences(GBIFTask):

    # This task downloads and consolidates the zipped occurrence datasets using
    # the previous list of download links.
    
    def requires(self):
        return GetDownloadLinks()
    
    def output(self):
        return luigi.LocalTarget('data/occurrences.zip', format=luigi.format.Nop)
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)
            download_links = infile.read().splitlines()
            with ZipFile(outfile, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for i, download_link in enumerate(download_links):
                    message = 'Progress: {0:.0%}'.format(i / len(download_links))
                    self.set_status_message(message)
                    print(message)
                    with s.get(download_link, stream=True, timeout=120) as r:
                        if r.status_code == requests.codes.ok:
                            stream = io.BytesIO(r.content)
                            utils.copy_stream(stream, archive)
                                

class GetRasterMetadata(luigi.Task):
    
    # This task stores metadata for the first raster file as a Pickle object.
    
    def output(self):
        return luigi.LocalTarget('data/raster-metadata.pickle',
            format=luigi.format.Nop)
    
    def run(self):    
        with ExitStack() as stack:
            outfile = stack.enter_context(self.output().open('w'))
            raster_dir = stack.enter_context(os.scandir('raster'))
            fpath = next(raster_dir).path
            raster_data = stack.enter_context(rasterio.open(fpath))
            pickle.dump(raster_data.meta, outfile)
            

class StackRasterData(luigi.Task):
    
    # This task iterates through the list of raster files and writes them to a 
	# new raster file as individual bands. A "stacked" raster file is returned.

    def requires(self):
        return GetRasterMetadata()

    def output(self):
        return luigi.LocalTarget('data/stacked-raster-data.tif',
            format=luigi.format.Nop)
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            raster_files = os.listdir('raster')
            metadata = pickle.load(infile, encoding='utf-8')
            metadata.update(count=len(raster_files))
            stacked = stack.enter_context(rasterio.open(outfile,'w', **metadata))
            for i, file_name in enumerate(raster_files):
                message = 'Progress: {0:.0%}'.format(i / len(raster_files))
                self.set_status_message(message)
                print(message)
                band = int(file_name[15:-4])
                with rasterio.open('raster/' + file_name) as source:
                    stacked.write_band(band, source.read(1))
            

class ConsolidateAndFilterOccurrences(luigi.Task):

    # This task iterates through the occurrence datasets and returns a
    # consolidated and filtered list of species keys and coordinates.

    # The coordinate uncertainty limit should be chosen based on the resolution 
	# of the raster data.
	coord_uncertainty_limit = luigi.IntParameter(default=4625)
    
    def requires(self):
        return [DownloadOccurrences(), GetRasterMetadata()]
    
    def output(self):
        return luigi.LocalTarget('data/consolidated-filtered-occurrences.txt')
    
    def run(self):
        with ExitStack() as outer_stack:
            infiles = [outer_stack.enter_context(input.open('r'))
						for input in self.input()]
			outfile = outer_stack.enter_context(self.output().open('w'))
            archive = outer_stack.enter_context(ZipFile(infiles[0], 'r'))
            files = archive.infolist()
            metadata = pickle.load(infiles[1], encoding='utf-8')
            xmin, ymin = metadata['transform'] * (0, metadata['height'])
            xmax, ymax = metadata['transform'] * (metadata['width'], 0)
            bounds = {'xmin': xmin, 'xmax': xmax, 'ymin': ymin, 'ymax': ymax}
            for i, file in enumerate(files):
                message = 'Progress: {0:.0%}'.format(i / len(files))
                self.set_status_message(message)
                print(message)
                with ExitStack() as inner_stack:
                    binary = inner_stack.enter_context(archive.open(file))
                    text = inner_stack.enter_context(io.TextIOWrapper(binary,
                        encoding='utf-8'))
                    reader = csv.reader(text, delimiter='\t',
                        quoting=csv.QUOTE_NONE)
                    next(reader) # Skips header.
                    for row in reader:                
                        coord_uncertainty = row[18]
                        x = row[17] #longitude
                        y = row[16] #latitude
                        species_key = row[29]
                        args = [coord_uncertainty, x, y,
                                self.coord_uncertainty_limit, bounds]
                        filter_ = utils.validate_and_filter(*args)
                        if filter_:
                            data = {'skey':species_key}
                            data.update(filter_)
                            outfile.write('{skey},{x},{y}\n'.format(**data))


class SampleRasterData(luigi.Task):

    # This task iterates through the list of filtered and consolidated species 
	# keys and coordinates and samples the raster file at each band.

    def requires(self):
        return [ConsolidateAndFilterOccurrences(),
                StackRasterData(),
                GetRasterMetadata()
				]
    
    def output(self):
        return luigi.LocalTarget('data/occurrences-climate-data.txt')
    
    def run(self):
        with ExitStack() as stack:
            infiles = [stack.enter_context(input.open('r')) for input
						in self.input()]
			stacked = stack.enter_context(rasterio.open(infiles[1]))
            outfile = stack.enter_context(self.output().open('w'))
            metadata = pickle.load(infiles[2], encoding='utf-8')
            nodata = metadata['nodata']
            lines = [line.split(',') for line in infiles[0].read().splitlines()]
            species_keys, x, y = zip(*lines)
            x = [float(i) for i in x]
            y = [float(i) for i in y]
            xy = list(zip(x,y))
            indexes = list(range(1, stacked.count + 1))
            samples = stacked.sample(xy, indexes)
            col_names = ['BIO' + str(i) for i in indexes]
            header = ['Species Key'] + col_names
            outfile.write(','.join(header) + '\n')
            for i, sample in enumerate(samples):
                message = 'Progress: {0:.0%}'.format(i / len(species_keys))
                self.set_status_message(message)
                if i % 1000 == 0: print(message)
                sample[sample == nodata] = np.NaN
                cleaned = [str(s) for s in list(sample.round(3))]
                data = [species_keys[i]] + cleaned
                outfile.write(','.join(data) + '\n')
        
        
class AggregateClimateData(luigi.Task):

    # This task groups the species keys and aggregates the bioclimatic variables
    # based on their mean.

    def requires(self):
        return SampleRasterData()
    
    def output(self):
        return luigi.LocalTarget('data/aggregated-occurrences.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            df = pd.read_csv(infile)
            grouped = df.groupby('Species Key')
            aggregated = grouped.mean().round(3)
            aggregated.to_csv(outfile)
            
            
class JoinData(luigi.Task):

    # This task performs a series of joins on the list of UIDs, Taxonomy IDs and
    # aggregated species keys.

    def requires(self):
        return [GetDocSummaries(), GBIFSpeciesMatch(), AggregateClimateData()]
    
    def output(self):
        return luigi.LocalTarget('data/joined-data.txt')
    
    def run(self):
        with ExitStack() as stack:
            infiles = [stack.enter_context(f.open('r')) for f in self.input()]
            outfile = stack.enter_context(self.output().open('w'))
            df2_column_names = ['Taxonomy ID',
                                'Species Key',
                                'Phylum',
                                'Order',
                                'Family',
                                'Genus',
                                'Species'
                                ]
            df1 = pd.read_csv(infiles[0], header=None,
						names=['UID','Taxonomy ID'], index_col=0)
            df2 = pd.read_csv(infiles[1], header=None,
						names=df2_column_names, index_col=0)
            df3 = pd.read_csv(infiles[2], index_col=0, float_precision='high')
            first_join = df1.join(df2, on='Taxonomy ID', how='inner')
            second_join = first_join.join(df3, on='Species Key', how='inner')
            second_join.to_csv(outfile)
            
            
class RunAllTasks(luigi.WrapperTask):
    
    # This dummy tasks invokes all upstream tasks.
    
    def requires(self):
        yield GetDOIs()
        yield JoinData()
        
    
if __name__ == '__main__':
    luigi.run()
        