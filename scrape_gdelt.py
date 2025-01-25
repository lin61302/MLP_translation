"""
Script for collecting URLS that GDLET has and I don't 
"""
import getpass
from p_tqdm import p_umap
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
from functools import partial

# from peacemachine.helpers import download_url
from peacemachine.helpers import urlFilter
from newsplease import NewsPlease
from datetime import datetime
from pymongo.errors import DuplicateKeyError

def gdelt_download(uri, n_cpu=0.5):
    pass


def download_url(uri, url, download_via=None, insert=True, overwrite=False):
    """
    process and insert a single url
    """
    db = MongoClient(uri).ml4p

    try:
        # download
        header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
        response = requests.get(url, headers=header) 
        # process
        article = NewsPlease.from_html(response.text, url=url).__dict__
        # add on some extras
        article['date_download']=datetime.now()
        if download_via:
            article['download_via'] = download_via
        # insert into the db
        if not insert:
            return article
        if article:
            try:
                year = article['date_publish'].year
                month = article['date_publish'].month
                colname = f'articles-{year}-{month}'
            except:
                colname = 'articles-nodate'
            try:
                if overwrite:
                    db[colname].replace_one(
                        {'url': url},
                        article,
                        upsert=True
                    )
                else:
                    db[colname].insert_one(
                        article
                    )
                db['urls'].insert_one({'url': article['url']})
                print("Inserted in ", colname, article['url'])
            except DuplicateKeyError:
                pass
        return article
    except Exception as err: 
        pass

class GdeltDownloader:

    def __init__(self, uri, num_cpus):
        
        self.uri = uri
        self.num_cpus = num_cpus
        self.db = MongoClient(uri).ml4p
        self.source_domains = self.db.sources.distinct('source_domain', filter={'include' : True})
        self.missing_domains = []

    def check_domains(self, gdelt_url):
        """
        :param gdelt_url: the url to the gdelt file to check
        """
        # check to see if I have never processed this link
        if self.db.gdelt.count_documents({'url': gdelt_url}, limit=1) == 0:
            return self.source_domains

        # if already run, see if needs rerun for new domains
        old_source_domains = self.db.gdelt.find_one({'url': gdelt_url})['included_domains']

        # compare to the current domains
        # missing = list(set(self.source_domains) - set(old_source_domains))
        # missing = old_source_domains
        missing = self.db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in': ['PHL']}})

        if len(missing) > 0:
            return missing

        # if I don't need to run this link
        return None


    def parse_file(self, gdelt_url):

        try:

            # first check if I need to download / get the domains
            self.missing_domains = self.check_domains(gdelt_url)

            if self.missing_domains == None:
                return

            # if I need to get it
            # load into memory
            df = pd.read_table(gdelt_url, compression='zip', header=None)
            urls = df.iloc[:, -1]


            # filter urls for my domains
            # TODO: figure out how much more efficient it is to just check the domains from missing_domains

            # check that I don't already have them in the db
            # urls = [url for url in urls if self.db.urls.count_documents({'url': url}, limit=1) == 0]

            # check that I don't have any of the blacklist patterns
            ufilter = urlFilter(self.uri)
            urls = ufilter.filter_list(urls)
            urls1 = []
            for sd in self.missing_domains:
                urls1 += [url for url in urls if sd in url]
            p_umap(download_url, [self.uri]*len(urls1), urls1, ['gdelt']*len(urls1), num_cpus=8)
        
        except Exception as err:
            print('PARSING ERROR')
            pass

        
        # update the entry
        self.db.gdelt.update_one(
            {
                'url': gdelt_url
            },
            {
                '$set': {
                    'included_domains': self.source_domains
                }
            },
            upsert=True
        )

# TODO: get this working as a function call

if __name__ == "__main__":
    
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    db = MongoClient(uri).ml4p

    gd = GdeltDownloader(uri, 1)


    # source_domains = db.sources.distinct('source_domain')

    # global _uri
    # _uri = uri
    # global _source_domains
    # _source_domains = source_domains



    # get all the v1 links http://data.gdeltproject.org/events/index.html
    v1_index = BeautifulSoup(requests.get(
        'http://data.gdeltproject.org/events/index.html').content)

    v1 = [ff['href'] for ff in v1_index.find_all('a')]
    v1 = ['http://data.gdeltproject.org/events/'+ff for ff in v1 if ff.endswith('.zip') and
            not ff.startswith('GDELT.MASTERREDUCED')]

    # get the v2 english links
    v2_eng_index = requests.get('http://data.gdeltproject.org/gdeltv2/masterfilelist.txt').text.split('\n')
    v2_eng = [ll.split() for ll in v2_eng_index]
    v2_eng = [ll for sublist in v2_eng for ll in sublist if ll.endswith('.export.CSV.zip')]

    # get the v2 trans links
    v2_trans_index = requests.get('http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt').text.split('\n')
    v2_trans = [ll.split() for ll in v2_trans_index]
    v2_trans = [ll for sublist in v2_trans for ll in sublist if ll.endswith('.export.CSV.zip')]

    # combine them
    files = list(set(v1+v2_eng+v2_trans))

    # sort by most recent
    
    files = sorted(files, key = lambda x: int(x.split('/')[4][:x.split('/')[4].index('.')]), reverse=True)

    for f in files:
        print("PARSING:", f)
        gd.parse_file(f)