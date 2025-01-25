# get the proper domain format and requests the wayback snapshots
# make sure Ruby is installed + https://github.com/hartator/wayback-machine-downloader
import random
import sys
import os
import re
import subprocess
from p_tqdm import p_umap
from tqdm import tqdm
import ast
from pymongo import MongoClient
import random
from urllib.parse import urlparse
from datetime import datetime

from pymongo.errors import DuplicateKeyError

from peacemachine.helpers import urlFilter
# from peacemachine.helpers import download_url
from peacemachine.helpers import cut_url_query

from newsplease import NewsPlease
from urllib.parse import urljoin, urlparse
import requests

class WaybackDownloader:

    def __init__(self, uri, num_cpus=0.5):
        self.uri = uri
        self.num_cpus=num_cpus
        self.db = MongoClient(uri).ml4p # TODO: integrate the db into the uri
        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        # self.domain = self.get_domain_format(domain)
        self.exclude_regex = re.compile(r'(\/sports\/|\/deportes\/|\/meta\/|\/tags?\/|\/user\/)|\.(pdf|docx?|xlsx?|pptx?|epub|jpe?g|png|bmp|gif|tiff|webp|avi|mpe?g|mov|qt|webm|ogg|midi|mid|mp3|wav|zip|rar|exe|apk|css)$')
        self.exclude_regex_string = r'/(\/sports\/|\/deportes\/|\/meta\/|\/tags?\/|\/user\/)|\.(pdf|docx?|xlsx?|pptx?|epub|jpe?g|png|bmp|gif|tiff|webp|avi|mpe?g|mov|qt|webm|ogg|midi|mid|mp3|wav|zip|rar|exe|apk|css)$/'
        self.inserted_count = 0

    def get_domain_format(self, domain):
        """
        checks for proper url formatting (ex: include www. or not)
        """
        if not domain.startswith('http'):
            return self.db.sources.find_one({'source_domain': domain}).get('full_domain')


    def list_urls(self, domain, write=False):
        """
        get the list of urls from the wayback machine
        """
        # pull the url list
        print('Pulling the url list')
        urls_string = subprocess.run(['wayback_machine_downloader', 
                        domain+'*', '-l'],
                        stdout=subprocess.PIPE, text=True).stdout
        # format the string output into a list                        
        urls_string = urls_string[urls_string.index('['):]
        urls_string = urls_string.replace('\n', '')
        domain_urls = [dd['file_url'] for dd in ast.literal_eval(urls_string)]
        # drop images and sports - should be done in wayback pull
        domain_urls = [dd for dd in domain_urls if not bool(self.exclude_regex.search(dd))]
        # filter the urls
        uf = urlFilter(self.uri)
        domain_urls = uf.filter_list(domain_urls)
        # filter out the urls I already have in the db
        print('Filtering out articles I already have')
        domain_urls = [url for url in tqdm(domain_urls) if not self.in_articles(url)]
        # if write:
        #     with open(f'data/wayback_urls/{domain}_urls.txt', 'w') as _file:
        #         for url in domain_urls:
        #             _file.write(f'{url}\n')
        return domain_urls


    def wb_download(self, domain):
        """
        downloads the raw pages from the wayback machine
        """
        start_dir = os.getcwd()
        os.chdir('/mnt/i/wayback_temp')
        # os.system(f'wayback_machine_downloader {self.domain+"*"} -c 20 -x {self.exclude_regex_string}')
        subprocess.run(['wayback_machine_downloader', domain+'*', '-c', '20', '-x', self.exclude_regex_string])
        os.chdir(start_dir)


    def in_articles(self, url): 
        """
        check to see if the url is already in the db
        """
        # if bool(self.db['urls'].find_one({'url': url})):
        if bool(self.db['urls'].find_one({'url': url})):
            return True
        return False


    def run(self, domains):
        """
        function to run the full process
        """
        if domains == 'all':
            self.domains = self.db.sources.distinct('full_domain')
        else:
            self.domains = domains
        # holder list
        links = []
        # get all the urls I need to download
        print('Collecting links from the wayback machine')
        for dom in tqdm(self.domains):
            links += self.list_urls(dom)
        # shuffle and download
        random.shuffle(links)

        p_umap(download_url, links, num_cpus=self.num_cpus)


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
                print("Inserted in ", colname)
            except DuplicateKeyError:
                pass
        return article
    except Exception as err: # TODO detail exceptions
        print("ERRORRRR......", err)
        pass


if __name__ == "__main__":
    # uri = 'mongodb://ml4pAdmin:ml4peace@152.3.22.155'
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    db = MongoClient(uri).ml4p

    wb = WaybackDownloader(uri, num_cpus=1)

    # domains = [doc['full_domain'] for doc in db.sources.find()]
    # domains = [doc['full_domain'] for doc in db.sources.find(
    #     {
    #         # 'source_domain' : {'$in' : ['csmonitor.com', 'wsj.com', 'theguardian.com']}
    #         'major_regional': True,
    #         'include': True
    #     }
    # )]
    countries = ['SRB']


    
    domains = [doc['full_domain'] for doc in db.sources.find({'primary_location': {'$in': countries}, 'include' : True})]
    # domains = ['https://www.laprensa.hn/']
    # domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in': ['PHL', 'BGD']}})
    #domains = ['elheraldo.co', 'eltiempo.com', 'elcolombiano.com']

    urls = []
    for domain in domains:
        try:
            # pull the url list
            print(f'Pulling the url list: ', domain)
            urls_string = subprocess.run(['wayback_machine_downloader', 
                            domain+'*', '-l'],
                            stdout=subprocess.PIPE, text=True).stdout
            # format the string output into a list                        
            urls_string = urls_string[urls_string.index('['):]
            urls_string = urls_string.replace('\n', '')
            domain_urls = [dd['file_url'] for dd in ast.literal_eval(urls_string)]
            # filter the urls
            uf = urlFilter(uri)
            domain_urls = uf.filter_list(domain_urls)
            # filter out the urls I already have in the db

            print('Filtering out articles I already have')
            domain_urls = [cut_url_query(url) for url in domain_urls]
            # domain_urls = [url for url in tqdm(domain_urls) if not wb.in_articles(url)]
            urls += domain_urls
        except:
            pass
    random.shuffle(urls)
    p_umap(download_url, [uri]*len(urls), urls, ['wayback']*len(urls), num_cpus=10)

