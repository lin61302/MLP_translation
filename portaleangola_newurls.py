# Packages:
import sys
import os
import re
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np 
from tqdm import tqdm

from pymongo import MongoClient


import bs4
from bs4 import BeautifulSoup
from newspaper import Article
from dateparser.search import search_dates
import dateparser
import requests
from urllib.parse import quote_plus

import urllib.request
import time
from time import time
import random
from random import randint, randrange
from warnings import warn
import json
from pymongo import MongoClient
from urllib.parse import urlparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
# from peacemachine.helpers import urlFilter
from newsplease import NewsPlease
from dotenv import load_dotenv

# db connection:
db = MongoClient('mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu').ml4p

direct_URLs = []

base = 'https://www.portaldeangola.com/sitemap-pt-post-'

for year in range(2021, 2022):
    year_str = str(year)
    for month in range(9, 13):
        if month < 10:
            month_str = '0' + str(month)
        else:
            month_str = str(month)

        sitemap = base + year_str +'-' + month_str +'.xml'
        print(sitemap)
        hdr = {'User-Agent': 'Mozilla/5.0'} #header settings
        req = requests.get(sitemap, headers = hdr)
        soup = BeautifulSoup(req.content)
        item = soup.find_all('loc')
        for i in item:
            url = i.text
            direct_URLs.append(url)

        print(len(direct_URLs))

final_result = direct_URLs.copy()
# final_result = final_result[3189:]
url_count = 0
processed_url_count = 0
source = 'portaldeangola.com'
for url in final_result:
    if url:
        print(url, "FINE")
        ## SCRAPING USING NEWSPLEASE:
        try:
            header = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36''(KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')}
            response = requests.get(url, headers=header)
            # process
            article = NewsPlease.from_html(response.text, url=url).__dict__
            # add on some extras
            article['date_download']=datetime.now()
            article['download_via'] = "Direct2"
            article['source_domain'] = source
            # title has no problem
            
            
            # custom parser
            soup = BeautifulSoup(response.content, 'html.parser')
            
            try:
                category = soup.find('a', {'class' :'tdb-entry-category'}).text.strip()
            except:
                category = 'News'
            print(category)
            if category in ['Opinião', 'Ciências e Tecnologia', 'CMaisinema', 'Cultura', 'Vida']:
                article['title'] = 'From uninterested category'
                article['date_publish'] = None
                article['maintext'] = None
                print(article['title'], category)
                
            else:
                try:
                    date = soup.find('time', {'class' : 'entry-date updated td-module-date'})['datetime']
                    article['date_publish'] = dateparser.parse(date).replace(tzinfo=None)
                except:
                    article['date_publish'] = None
                print("newsplease date: ",  article['date_publish'])
                
                print("newsplease title: ", article['title'])
                print("newsplease maintext: ", article['maintext'][:50])
      
  
      
            
            try:
                year = article['date_publish'].year
                month = article['date_publish'].month
                colname = f'articles-{year}-{month}'
                
            except:
                colname = 'articles-nodate'
            
            # Inserting article into the db:
            try:
                db[colname].insert_one(article)
                # count:
                if colname != 'articles-nodate':
                    url_count = url_count + 1
                    print("Inserted! in ", colname, " - number of urls so far: ", url_count)
                db['urls'].insert_one({'url': article['url']})
            except DuplicateKeyError:
                pass
                print("DUPLICATE! Not inserted.")
                
        except Exception as err: 
            print("ERRORRRR......", err)
            pass
        processed_url_count += 1
        print('\n',processed_url_count, '/', len(final_result), 'articles have been processed ...\n')
 
    else:
        pass

print("Done inserting ", url_count, " manually collected urls from ",  source, " into the db.")
