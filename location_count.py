'''
created on 2022-11-30
zung-ru
'''
import os
import getpass
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
from pathlib import Path
import re
import pandas as pd
import time
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
import pandas as pd
import os
import re
from random import randint
from time import sleep
import sys
import time
from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
#import torch
#from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import nltk
import six
import json
import pycountry
import random
from cliff.api import Cliff
import multiprocessing


#1. mongodb pull data
#2. count doc['cliff_locations'] by year/ location

def count_location_pipeline(mongo_uri, batch_size, sources, my_cliff,  country):
    db = MongoClient(uri).ml4p
    locs = Clifftag(mongo_uri=uri, batch_size=batch_size, sources=sources, my_cliff=my_cliff,  country= country)
    locs.run()

class Clifftag:

    def __init__(self, mongo_uri, batch_size, sources, my_cliff,  country):
        """
        :param mongo_uri: the uri for the db
        :param batch_size: run one batch size at a time (geoparse, update)
        :param sources: source domains of interest
        :param my_cliff: Cliff api server url
        """
        self.mongo_uri = mongo_uri
        self.batch_size = batch_size
        self.sources = sources
        self.db = MongoClient(mongo_uri).ml4p
        self.my_cliff = my_cliff 
        self.country = country
        
    def pull_data(self, date):
        self.colname = f'articles-{date.year}-{date.month}'
        cursor = self.db[self.colname].find(
            {
                #'id': '60882a63f8bb748cadce4bf0'
                'source_domain': {'$in':self.sources},
                'include': True,
                'title_translated': {
                                    '$exists': True,
                                    '$ne': '',
                                    '$ne': None,
                                    '$type': 'string'
                                },
                'maintext_translated': {
                                        '$exists': True,
                                        '$ne': '',
                                        '$ne': None,
                                        '$type': 'string'
                                        },
                'cliff_locations':{'$exists':True}
                
                #'mordecai_locations.' + country_code : {'$exists' : True}
                #'mordecai_locations':{'$exists':True}
            }, batch_size=1
        )
        docs = [doc for doc in cursor]
        return docs
    
    def count_locations(self, all_years_data):
        
        locations_dict = {}
        
        # iterate by year: (list of data, year)
        for docs, year in zip(all_years_data, self.years):
            
            # iterate doc by year
            for doc in docs:
                
                # iterate (country, location list) 
                try:
                    for item in doc['cliff_locations'].items():
                        country_code = item[0] 
                        location_list = item[1]
                        
                        for location in location_list:
                            
                            key = f'{location}  ({country_code})'
                            
                            if key not in locations_dict:
                                locations_dict[key] = {yr:0 for yr in self.years}
                                locations_dict[key]['Country'] = country_code
                                
                                
                            locations_dict[key][year] += 1
                except:
                    pass
                        
            print(f'({self.country}) Count done -- ', year)
                    
                    
                    
        return locations_dict
                    
        
    
            
        
        
    def run(self):
        
        self.year_months = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M')
        years = list(set([date.year for date in self.year_months]))
        years.sort()
        self.years = years
        
    
        all_years_data = []
         
        for year in self.years:
            
            year_data = []
            for year_month in self.year_months:
                if year_month.year == year:
                    
                    # pull yearly data as a list
                    month_data = self.pull_data(year_month)
                    print(f'({self.country}) pulling data from ', year_month)
                    year_data += month_data
                    
                else:
                    continue
        
            all_years_data.append(year_data)
            
            
             
        locations_dict = self.count_locations(all_years_data)
        df = pd.DataFrame(locations_dict)
        path = f'/home/ml4p/Dropbox/Dropbox/ML for Peace/Location_Count/'
        if not os.path.exists(path):
            Path(path).mkdir(parents=True, exist_ok=True)

        df.to_csv(path + f'locations_mentioned_by_{self.country}.csv')
        # df.to_csv(f'/home/ml4p/peace-machine/peacemachine/{self.country}.csv')
        
            
                
        
        
            
        
        
    
    
    
if __name__ == '__main__':

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    my_cliff = Cliff('http://localhost:8080')
    
    
    # source_domains = ['balkaninsight.com']
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['']}})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    
    countries = [
        # ('Albania', 'ALB'), 
        # ('Benin', 'BEN'),
        # ('Colombia', 'COL'),
        # ('Ecuador', 'ECU'),
        # ('Ethiopia', 'ETH'),
        # ('Georgia', 'GEO'),
        # ('Kenya', 'KEN'),
        # ('Paraguay', 'PRY'),
        # ('Mali', 'MLI'),
        # ('Morocco', 'MAR'),
        # ('Nigeria', 'NGA'),
        # ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        # ('Ukraine', 'UKR'),
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania', 'MRT'),
        # ('Zambia', 'ZMB'),
        # ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Philippines', 'PHL'),
        # ('Ghana', 'GHA'),
        # ('Rwanda','RWA'),
        # ('Guatemala','GTM'),
        # ('Belarus','BLR'),
        # ('DR Congo','COD'),
        # ('Cambodia','KHM'),
        # ('Turkey','TUR'),
        # ('Bangladesh', 'BGD'),
        # ('El Salvador', 'SLV'),
        # ('South Africa', 'ZAF'),
        # ('Tunisia','TUN'),
        # ('Indonesia','IDN'),
        # ('Nicaragua','NIC'),
        # ('Angola','AGO'),
        # ('Armenia','ARM'),
        # ('Sri Lanka', 'LKA'),
        ('Malaysia','MYS'),
        # ('Cameroon','CMR'),
        # ('Hungary','HUN'),
        # ('Malawi','MWI'),
        # ('Uzbekistan','UZB')
    ]
    
    for country_tuple in countries:
        country = country_tuple[1]
        
        source_domains = [doc['source_domain'] for doc in db['sources'].find(
                {
                    'primary_location': {'$in': [country]},
                    'include': True
                }
            )]

        count_location_pipeline(mongo_uri=uri, batch_size=128, sources=source_domains, my_cliff=my_cliff, country = country)