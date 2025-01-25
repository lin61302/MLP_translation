import re
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
import torch
from pymongo import MongoClient

uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
today = pd.Timestamp.now()
db = MongoClient(uri).ml4p

def update_info(docs, list_maintext, list_title, colname):
    """
    updates the docs into the db
    """
    db = MongoClient(uri).ml4p

    for nn, _doc in enumerate(docs):
        colname = f"articles-{_doc['date_publish'].year}-{_doc['date_publish'].month}"
        db[colname].update_one(
            {
                '_id': _doc['_id']
            },
            {
                '$set':{
                    'maintext_translated':list_maintext[nn],
                    'title_translated':list_title[nn]
                            
                }
            }
        )

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
        #('Nigeria', 'NGA'),
        # ('Serbia', 'SRB'),
        # ('Senegal', 'SEN'),
        # ('Tanzania', 'TZA'),
        # ('Uganda', 'UGA'),
        # ('Ukraine', 'UKR'),
        # ('Zimbabwe', 'ZWE'),
        # ('Mauritania','MRT'),
        # ('Zambia', 'ZMB'),
        # ('Kosovo', 'XKX'),
        # ('Niger', 'NER'),
        # ('Jamaica', 'JAM'),
        # ('Honduras', 'HND'),
        # ('Rwanda', 'RWA'),
        # ('Philippines', 'PHL')
        ('Ghana', 'GHA')
    ]
for ctup in countries:
    country_name = ctup[0]
    country_code = ctup[1]

    date_range = pd.date_range(start='2012-1-1', end=today + pd.Timedelta(31, 'd'), freq='M')
    source_domain = [doc['source_domain'] for doc in db['sources'].find({'primary_location': {'$in': [country_code]},'include': True})]
    print(f'Start creating translating columns: {country_name}')
    for dt in date_range:
        collection = f'articles-{dt.year}-{dt.month}'
        cursor =  db[collection].find(        
            {
                'source_domain': source_domain,
                'include' : True,
                'language':'en',
                '$or':  [
                        {'maintext_translated': {'$exists': False}},
                        {'maintext_translated': {'$exists': False}}
                ]

            }
        )
        docs = [doc for doc in cursor]
        list_maintext = [doc['maintext'] for doc in docs]
        list_title = [doc['title'] for doc in docs]
        proc = multiprocessing.Process(target=update_info(docs = docs, list_maintext=list_maintext, list_title=list_title, colname = collection))
        proc.start()







