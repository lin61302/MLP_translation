'''
created: oct, 2021
zung-ru
'''
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
import torch
from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import nltk
import six

today = pd.Timestamp.now()

# url = "https://nlp-translation.p.rapidapi.com/v1/translate"

# headers = {
#     'x-rapidapi-host': "nlp-translation.p.rapidapi.com",
#     'x-rapidapi-key': "bdc32820ffmsh588cc1f65cabc7ep195c0fjsna6396a971023"
#     }

uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
# today = pd.Timestamp.now()
db = MongoClient(uri).ml4p


def pull_data(colname, src, lan):
    docs=[]
    cursor =  db[colname].find(        
        {
            'source_domain': {'$in':src},
            'include' : True,
            'language':lan,
            'title' :{'$type': 'string'},
            'title' : {'$not': {'$type': 'null'}},
            'title' : {'$not': {'$eq':None}},
            'title': {'$ne': ''},
            'test_title_translated': {'$exists': False},
            'maintext' :{'$type': 'string'},
            'maintext': {'$ne': ''},
            'maintext' : {'$not': {'$type': 'null'}},
            'maintext' : {'$not': {'$eq':None}},
            'maintext' : {'$not': {'$type': 'number'}},
            'test_maintext_translated': {'$exists': False}
        }
    ).limit(100)
    docs = [doc for doc in cursor]
    for i, doc in enumerate(docs):
        try:
            s = docs[i]['maintext'].replace('\n', '')
            s = docs[i]['maintext'].replace('"', '')
            s = docs[i]['maintext'].replace('“', '')
            docs[i]['maintext'] = s[:600]
        except:
            pass
    return docs


def translate_text(lan, text):
    url = "https://nlp-translation.p.rapidapi.com/v1/translate"
    
    headers = {
    'x-rapidapi-key': "e530af9f1dmsh2838648ebeddfb5p1acd2bjsn657d52d899a7",
    'x-rapidapi-host': "nlp-translation.p.rapidapi.com"
    }
    

    try:
        text = text.strip()
    except:
        pass

    
    try:
        payload = {"text":text,"to":"en","from":lan}
        response = requests.request("GET", url, headers=headers, params=payload)
        translated_text = re.findall('"translated_text":{"en":"(.*)"},', response.text)[0]
    except:
        translated_text = None    

    return translated_text



def main():

    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'

    df = pd.DataFrame()
    df['date'] = pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd') , freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    
    lan = 'rw'
    #src = ['habarileo.co.tz']
    src = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['UGA']}})
    print(src)


    for dt in df.index:
        colname = f'articles-{dt.year}-{dt.month}'
        docs = pull_data(colname=colname, src=src, lan=lan)
        title_translated = []
        maintext_translated = []
        print(colname,':',len(docs))
        for i, doc in enumerate(docs):
            try:
                trans_title = translate_text(lan=lan, text=docs[i]['title'])
                title_translated.append(trans_title)
                print(colname,'(title):', trans_title, '---',i,'/',len(docs))
            except Exception as err:
                print(err)
                pass
            try:
                trans_maintext = translate_text(lan=lan, text=docs[i]['maintext'])
                maintext_translated.append(trans_maintext)
                print(colname,'(maintext):', trans_maintext, '---',i,'/',len(docs))
            except Exception as err:
                print(err)
                pass
        
        for i, doc in enumerate(docs):
            try:

                db[colname].update_one(
                    {'_id': doc['_id']},
                    {
                    '$set': {
                        'language_translated': 'en',
                        'test_title_translated': title_translated[i],
                        'test_maintext_translated': maintext_translated[i]
                    }
                }
                )
                print(colname, title_translated[i],maintext_translated[i])
            except Exception as err: 
                print(err)
            

main()