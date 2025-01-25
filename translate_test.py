from deep_translator import GoogleTranslator
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

uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
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
            # 'title_translated': {'$exists': False},
            'maintext' :{'$type': 'string'},
            'maintext': {'$ne': ''},
            'maintext' : {'$not': {'$type': 'null'}},
            'maintext' : {'$not': {'$eq':None}},
            'maintext' : {'$not': {'$type': 'number'}},
            # 'maintext_translated': {'$exists': False}
        }
    )
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
    
    

    try:
        text = text.strip()
    except:
        pass

    
    try:
        translated_text = GoogleTranslator(source=lan, target='en').translate(text)
    except:
        translated_text = None    

    try:
        translated_text = translated_text.replace('\\', '')
    except:
        pass

    return translated_text

# Use any translator you like, in this example GoogleTranslator
# output -> Weiter so, du bist großartig


def main():

    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'

    df = pd.DataFrame()
    df['date'] = pd.date_range('2016-1-1', today + pd.Timedelta(31, 'd') , freq='M')
    df.index = df['date']
    df['year'] = [dd.year for dd in df.index]
    df['month'] = [dd.month for dd in df.index]
    
    lan = 'uz'
    #src = ['habarileo.co.tz']
    src = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['UZB']}})
    # src = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
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
                if title_translated[i]==None or maintext_translated[i]==None:
                    print('no maintext or title translated, no upload, continue')
                    continue

                db[colname].update_one(
                    {'_id': doc['_id']},
                    {
                    '$set': {
                        'language_translated': 'en',
                        'title_translated': title_translated[i],
                        'maintext_translated': maintext_translated[i]
                    }
                }
                )
                print(colname, title_translated[i],maintext_translated[i])
            except Exception as err: 
                print(err)
            

main()
