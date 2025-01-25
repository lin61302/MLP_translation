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
            'title' : {'$not': {'$type': 'null'}},
            'title': {'$ne': ''},
            'title_translated': {'$exists': False},
            'maintext': {'$ne': ''},
            'maintext': {'$ne': None},
            'maintext' : {'$not': {'$type': 'null'}},
            'maintext_translated': {'$exists': False}
        }
    )
    docs = [doc for doc in cursor]
    for i, doc in enumerate(docs):
        s = docs[i]['maintext'].replace('\n', '')
        s = docs[i]['maintext'].replace('"', '')
        s = docs[i]['maintext'].replace('“', '')
        docs[i]['maintext'] = s[:1700]
    return docs


def translate_text(lan, text):
    url = "https://rimedia-translation.p.rapidapi.com/api_translate_unlimited.php"
    headers = {
    'content-type': "application/x-www-form-urlencoded",
    'x-rapidapi-key': "e530af9f1dmsh2838648ebeddfb5p1acd2bjsn657d52d899a7",
    'x-rapidapi-host': "rimedia-translation.p.rapidapi.com"
    }

    try:
        text = text.strip()
    except:
        pass

    
    try:
        payload = f'text={text}&from={lan}&to=en&translate_capital=true'
        response = requests.request("POST", url, data=payload.encode('utf-8'), headers=headers)
        translated_text = re.findall('"en": "(.*)"', response.text)[0]
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
    
    lan = 'ka'
    #src = ['kosova-sot.info']
    src = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['GEO']}})
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
                s = docs[i]['maintext']
                t = {'s1':['',True], 's2':['',False], 's3':['',False]}
                main_sents = nltk.sent_tokenize(s)
                l = ['s1','s2','s3']

                for j, string in enumerate(l):
                    while t[string][1]:                        
                        try:
                            sent = main_sents[0]
                        except:
                            sent = ''
                        
                        if len(t[string][0]+sent)<=450:
                            t[string][0]+=sent                           
                            try:
                                main_sents.pop(0)
                            except:
                                break
                        else:
                            t[string][1] = False
                            try:
                                t[l[j+1]][1]=True
                            except:
                                break

                if t['s1'][0]=='':
                    t['s1'][0] = s[:470]
                    t['s2'][0] = s[471:940]
                    t['s3'][0] = s[941:1411]

                elif t['s1'][0]!='' and t['s2'][0]=='':
                    t['s2'][0] = s[471:940]
                    t['s3'][0] = s[941:1411]
                    
                elif t['s2'][0]!='' and t['s3'][0]=='':
                    t['s3'][0] = s[941:1411]

                try:
                    trans_maintext = translate_text(lan=lan, text=t['s1'][0])+translate_text(lan=lan, text=t['s2'][0])+translate_text(lan=lan, text=t['s3'][0])
                except:
                    try:
                        trans_maintext = translate_text(lan=lan, text=t['s1'][0])+translate_text(lan=lan, text=t['s2'][0])
                    except:
                        trans_maintext = translate_text(lan=lan, text=t['s1'][0])
                
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
                        'title_translated': title_translated[i],
                        'maintext_translated': maintext_translated[i]
                    }
                }
                )
                print(colname, title_translated[i],maintext_translated[i])
            except Exception as err: 
                print(err)
            

main()