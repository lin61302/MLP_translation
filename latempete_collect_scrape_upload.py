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
import re
import bs4
from bs4 import BeautifulSoup
from newspaper import Article
from dateparser.search import search_dates
import dateparser
import requests
from urllib.parse import quote_plus

import time
import json
import multiprocessing

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}



def latempete_story(soup):

    """
    Function to pull the information we want from latempete.info stories
    :param soup: BeautifulSoup object, ready to parse
    """
    #hold_dict = {}

    try:
        article_title = soup.find('h1', {'class':"tt-heading-title"}).text
         

    except:
        article_title = None
           
        
    try:
        finaltext = ''
        for i in soup.find_all('p'):
            finaltext += i.text

    except: 
        finaltext = None 



    try:
        
        article_date_text = soup.find('span',{'class':'tt-post-date-single'}).text
        article_date = dateparser.parse(article_date_text).replace(tzinfo = None)
        

    except:
        article_date = None


    return article_title,finaltext, article_date 


def update_db(list_title, list_maintext, list_date, list_year, list_month, urls):
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    db = MongoClient(uri).ml4p
    for i in range(len(list_date)):
        try:
            try:
                col_year =  str(int(list_year[i]))
                col_month = str(int(list_month[i]))
                colname = f'articles-{col_year}-{col_month}'
            
            except:
                col_year = None
                col_month = None
                colname = 'articles-nodate'
            
            
            
            l = [j for j in db[colname].find(
            {
                'url': urls[i]
            } )] 
            if l ==[]:
                db[colname].insert_one(
                    {
                    'date_publish': list_date[i],
                    'language': 'fr',
                    'title': list_title[i],
                    'source_domain': 'latempete.info',
                    'maintext': list_maintext[i],
                    'url': urls[i],
                    'year': col_year,
                    'month': col_month    
                }
                )   
                print(f'{i+1}/{len(list_date)}: inserted!:  {col_year}/{col_month} ------- {list_title[i]}')
                print('maintext:   ', list_maintext[i][:100])
            else:
                db[colname].update_one(
                    {'url': urls[i]},
                    {
                        '$set': {
                            'date_publish': list_date[i],
                            'language': 'fr',
                            'title': list_title[i],
                            'source_domain': 'latempete.info',
                            'maintext': list_maintext[i],
                            'year': col_year,
                            'month': col_month
                        }
                    }
                )
                print(f'{i+1}/{len(list_date)}: updated!:   {col_year}/{col_month} ------- {list_title[i]}')
                print('maintext:   ', list_maintext[i][:100])
                pass
        except:
            print('error!')

#keywords = [ ('was',630),('on',650), ('has',700), ('an',650), ('not',920),('were',210), ('not',920), ('all',50), ('have',400)]
#keywords = [('for',650), ('to',400),('as',400)]
universal_urls = []
indicator = True
   
for page in range(223,4193):
    
    list_title = []
    list_maintext = []
    list_date = []
    list_year = []
    list_month = []
    urls =[]
    print(page, '/4193 ----------------')
    try:
        #s = requests.Session()
        #s.cookies.clear()
        
        page_url = f'https://www.latempete.info/?cat=0&paged={page}'
        
        try:
            page_req = requests.get(page_url,headers=headers)
            page_soup = BeautifulSoup(page_req.content, features="lxml")
            for i in page_soup.find_all('a', {'class':"tt-post-title c-h5"}):
                link = i['href']
                urls.append(link)
        except:
            indicator = False
            while indicator == False:
                print("no urls, let's sleep")
                time.sleep(600)   
                page_url = f'https://www.latempete.info/?cat=0&paged={page}'
                try:
                    page_req = requests.get(page_url,headers=headers)
                    page_soup = BeautifulSoup(page_req.content, features="lxml")
                    for i in page_soup.find_all('a', {'class':"tt-post-title c-h5"}):
                        link = i['href']
                        urls.append(link)
                    indicator = True
                except:
                    pass  
      
    except:
        print('sleep error')
        pass

    
    for j, url in enumerate(urls):
        
        try:
            req = requests.get(url,headers=headers)
            soup = BeautifulSoup(req.content, features="lxml")
            title, text, date = latempete_story(soup=soup)
            list_title.append(title)
            list_maintext.append(text)
            list_date.append(date)
            
            try:
                year = date.year
                list_year.append(year)
            except:
                year = None
                list_year.append(year)
            try:
                month = date.month
                list_month.append(month)
            except:
                month = None
                list_month.append(month)

        except:
        
            list_title.append(None)
            list_maintext.append(None)
            list_date.append(None)
            list_year.append(None)
            list_month.append(None)
        
    proc = multiprocessing.Process(target=update_db(list_title=list_title, list_maintext=list_maintext, list_date=list_date, list_year=list_year, list_month=list_month, urls=urls))
    proc.start()        



