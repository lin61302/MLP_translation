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



def kyivpostcom_story(soup):

    """
    Function to pull the information we want from kyivpost.com stories
    :param soup: BeautifulSoup object, ready to parse
    """
    hold_dict = {}

    try:
        article_title = soup.find("h1", {"class": "post-title"}).text
         

    except:
        article_title = None
           
        

    #if the news is behind the paywall, get every paragraph available
    try:
        maintext_body = soup.find("div", {"class": "content-block"})
        try:
            maintext = maintext_body.find_all('p')
            finaltext = ''
            for paragraph in maintext:
                paragraph_final = paragraph.text.strip()
                finaltext += paragraph_final
    # if there is no paywall block, then get everything
        except:
            finaltext = soup.find("div", {"id": "printableAreaContent"}).text.strip()

        finaltext = finaltext.replace('Read more here.', '') 
        print(finaltext[:50])

    except: 
        finaltext = None 



    try:
        
        article_date_text = soup.find("time").text
        article_date_text = article_date_text.strip()
        article_date_text = article_date_text.split('  ')[0]
        article_date_text = article_date_text.replace('Published', '').strip()
        article_date = dateparser.parse(article_date_text)
        


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
                    'language': 'en',
                    'title': list_title[i],
                    'source_domain': 'kyivpost.com',
                    'maintext': list_maintext[i],
                    'url': urls[i],
                    'year': col_year,
                    'month': col_month    
                }
                )   
                print(f'{i+1}/{len(list_date)}: insert success!: {colname} - {list_title[i]} - {urls[i]}')
            else:
                db[colname].update_one(
                    {'url': urls[i]},
                    {
                        '$set': {
                            'date_publish': list_date[i],
                            'language': 'en',
                            'title': list_title[i],
                            'source_domain': 'kyivpost.com',
                            'maintext': list_maintext[i],
                            'year': col_year,
                            'month': col_month
                        }
                    }
                )
                print(f'{i+1}/{len(list_date)}: already in, pass!: {colname}-{list_title[i]}')
                pass
        except:
            print('error!')

#keywords = [ ('was',630),('on',650), ('has',700), ('an',650), ('not',920),('were',210), ('not',920), ('all',50), ('have',400)]
keywords = [('',45)]
universal_urls = []

for index, keyword_set in enumerate(keywords):
    keyword = keyword_set[0]
    num = keyword_set[1]
    list_title = []
    list_maintext = []
    list_date = []
    list_year = []
    list_month = []
    urls =[]
    
    for page in range(0,num):
        try:
            
            page_url = f'https://www.kyivpost.com/page/{page}/?s={keyword}'
            page_req = requests.get(page_url,headers=headers)
            page_soup = BeautifulSoup(page_req.content, features="lxml")
            for i in page_soup.find('div', {'class':"blog-loop grid-row"}).find_all('a'):
                link = i['href']
                if link in universal_urls:
                    pass
                else:
                    universal_urls.append(link)
                    urls.append(link)
            print(f'{index+1}/16: {keyword} ----- Page:{page} ----- total unique urls:{len(urls)}')
        except:
            pass
        
    print(len(urls))
    print(f'{index+1}: start scraping!')    
    
    
    for j, url in enumerate(urls):
        try:
            req = requests.get(url,headers=headers)
            soup = BeautifulSoup(req.content, features="lxml")
            title, text, date = kyivpostcom_story(soup=soup)
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
            print(f'({keyword}){j}/{len(urls)}: {title} ----- {year}-{month}')

        except:
        
            list_title.append(None)
            list_maintext.append(None)
            list_date.append(None)
            list_year.append(None)
            list_month.append(None)
        
    proc = multiprocessing.Process(target=update_db(list_title=list_title, list_maintext=list_maintext, list_date=list_date, list_year=list_year, list_month=list_month, urls=urls))
    proc.start()        



