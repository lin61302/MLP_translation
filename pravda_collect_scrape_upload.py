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


def pravda_story(soup):

    """
    Function to pull the information we want from kp.com stories
    :param soup: BeautifulSoup object, ready to parse
    """
   

    # Get Title: 
    try:
        title = soup.find_all('meta',property='og:title')[0]['content'] 
        print('title:   ',title)
    except:
        try:
            title = ''
            for i in soup.find_all('h1'):
                title += i.text
            print('title:   ',title)
        except:
            title = None
            
           
        

    #if the news is behind the paywall, get every paragraph available
    try:
        maintext = ''
        for i in soup.find_all('p'):
            maintext+=i.text
            
        print('maintext:   ', maintext[:50])


    except: 
        try: 
            maintext = ''
            for i in soup.find_all('p'):
                maintext+=i.text
            for i in soup.find_all('div', style="text-align: justify; "):
                maintext += i.text
            for i in soup.find_all('div', style="text-align: justify;"):
                maintext += i.text
            print('maintext:   ',maintext[:50])
            print('first try')

        except:
            try:
                maintext = ''
                for i in soup.find_all('p'):
                        maintext += i.text
                print('maintext:   ',maintext[:50])
                print('second try')

            except:
                try:
                    maintext = ''
                    for i in soup.find('div', {'id':'article_content'}).find_all('p'):
                        maintext += i.text
                    print('maintext:   ',maintext[:50])
                    

                except:
                    try:
                        maintext = ''
                        for i in soup.find_all('div', style="text-align: justify;"):
                            maintext += i.text
                        print('maintext:   ',maintext[:50])
                        print('third try')

                    except:  
                        try:
                            maintext = ''
                            for i in soup.find('div', {'id':'article-content'}).find_all('div', {'class':'a3s'}):
                                maintext += i.text   
                            print('maintext:   ',maintext)
                            print('fourth try')
                        except:
                            try:
                                maintext = ''
                                for i in soup.find_all('p'):
                                    maintext += i.text
                                print('maintext:   ',maintext)
                                print('fifth try')

                            except:
                                maintext = None
                                print('Empty maintext!') 



    try:
        ds = str(json.loads(soup.find('script',type="application/ld+json").string))
        date = re.findall('article_publication_date": "(.*)"', s)[0]
        date = dateparser.parse(date).replace(tzinfo = None) 

    except:
        try:
            s = str(json.loads(soup.find('script',type="application/ld+json").string))
            date = re.findall("'datePublished': '(.*)', 'dateModified", s)[0]
            date = dateparser.parse(date).replace(tzinfo = None) 
        except:
            try:
                s = str(json.loads(soup.find('script',type="application/ld+json").string))
                date = re.findall('"datePublished": "(.*)", "dateModified', s)[0]
                date = dateparser.parse(date).replace(tzinfo = None)

            except:
                try:
                    s = str(soup.find_all('div',{'class':'date1'}))
                    date = re.findall('--></div>\n(.*)\n</div>', s)[0]
                    date = dateparser.parse(date).replace(tzinfo = None)
                except:    
                    try:
                        ds = str(json.loads(soup.find('script',type="application/ld+json").string))
                        date = re.findall("'article_publication_date': '(.*)'", s)[0]
                        date = dateparser.parse(date).replace(tzinfo = None)
                    except:
                        try:
                            date = '20'+re.findall("/20(.*)", url)[0][:5]
                            date = dateparser.parse(date).replace(tzinfo = None)

                        except:
                            date = None 





    return title,maintext, date 


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
                    'language': 'ru',
                    'title': list_title[i],
                    'source_domain': 'pravda.com.ua',
                    'maintext': list_maintext[i],
                    'url': urls[i],
                    'year': col_year,
                    'month': col_month    
                }
                )   
                print(f'{i+1}/{len(list_date)}: insert success!: {colname} - {list_title[i]} - {urls[i]}')
            else:
                # db[colname].update_one(
                #     {'url': df['article_url'][i]},
                #     {
                #         '$set': {
                #             'date_publish': list_date[i],
                #             'language': 'en',
                #             'title': list_title[i],
                #             'source_domain': 'pravda.com.ua',
                #             'maintext': list_maintext[i],
                #             'year': col_year,
                #             'month': col_month
                #         }
                #     }
                # )
                print(f'{i+1}/{len(list_date)}: already in, pass!: {colname}-{list_title[i]}')
                pass
        except:
            print('error!')

#keywords = ['the', 'is', 'for', 'to', 'as', 'be', 'can', 'was', 'were', 'not', 'on', 'all', 'has', 'have', 'an', 'will']
years = [i for i in range(2022,2023)]
months = ["%.2d" % i for i in range(1, 3)]
days = ["%.2d" % i for i in range(1,32)]

for yy in years:
    for mm in months:
        
        #empty lists for monthly data
        list_title = []
        list_maintext = []
        list_date = []
        list_year = []
        list_month = []
        urls =[]
        
        #iterate thru a month (31 days), collect all urls
        for dd in days:
            try:

                page_url = f'https://www.pravda.com.ua/news/date_{dd}{mm}{yy}/'
                page_req = requests.get(page_url,headers=headers)
                page_soup = BeautifulSoup(page_req.content, features="lxml")
                for foo in page_soup.find_all('div', {'class':'article_header'}):
                    link = 'https://www.pravda.com.ua' + foo.find('a')['href']
                    if link in urls:
                        pass
                    else:
                        urls.append(link)
                print(f'{yy}/{mm}/{dd} --------------- total unique urls:{len(urls)}')
            except:
                pass   
        
        print(len(urls))
        print(f'{yy}/{mm}/{dd}: start scraping!') 
        
        #scrape the monthly urls, append to lists
        for j, url in enumerate(urls):
            try:
                req = requests.get(url,headers=headers)
                soup = BeautifulSoup(req.content, features="lxml")
                title, text, date = pravda_story(soup=soup)
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
                print(f'({yy}/{mm}){j}/{len(urls)}: {title} ----- {year}-{month}')
            except:
                
                list_title.append(None)
                list_maintext.append(None)
                list_date.append(None)
                list_year.append(None)
                list_month.append(None)
            
        
        #update monthly data to db
        proc = multiprocessing.Process(target=update_db(list_title=list_title, list_maintext=list_maintext, list_date=list_date, list_year=list_year, list_month=list_month, urls=urls))
        proc.start()
        
        
            





