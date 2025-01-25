"""
Created on Nov 8 

Modified: zung-ru
"""
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
from newsplease import NewsPlease
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
# headers = {
#     "Connection": "keep-alive",
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
# }
def custom_parser(soup):

    """
    Custom parse the content given url and requested content
    Need to Modify: (1)title parser (2)maintext parser (3)date parser
    Return: title, maintext, and date
    Print: scraped content
    """
   
    
    # Title Parser: 
    try:
        for i in soup.find_all('h1'):
            title = i.text 
            #print('title:   ',title)
    except:
        try:
            title = soup.find_all('meta',property='og:title')[0]['content'] 
            #print('title:   ',title)
        except:
            title = None
            print('Custom_parser: Empty title!')
            
           
        

    # Maintext Parser
    try:
            maintext = ''
            for i in soup.find_all('p'):
                maintext+=i.text
            for i in soup.find_all('div', style="text-align: justify; "):
                maintext += i.text
            for i in soup.find_all('div', style="text-align: justify;"):
                maintext += i.text
            
            #print('maintext:   ', maintext[:50])


    except: 
        try: 
            maintext = ''
            for i in soup.find_all('div', style="text-align: justify; "):
                maintext += i.text
            #print('maintext:   ',maintext[:50])
            print('first try')

        except:
            try:
                maintext = ''
                for i in soup.find_all('p'):
                        maintext += i.text
                #print('maintext:   ',maintext[:50])
                print('second try')

            except:
                try:
                    maintext = ''
                    for i in soup.find('div', {'id':'article_content'}).find_all('p'):
                        maintext += i.text
                    #print('maintext:   ',maintext[:50])
                    

                except:
                    try:
                        maintext = ''
                        for i in soup.find_all('div', style="text-align: justify;"):
                            maintext += i.text
                        #print('maintext:   ',maintext[:50])
                        print('third try')

                    except:  
                        try:
                            
                            maintext = ''
                            for i in soup.find('div', {'id':'article-content'}).find_all('div', {'class':'a3s'}):
                                maintext += i.text   
                            #print('maintext:   ',maintext)
                            print('fourth try')
                        except:
                            try:
                                maintext = ''
                                for i in soup.find_all('p'):
                                    maintext += i.text
                                #print('maintext:   ',maintext)
                                print('fifth try')

                            except:
                                maintext = None
                                print('Custom_parser: Empty maintext!') 


    # Date Parser
    try:
        for i in soup.find_all('meta',{'property':'article:published_time'}):
            date = i['content']
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
                            print('Custom_parser: Empty date!')


    return title,maintext, date 





def update_db(list_article, list_year, list_month):
    '''
    Update database given title, maintext, date, urls, year, month, src and lan
    
    Need to Modify: No, all being assigned as arguments
    Return: X
    Print: insert or update outcome
    '''
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    db = MongoClient(uri).ml4p
    for i in range(len(list_article)):
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
                'url': list_article[i]['url']
            } )] 
            if l ==[]:
                db[colname].insert_one(
                    list_article[i]
                )   
                print(f'{i+1}/{len(list_article)}: insert success!: {colname} ---- (title){list_article[i]["title"][:40]}****(main){list_article[i]["maintext"][:40]}')
            else:
                # db[colname].update_one(
                #     {'url': list_article[i]['url']},
                #     {
                #         '$set': {
                #             'date_publish': list_article[i]['date_publish'],
                #             'title': list_article[i]['title'],
                #             'maintext': list_article[i]['maintext'],
                #             'year': col_year,
                #             'month': col_month
                #         }
                #     }
                #      )
                print(f'{i+1}/{len(list_article)}: already in, pass!: {colname} ---- (title){list_article[i]["title"][:40]}****(main){list_article[i]["maintext"][:40]}')
                
        except Exception as err:
            print(f'Error !!!! when updating ({err})')

def collect_urls_sitemap(num):  
    '''
    Collect urls given the iterative items in url  
    Need to Modify: (1) url pattern for sitemap
    '''
    urls=[]
    url = f"https://delo.ua/sitemaps/months/{num}/0.xml"
    print("Extracting from: ", url)
    reqs = requests.get(url, headers=headers)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    for link in soup.find_all('loc'):
        urls.append(link.text)
    #for link in soup.find_all('a'):
    #    urls.append(link.get('href')) 
    print(len(urls))
    
    return urls


def collect_urls_year_month_day(yy, mm, days): #(keyword[0],keyword[1])
    '''
    Collect urls given the iterative items in url
    Need to Modify: (1) iterable urls pattern (2) ways to collect urls (3) if any, can filter blacklist
    Return: list of urls
    Print: collecting outcome
    '''

    #iterate thru a month (31 days), collect all urls
    for dd in days:
        urls = []
        try:
            #### change url pattern
            page_url = f'https://kp.ua/archive/{yy}/{mm}/{dd}/'
            page_req = requests.get(page_url,headers=headers)
            page_soup = BeautifulSoup(page_req.content, features="lxml")
            for i in page_soup.find_all('a', {'class':'materials__title'}):
                link = 'https://kp.ua' + i['href']
                if '/daily/' in link:
                    continue
                if link in urls:
                    continue
                else:
                    urls.append(link)
            print(f'{yy}/{mm}/{dd} --------------- total urls:{len(urls)}')
        except:
            print(f'ERROR !!! collecting urls ----------- {yy}/{mm}/{dd}')
            pass   

    print(len(urls))
    
    return urls


def collect_urls_keyword(keyword, page):
    '''
    Collect urls given the iterative items in url
    Need to Modify: (1) iterable urls pattern (2) ways to collect urls (3) if any, can filter blacklist
    Return: list of urls
    Print: collecting outcome
    '''

    
    urls = []
    try:
        #### change url pattern
        page_url = f'https://delo.ua/ru/search/?date_from={keyword}-01-01&date_to={keyword}-12-31&page={page}&q=a&rubrics%5B0%5D=politics&rubrics%5B1%5D=economy&rubrics%5B2%5D=finance&rubrics%5B3%5D=society&rubrics%5B4%5D=opinions'
        page_req = requests.get(page_url,headers=headers)
        soup = BeautifulSoup(page_req.content, features="lxml")
        for i in soup.find_all('a',{'class':'c-card-list__link o-card__link'}):
            link = i['href']
            urls.append(link)
        urls = list(set(urls))
        print(f'total urls:{len(urls)}')
        #######
    except:
        print(f'ERROR !!! collecting urls ----------- {page}')
        pass   

    
    return urls         
            
            
    
       
            
def news_please(response, download_via, url):
    
    article = NewsPlease.from_html(response.text, url=url).__dict__
    article['date_download']= datetime.now()
    article['download_via'] = download_via
    
    return article
            

def get_content(urls, _custom_parser, download_via, _title, _maintext, _date):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    list_article = []
    list_year = []
    list_month = []
    
    for j, url in enumerate(urls):
        
        try:
            response = requests.get(url,headers=headers)
            article = news_please(response = response, download_via=download_via, url=url)

            soup = BeautifulSoup(response.content, features="lxml")
            
            if _custom_parser:
                try:
                
                    title, maintext, date_publish = custom_parser(soup=soup)
                    if _title:
                        article['title'] = title  
                    if _maintext:  
                        article['maintext'] = maintext
                    if _date:
                        article['date_publish'] = date_publish

                    

                except:
                    print('Error when custom parsing, please fix it!')
                
            list_article.append(article)
                    
            try:
                year = article['date_publish'].year
                month = article['date_publish'].month
                list_year.append(year)
                list_month.append(month)
                
            except:
                year = None
                month = None
                list_year.append(year)
                list_month.append(month)
            
        except Exception as err:
            print(err)
            list_article.append(None)
            list_year.append(None)
            list_month.append(None)
            
        try:
            print(f'({j+1}/{len(urls)})fixing content  {year}/{month}:  (title){article["title"][:40]} ----- (main){article["maintext"][:40]}')
        except:
            print('Error! Check whether article title and maintext exist')
            
            
    return list_article, list_year, list_month
    
def split_list(list_to_split, batch_size=20):
    length = len(list_to_split)
    wanted_parts= (length//batch_size)+1
    return [ list_to_split[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ] 
    

def pipeline(sitemap,
            year_month_day,
            keyword,
            _custom_parser,
            _title, _maintext, _date):
    unique_urls = []
    list_article = []
    list_year = []
    list_month = []
    
    
    if sitemap:
        '''
        Unit: each sitemap
        '''        
        begin_num = 1
        end_num = 3
        #selected_month = [1,10,11,12]
        for num in range(begin_num, end_num):
            # if num not in selected_month:
            #     continue
            
            print('sitemap: ',int(num)-int(begin_num),'/', int(end_num)-int(begin_num))
            #collect urls
            urls1 = collect_urls_sitemap(num)
            batch_size = 20
            ### if the progree is interruppted, you can restart from urls1[index * batch_size] 
            ### eg. if len(urls)= 2000, batch_size = 20, so the len(batched_list) =101,
            ### That is, if you stop at 30/101, you can restart at urls1[29*20] = urls1[580:]
            if len(urls1) > batch_size:
                batched_lists = split_list(urls1, batch_size = batch_size)
                for index, ll in enumerate(batched_lists):
                    print('sitemap: ',int(num)-int(begin_num),'/', int(end_num)-int(begin_num))
                    print(index+1, '/',len(batched_lists))
                    # check if unique
                    ll_urls = [url for url in ll if url not in unique_urls]
                    unique_urls += ll_urls
                    
                    #get content and fix it 
                    content1 = get_content(urls=ll_urls, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
                    list_article, list_year, list_month = content1[0], content1[1], content1[2]
                    
                    #update db by each sitemap
                    proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
                    proc.start()


            else:
                # check if unique
                urls1 = [url for url in urls1 if url not in unique_urls]
                unique_urls += urls1
                
                #get content and fix it 
                content1 = get_content(urls=urls1, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
                list_article, list_year, list_month = content1[0], content1[1], content1[2]
                
                #update db by each sitemap
                proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
                proc.start()
        

    
    if year_month_day:
        '''
        Unit: month
        '''
        years = [i for i in range(2012,2021)]
        months = ["%.2d" % i for i in range(12, 13)]
        days = ["%.2d" % i for i in range(1,32)]
        
        for yy in year:
            for mm in months:
                #collect urls
                urls2 = collect_urls_year_month_day(yy=yy, mm=mm, days=days)
                
                   # check if unique
                urls2 = [url for url in urls2 if url not in unique_urls]
                unique_urls += urls2
                
                #get content and fix it 
                content2 = get_content(urls=urls2, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
                list_article, list_year, list_month = content2[0], content2[1], content2[2]
                
                #update db by month
                proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
                proc.start()
                
                
    if keyword:
        
        #keywords = [ ('was',630),('on',650), ('has',700), ('an',650), ('not',920),('were',210), ('not',920), ('all',50), ('have',400)]
        keywords = [i for i in range(2015,2018)]
        for keyword_tuple in keywords:
            
            keyword = keyword_tuple
            num_page = 333
            
            for page in range(0,num_page):
                #collect urls
                print(f'keyword: {keyword} -------------------------- {page}/{num_page} ')
                urls3 = collect_urls_keyword(keyword=keyword, page=page)
                
                
                    # check if unique
                urls3 = [url for url in urls3 if url not in unique_urls]
                unique_urls += urls3
                print(f'keyword: {keyword} -------------------------- {page}/{num_page} ')
                
                #get content and fix it 
                content3 = get_content(urls=urls3, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
                list_article, list_year, list_month = content3[0], content3[1], content3[2]
                print(f'keyword: {keyword} -------------------------- {page}/{num_page} ')
                #update db by month
                proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
                proc.start()
                
def query_data(src, colname):
    uri = 'mongodb://ml4pAdmin:ml4peace@research-devlab-mongodb-01.oit.duke.edu'
    db = MongoClient(uri).ml4p

    cur = db.colname.find({
        'source_domain':{'$in':src}

    })
    query = [data for data in cur]

    return query

def fix_only_with_custom_parser(collections, start_year, end_year, src, _custom_parser, _title, _maintext, _date):

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    dates = pd.date_range(start=datetime(start_year,1,1), end=datetime(end_year,12,1), freq='m')

    if collections == 'year_month':
        for date in tqdm(dates):

            year = date.year
            month = date.month
            colname = f'articles-{year}-{month}'
            articles = query_data(src=src, colname=colname)

            urls = [article['url'] for article in articles]

            content = get_content(urls=urls, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
            list_article, list_year, list_month = content[0], content[1], content[2]

            proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
            proc.start()

    elif collections == 'nodate':

        articles = query_data(src=src, colname='articles-nodate')
        urls = [article['url'] for article in articles]
        content = get_content(urls=urls, _custom_parser = _custom_parser, download_via='',_title=_title, _maintext=_maintext, _date=_date)
        list_article, list_year, list_month = content[0], content[1], content[2] 

        proc = multiprocessing.Process(target=update_db(list_article = list_article, list_year = list_year, list_month = list_month))
        proc.start()
    else:
        print('No collection type to scrape specified!!!')
            
            
if __name__ == "__main__": 
    
    pipeline(sitemap = False,
            year_month_day = False,
            keyword = True,
            _custom_parser = False,
            _title=False, _maintext=False, _date=False)
            
### things to do everytime:
# choose at least one way: collect by sitemap, year_month_day, or keywords
# modify the fuction wrt the way you chose
# modify custom parser to fix title, maintext, date
# 

    # fix_only_with_custom_parser(collections='year_month', 
    #                             start_year=2012, 
    #                             end_year=2021, 
    #                             src=['newtimes.com'], 
    #                             _custom_parser=True,#always True
    #                             _title=False, 
    #                             _maintext=False, 
    #                             _date=True)
            