# import packages
import json
from pymongo import MongoClient
import os
import sys

from peacemachine.helpers import regex_from_list
from dotenv import load_dotenv

# create the sitelist file
def create_sitelist(uri, config_location):
    """
    creates the sitelist.hjson for scraping
    :param url: the url 
    """
    # connect to the db
    db = MongoClient(uri).ml4p
    # create a holding list
    site_list = []
    # pull all the data from the db
    # query = {'source_domain': {'$in' : ['telegraf.al', 'panorama.com.al', 'gazetatema.net']}}
    countries = ['SRB']
    query = {'primary_location': {'$in' : countries}, 'include' : True}
    # query = {'major_international' : True, 'include' : True}



    for source in db.sources.find(query):
        if source.get('blacklist_url_patterns'):
            black_regex = regex_from_list(source.get('blacklist_url_patterns'), compile=False)
            s_dict_rec = {
                'url': source.get('full_domain'),
                'crawler': 'RecursiveSitemapCrawler',
                'ignore_regex': black_regex
            }
            s_dict_rss = {
                'url': source.get('full_domain'),
                'crawler': 'RssCrawler',
                'ignore_regex': black_regex
            }
        else: 
            s_dict_rec = {
                'url': source.get('full_domain'),
                'crawler': 'RecursiveSitemapCrawler',
            }
            s_dict_rss = {
                'url': source.get('full_domain'),
                'crawler': 'RssCrawler'
            }
        
        # append
        site_list.append(s_dict_rec)
        site_list.append(s_dict_rss)

    # create the final form
    main_dict = {'base_urls': site_list}

    # write the file
    if not config_location.endswith('/'):
        config_location = config_location + '/'

    with open(config_location + 'sitelist.hjson', 'w') as file:
        file.write(json.dumps(main_dict, indent=4))


def scrape_direct(uri, config_location):
    """
    directly scrapes the sites in the db from 
    """
    pass


# run the scraper
if __name__ == '__main__':
    
    load_dotenv()
    uri = os.getenv('DATABASE_URL')
    currpath = os.path.abspath(__file__)
    os.environ["NEWSPLEASE_CONFIG"] = os.path.dirname(os.path.dirname(os.path.dirname(currpath))) + '/newsplease_repo/config/'
    #config_path = os.getenv('NEWSPLEASE_CONFIG')
    config_path = '/home/ml4p/peace-machine/peacemachine/newsplease_repo/config/'
    create_sitelist(uri, config_path)

