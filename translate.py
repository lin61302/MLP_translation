import re
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
import torch
from transformers import MarianMTModel, MarianTokenizer
from pymongo import MongoClient
import subprocess
# from peacemachine.decorators import calculate_time

# import peacemachine stuff
# from peacemachine.helpers import UrlFilter

# TODO: switch to "title" + "maintext" / "title_original" + "maintext_original"

def translate_pipe(uri, language, batch_size, sources):
    """
    :param uri: The MongoDB uri for connecting to the main DB
    :param language: the iso2 code for the language to translate or "all" 
    """
    db = MongoClient(uri).ml4p

    if language == 'all':
        all_languages = db['languages'].distinct('language')
        for lang in all_languages:
            trans = Translator(uri, lang, batch_size)
            trans.run()

    elif isinstance(language, list):
        for lang in language:
            trans = Translator(uri, lang, batch_size)
            trans.run()

    else:
        trans = Translator(uri, language, batch_size, sources)
        trans.run()

def run_git_commands(commit_message):
    try:
        # Add only Python files using shell globbing
        subprocess.run("git add *.py", shell=True, check=True)
        # Commit changes with a message
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        # Push changes to the repository
        subprocess.run(["git", "push"], check=True)
        print("Git commands executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running Git commands: {e}")

class Translator:

    def __init__(self, mongo_uri, language, batch_size, sources):
        """
        :param mongo_uri: the uri for the db
        :param language: the iso2 language to translate for translation
        """
        self.mongo_uri = mongo_uri
        self.language = language
        self.batch_size = batch_size
        self.sources = sources
        self.db = MongoClient(mongo_uri).ml4p
        self.lang_info = self.db['languages'].find_one({'iso_code': language})
        self.model_type = self.lang_info['model_type']
        # self.model_location = self.lang_info['model_location']
        self.huggingface_name = self.lang_info['huggingface_name'] # ex 'Helsinki-NLP/opus-mt-ROMANCE-en'

        # get a filter instance going
        # self.filter = UrlFilter()

    def prep_batch(self, chunk, max_length):
        return self.tokenizer.prepare_seq2seq_batch(src_texts=chunk, max_length=max_length, return_tensors="pt")
    
    # @calculate_time
    def batch_translate(self, list_strings, max_length=100, num_beams=4):
        """
        batches and translates a list of strings
        :param list_strings: list of strings to translate
        :return: list of translated strings
        """
        # chunk the list into chunks size=n
        chunks = [list_strings[i:i + self.batch_size] for i in range(0, len(list_strings), self.batch_size)]
        chunks = [self.prep_batch(ch, max_length) for ch in chunks]
        # translate each chunk
        res = []
        for chunk in chunks:
            # send tensors to cuda
            batch = chunk.to(device='cuda')
            # translate
            translated_chunk = self.lang_model.generate(
                **batch,
                max_length=max_length,
                num_beams=num_beams,
                early_stopping=True
            )
            # decode
            translated = self.tokenizer.batch_decode(translated_chunk, skip_special_tokens=True, clean_up_tokenization_spaces=True)
            # add back into master list
            res += translated
        return res

    def fix_maintext(self, mt):
        # if self.language == 'hy':
        #     try:
        #         s = mt.replace('\n', '')
        #         s = mt.replace('"', '')
        #         s = mt.replace('“', '')
                
        #     except:
        #         pass
        if not mt:
            return '.'
        try:
            mt = re.sub(r'(\n)', '', mt)
            mt = re.sub(r'(-{3,})', ' ', mt)
        except AttributeError:
            return '.'
        except TypeError:
            return '.'
        return mt
    
    def fix_spanish(self, text):
        try:
            fixed_text = text.encode('latin1').decode('utf-8')
            return fixed_text
        except:
            return text

        

    
    def tci_locals(self):
        ### pull what I want 
        _ids = [_doc['_id'] for _doc in self.list_docs]

        raw_titles = [_doc['title'] for _doc in self.list_docs]
        if self.language == 'hy':
            raw_titles = [self.fix_maintext(rm) for rm in raw_titles]
        if self.language == 'es' or 'es2':
            raw_titles = [self.fix_spanish(rm) for rm in raw_titles]

        raw_maintext = [_doc['maintext'] for _doc in self.list_docs]
        raw_maintext = [self.fix_maintext(rm) for rm in raw_maintext]
        if self.language == 'es' or 'es2':
            raw_maintext = [self.fix_spanish(rm) for rm in raw_maintext]

        #### translate
        translated_titles = self.batch_translate(raw_titles)
        translated_maintext = self.batch_translate(raw_maintext)
        print(f'Articles translated in {self.colname}: ' + str(len(translated_titles)))

        # insert
        for nn, _id in enumerate(_ids):
            # insert everthing into article
            # _year = self.list_docs[nn]['date_publish'].year
            # _month = self.list_docs[nn]['date_publish'].month
            # colname = f'articles-{_year}-{_month}'
            try:
                # self.db[self.colname].update_one(
                #     {'_id': _id}, 
                #     {'$set': {
                #         'title_translated': translated_titles[nn],
                #         'maintext_translated': translated_maintext[nn],
                #         'language_translated': 'en'
                #     }}
                # )
                self.db[self.colname].update_one(
                    {'_id': _id}, 
                    {'$set': {
                        'title_translated': translated_titles[nn],
                        'maintext_translated': translated_maintext[nn],
                        # 'es_translation_update': 'Second', 
                        'language_translated': 'en'
                    }}
                )
            except:
                print('ERROR INSERTING')

    def run(self):
        """
        main function to run the translator
        """
        if self.model_type == 'huggingface':
            # load the translation model
            self.tokenizer = MarianTokenizer.from_pretrained(self.huggingface_name)
            # load the model 
            self.lang_model = MarianMTModel.from_pretrained(self.huggingface_name)
            self.lang_model = self.lang_model.to('cuda')

            dates = pd.date_range('2012-1-1', datetime.now()+relativedelta(months=1), freq='M')

            if self.language=='km2':
                lan='km'
            elif self.language=='pt2':
                lan='pt'
            elif self.language=='es2':
                lan='es'
            else:
                lan=self.language

            for date in dates:
                self.colname = f'articles-{date.year}-{date.month}'

                cursor = self.db[self.colname].find(
                    {
                        'source_domain': {'$in': self.sources},
                        'language': lan,
                        'include': True,
                        '$or':[{'title_translated': {'$exists': False}},
                        {'maintext_translated': {'$exists': False}}
                        ],
                        'title':{'$not': {'$type': 'null'}},
                        'title':{'$ne': ''},
                        'title':{'$type': 'string'},
                        'maintext':{'$not': {'$type': 'null'}},
                        'maintext':{'$ne': ''},
                        'maintext':{'$type': 'string'},
                        
                        # 'es_translation_update': {'$ne': 'Second'}
                    }
                ).batch_size(self.batch_size)

                self.list_docs = []

                for _doc in tqdm(cursor):
                    self.list_docs.append(_doc)
                    if len(self.list_docs) >= self.batch_size:
                        print('Translating')
                        try:
                            self.tci_locals()
                        except ValueError:
                            print('ValueError')
                        except AttributeError:
                            print('AttributeError')
                        self.list_docs = []

                # handle whatever is left over
                self.tci_locals()
                self.list_docs = []


        elif self.model_type == 'opennmt':
            # TODO: fill out the opennmt process 
            pass
    


if __name__ == '__main__':
    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    # # languages = [ 'es2', 'ar', 'uk', 'ru', 'fr', 'zh'] #international
    # languages = ['fr','es2','ar','sr','mk','ru'] #regional  Macedonian
    # # languages = ['mk','ru']
    # # languages = ['pt2','km2']
    # # languages = ['ru','kg']
    # # languages = ['km2']
    # # #dont do uz for UZB
    # # #dont do az for AZE
    
    
    
    # for language in languages:
    #     # source_domains = ['nicaraguainvestiga.com', 'agenciaocote.com', 'prensacomunitaria.org']
    #     # source_domains = ['divergentes.com', 'revistafactum.com', 'alharaca.sv']
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['KHM']}})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    #     source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    #     print(f'Start: {source_domains} -----in----- {languages}')
    #     translate_pipe(uri, language, 128, source_domains)
    # print(f'Done: {source_domains} -----in----- {languages}')


    # lan_dic = {'MOZ': ['pt2'], 'IDN': ['id'], 'CMR':['fr'], 'MAR':['ar','fr'], 'AGO': ['pt2'], 'PRY':['es'], 'MRT':['ar','fr']}
    # lan_dic = { 'DZA':['fr','ar'], 'ALB':['sq'],'MKD':['mk','sq'],'KHM':['km2'],'UKR':['uk','ru'],'UZB':['ru','uz'] }
    # lan_dic = {'ENV_UZB':['uz']}
    # lan_dic = {'ENV_AZE':['az'],'ENV_KGZ':['kg','ru'],'ENV_IDN':['id'],'ENV_MDA':['ro'],'ENV_MKD':['mk'],'ENV_COD':['fr'],'ENV_KAZ':['kk','ru'],'ENV_COL':['es'],'ENV_GTM':['es']}#,'ENV_':[],'ENV_':[]}
    # lan_dic = {'ENV_PRY':['es'], 'ENV_COL':['es'], 'ENV_TUN':['fr'],'ENV_BLR':['fr'],'ENV_IDN':['id'],'ENV_COD':['fr','ar'],'ENV_KHM':['km2'],'ENV_MLI':['fr']}
    # lan_dic = {'ENV_COL':['es'],'ENV_TUN':['fr'], 'ENV_DZA':['ar','fr'], 'ENV_PRY':['es'],  'ENV_XKX':['sq'],  'ENV_SEN':['fr'], 'ENV_IND':['hi'],'ENV_MOZ':['pt2'],'ENV_MEX':['es']}
    lan_dic  = {'BEN':['fr'],'UKR':['ru','uk'],'GTM':['es'],'NIC':['es'],'PRY':['es']}
    # lan_dic = {'ENV_KGZ':['kg','ru'], 'ENV_TUN':['fr'], 'ENV_IDN':['id'], 'ENV_MRT':['fr'], 'ENV_PER':['es'], 'ENV_PRY':['es'], 'ENV_RWA':['rw'], 'ENV_SEN':['fr'], 'ENV_TUR':['tr'], 'ENV_UKR':['uk','ru'], 'ENV_XKX':['sq'],}


    for country, languages in lan_dic.items():
        for language in languages:
            source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : [country]}})
            # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
            # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
            # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
            print(f'Start: {source_domains} -----in----- {language}')
            translate_pipe(uri, language, 128, source_domains)
            print(f'Done: {source_domains} -----in----- {language}')
     # Git operations
    commit_message = f"translation ({lan_dic.keys()}) update"
    run_git_commands(commit_message)
    print("it's working")

    

    # lan_dic = {'fr': ['SEN', 'BFA', 'BEN', 'CMR' ], 'es':['COL','SLV','NIC'], 'ru':['ARM'], 'hy':['ARM'], 'ur':['PAK']}
    # lan_dic = {'be':['BLR'], 'ru':['BLR'],'hu':['HUN'],'fr':['SEN'],'bn':['BGD'],'sq':['XKX']}

    # for language, countries in lan_dic.items():
        
    #     source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : countries}})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    #     print(f'Start: {source_domains} -----in----- {language}')
    #     translate_pipe(uri, language, 128, source_domains)
    #     print(f'Done: {source_domains} -----in----- {language}')



    # es retranslation from 2015
    #checklist: source_domain, 'es_translation_update': {'$ne': 'Second'} and 'es_translation_update': 'Second' Update, 2015~


##


