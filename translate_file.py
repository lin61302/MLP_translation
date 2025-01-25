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
# from peacemachine.decorators import calculate_time

# import peacemachine stuff
# from peacemachine.helpers import UrlFilter

# TODO: switch to "title" + "maintext" / "title_original" + "maintext_original"

def translate_pipe(uri, language, batch_size, sources, file_path):
    """
    :param uri: The MongoDB uri for connecting to the main DB
    :param language: the iso2 code for the language to translate or "all" 
    """
    db = MongoClient(uri).ml4p

    if language == 'all':
        all_languages = db['languages'].distinct('language')
        for lang in all_languages:
            trans = Translator(uri, lang, batch_size, file_path)
            trans.run()

    elif isinstance(language, list):
        for lang in language:
            trans = Translator(uri, lang, batch_size, file_path)
            trans.run()

    else:
        trans = Translator(uri, language, batch_size, sources, file_path)
        trans.run()


class Translator:

    def __init__(self, mongo_uri, language, batch_size, sources, file_path):
        """
        :param mongo_uri: the uri for the db
        :param language: the iso2 language to translate for translation
        """
        self.mongo_uri = mongo_uri
        self.language = language
        self.batch_size = batch_size
        self.sources = sources
        self.file_path = file_path
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
    def batch_translate(self, list_strings, max_length=400, num_beams=4):
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
        if not mt:
            return '.'
        try:
            mt = re.sub(r'(\n)', '. ', mt)
            mt = re.sub(r'(-{3,})', ' ', mt)
        except AttributeError:
            return '.'
        except TypeError:
            return '.'
        return mt

    
    def tci_locals(self, column_name):
        
        ### pull what I want 
        # _ids = [_doc['IDtweet'] for _doc in self.list_docs]


        raw_maintext = [_doc[column_name] for _doc in self.list_docs]
        raw_maintext = [self.fix_maintext(rm) for rm in raw_maintext]

        #### translate
        translated_maintext = self.batch_translate(raw_maintext)
        print(f'Articles translated: ' + str(len(translated_maintext)))
        

        # insert
        for nn, _id in enumerate(raw_maintext):
            # insert everthing into article
            # _year = self.list_docs[nn]['date_publish'].year
            # _month = self.list_docs[nn]['date_publish'].month
            # colname = f'articles-{_year}-{_month}'
            
            try:
                self.maintext_translated_list.append(translated_maintext[nn])
                # print(translated_maintext[nn])
            except:
                self.maintext_translated_list.append('')
                print('ERROR INSERTING')
                
    def split_list(self, list_to_split):
        batch_size = self.batch_size 
        length = len(list_to_split)
        wanted_parts= (length//batch_size)+1
        return [ list_to_split[i*length // wanted_parts: (i+1)*length // wanted_parts] 
                for i in range(wanted_parts) ] 

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

            df = pd.read_csv(self.file_path)
            self.file = df[df['google_language2']=='am']
            self.maintext_translated_list = []

            self.list_docs = []
            list_df = self.file.to_dict('records')

            ##
            columns = ['text_q83_1','text_q83_2','text_q83_3','text_q83_4','text_q83_5','text_q83_6','text_q83_7']
            ##
            for column_name in columns:
                for _doc in tqdm(list_df):
                    self.list_docs.append(_doc)
                    if len(self.list_docs) >= self.batch_size:
                        
                        print('Translating')
                        
                        try:
                            self.tci_locals(column_name)
                        except ValueError:
                            print('ValueError')
                        except AttributeError as err:
                            print('AttributeError', err)
                        self.list_docs = []

                # handle whatever is left over
                
                self.tci_locals(column_name)
                self.list_docs = []
                
                new_col_name = column_name + '(translated)'
                self.file[new_col_name] = self.maintext_translated_list
                self.maintext_translated_list = []
                
            self.file.to_csv('/home/ml4p/peace-machine/peacemachine/(am)text_endline_lang_q83(translated).csv')


        elif self.model_type == 'opennmt':
            # TODO: fill out the opennmt process 
            pass
    

if __name__ == '__main__':
    uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
    db = MongoClient(uri).ml4p
    #languages = ['fr', 'es','zh','ar','ru','uk']
    language = 'am'
    file_path = '/home/ml4p/peace-machine/peacemachine/text_endline_lang_q83.csv'
    source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['TUR']}})
    
    
    
    # for language in languages:
    #     #source_domains = ['euronews.com']
    #     source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['TUR']}})
    #     # source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'primary_location' : {'$in' : ['MLI']}, 'primary_language':language})
    #     #source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_international' : True})
    #     #source_domains = db.sources.distinct('source_domain', filter={'include' : True, 'major_regional' : True})
    translate_pipe(uri, language, 32, source_domains, file_path)


# wget -O UKtweets.csv "https://www.dropbox.com/s/650384r8ckl2bfs/TimelinesUkraineMPs-Government12082022.RData?dl=0"


