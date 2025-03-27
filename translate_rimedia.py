import requests
import pandas as pd
import re
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Set up MongoDB connection
uri = 'mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true'
db = MongoClient(uri).ml4p

# Define function to clean text
def clean_text(text):
    text = text.strip()
    text = re.sub(r'[\n\r]', ' ', text)      # Replace newlines with spaces
    text = re.sub(r'["“”]', '', text)        # Remove quotation marks
    text = re.sub(r'[^a-zA-Z0-9 .,!?]', '', text)  # Remove special characters
    return text[:600]  # Truncate if needed

def pull_data(colname, src, lan):
    # Query database with conditions for text quality and content
    cursor = db[colname].find({
        'source_domain': {'$in': src},
        'include': True,
        'language': lan,
        '$or':[{'title_translated': {'$exists': False}},
                        {'maintext_translated': {'$exists': False}}
                        ],
        'title':{'$not': {'$type': 'null'}},
        'title':{'$ne': ''},
        'title':{'$type': 'string'},
        'maintext':{'$not': {'$type': 'null'}},
        'maintext':{'$ne': ''},
        'maintext':{'$type': 'string'},
    })
    docs = [doc for doc in cursor]
    
    # Clean maintext field in each document
    for doc in docs:
        doc['maintext'] = clean_text(doc.get('maintext', ''))
    
    return docs

def translate_text(lan, text, max_chars=800):
    url = "https://nlp-translation.p.rapidapi.com/v1/translate"
    headers = {
        'x-rapidapi-key': "434ce95da0mshb30f379d71de653p1bf37fjsn1d589977c85b",
        'x-rapidapi-host': "nlp-translation.p.rapidapi.com"
    }

    # Clean and truncate text before sending to the API
    text = clean_text(text)[:max_chars]

    try:
        payload = {"text": text, "to": "en", "from": lan}
        response = requests.get(url, headers=headers, params=payload)
        translated_text = re.search(r'"translated_text":{"en":"(.*?)"}', response.text)
        if translated_text:
            return translated_text.group(1).replace('\\', '')
        return ""
    except Exception as err:
        print("Translation error:", err)
        return ""

def main():
    today = pd.Timestamp.now()
    df = pd.DataFrame({'date': pd.date_range('2012-1-1', today + pd.Timedelta(31, 'd'), freq='M')})
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    lan = 'tet'
    src = db.sources.distinct('source_domain', filter={'include': True, 'primary_location': {'$in': ['TLS']}})
    print("Sources:", src)

    for dt in df['date']:
        colname = f'articles-{dt.year}-{dt.month}'
        docs = pull_data(colname=colname, src=src, lan=lan)
        print(f"{colname}: {len(docs)}")

        # Translate and update each document
        title_translated, maintext_translated = [], []
        for i, doc in enumerate(docs):
            trans_title = translate_text(lan, doc.get('title', ''))
            trans_maintext = translate_text(lan, doc.get('maintext', ''))
            title_translated.append(trans_title)
            maintext_translated.append(trans_maintext)
            print(f"{colname} (title): {trans_title} --- {i+1}/{len(docs)}")
            print(f"{colname} (maintext): {trans_maintext} --- {i+1}/{len(docs)}")

        # Update documents in MongoDB
        for i, doc in enumerate(docs):
            if title_translated[i] and maintext_translated[i]:
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
                    print(f"Updated {colname}: {title_translated[i]} - {maintext_translated[i]}")
                except Exception as err:
                    print(f"Update error for document {doc['_id']}:", err)

main()
