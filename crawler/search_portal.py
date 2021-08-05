import json
from time import sleep
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import logging

def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            index_name: {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "link": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connected')
    else:
        print('Awww it could not connect!')
    return _es
def store_record(elastic_object, index_name, id, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, id=id, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
logging.basicConfig(level=logging.ERROR)
es_client = connect_elasticsearch()
urls_to_scrape =[]
urls_to_scrape.append('https://pureportal.coventry.ac.uk/en/persons/')
page = 1
while page != 45:
    url = f"https://pureportal.coventry.ac.uk/en/persons/?format=&page={page}"
    urls_to_scrape.append(url)
    page = page + 1

i = 1
for url in urls_to_scrape:
     r = requests.get(url, headers=headers)
     if r.status_code == 200:
         soup = BeautifulSoup(r.text, "html.parser")
         persons_name_list = soup.find_all('h3',class_='title')
         for person in persons_name_list:
             if person.find('a'):
                 sleep(1)
                 text = person.find('a').text
                 link = person.find('a')['href'] + '/publications/'
                 doc = {'title': text, 'link': link}
                 store_record(es_client, 'profiles', i, doc)
                 i = i+1
profiles_len = i-1
profile_id = 1
_id =1
while profile_id!=profiles_len:
    profile_doc = es_client.get(index='profiles', id =profile_id)
    url = profile_doc['_source']['link']
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        papers_list = soup.find_all('h3',class_='title')
        for paper in papers_list:
            if paper.find('a'):
                sleep(1)
                text =(paper.find('a').text)
                link =(paper.find('a')['href'])
                doc = {'title': text,'link': link}
                store_record(es_client, 'research_output', _id, doc)
                _id = _id +1
    profile_id = profile_id+1