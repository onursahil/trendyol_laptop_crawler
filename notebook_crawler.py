from bs4 import BeautifulSoup
import requests
import csv
import re
import time
import pandas as pd
from pymongo import MongoClient 
import fasttext
import fasttext.util
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from elasticsearch_dsl import Search, Q

agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}

try: 
    conn = MongoClient(port=27017)
    print("Connected successfully!!!")
except:   
    print("Could not connect to MongoDB")


def index_to_elasticsearch(product_df):
    print("\nSTART INDEXING TO ELASTICSEARCH")

    if '_id' in product_df.colums:
        del product_df['_id']

    es = Elasticsearch(['http://localhost:9200'], http_auth=('elastic', 'changeme'))

    es_index = {
        "mappings": {
        "properties": {
            "product_link": {
            "type": "text"
            },
            "product_category": {
            "type": "text"
            },
            "product_title": {
            "type": "text"
            },
            "product_information": {
            "type": "text"
            },
            "product_cpu_type": {
            "type": "text"
            },
            "product_ram": {
            "type": "text"
            },
            "product_display_card_type": {
            "type": "text"
            },
            "product_ssd_capacity": {
            "type": "text"
            },
            "product_display_card_memory": {
            "type": "text"
            },
            "product_cpu_model": {
            "type": "text"
            },
            "product_cpu_generation": {
            "type": "text"
            },
            "product_capacity": {
            "type": "text"
            },
            "product_os": {
            "type": "text"
            },
            "product_title_vector": {
            "type": "dense_vector",
            "dims": 300
            }
        }
        }
    }

    es.indices.create(index='trendyol_laptop', body=es_index, ignore=[400])

    def getQuotes():
        for index, row in product_df.iterrows():
            yield {
                "_index": 'trendyol_laptop',
                "product_link": row['link'],
                "product_category": row['category'],
                "product_title": row['title'],
                "product_information": row['information'],
                "product_cpu_type": row['İşlemci Tipi'],
                "product_ram": row['Ram (Sistem Belleği)'],
                "product_display_card_type": row['Ekran Kartı Tipi'],
                "product_ssd_capacity": row['SSD Kapasitesi'],
                "product_display_card_memory": row['Ekran Kartı Hafızası'],
                "product_cpu_model": row['İşlemci Modeli'],
                "product_cpu_generation": row['İşlemci Nesli'],
                "product_capacity": row['Kapasite'],
                "product_os": row['İşletim Sistemi'],
                "product_title_vector": row['item_vectors']
            }
    bulk(client=es, actions = getQuotes(), request_timeout = 120)

    print('\nELASTICSEARCH INDEXING ENDED...\n')

def create_vectors(product_df):
    ft = fasttext.load_model('cc.tr.300.bin')

    itemname_vectors = []
    item_title = product_df['title'].tolist()
    
    for i in range(len(item_title)):
        vector = ft.get_sentence_vector(item_title[i])
        itemname_vectors.append(list(vector))
    
    return itemname_vectors


def write_to_file(product):
    print("\nWRITING TO FILE...")
    product_df = pd.DataFrame(product)
    product_df.to_csv('trendyol_laptop_vectors.csv')

    return product_df


def write_to_mongodb(product):
    # database 
    db = conn.my_db

    # Created or Switched to collection names: my_gfg_collection
    collection = db.trendyol_notebook
    
    # Insert Data
    rec_id = collection.insert_one(product)
    print("Inserted Data")

    # print("Data inserted with record ids ", rec_id)

    # Printing the data inserted
    # cursor = collection.find()
    # for record in cursor:
    #     print(record)


def crawl(START_PAGE, pi):
    print("\nSTARTED CRAWLING DATA...")
    products = []
    for i in range(1, pi+1):
        START_PAGE = START_PAGE + '?pi=' + str(i)
        r = requests.get(START_PAGE, headers=agent)
        soup = BeautifulSoup(r.content, "lxml")
        product_div = soup.findAll('div', attrs={"class": "p-card-chldrn-cntnr"})
        for i in range(len(product_div)):
            product = {}
            # print("{}th product".format(i + 1))
            link = product_div[i].find('a', href=True)['href']

            # PRODUCT LINK
            product_link  = 'https://www.trendyol.com/' + link

            product_page = requests.get(product_link, headers=agent)
            product_soup = BeautifulSoup(product_page.content, "lxml")

            # PRODUCT CATEGORY
            product_path = product_soup.findAll("a", attrs={"class": "breadcrumb-item"})
            try:
                product_category = product_path[len(product_path) - 2].get_text()
            except:
                product_category = ""
                continue

            # PRODUCT INFORMATION
            product_info = product_soup.find("div", attrs={"class": "pr-in-dt-cn"}).get_text()

            # PRODUCT TITLE
            product_title = product_soup.find('h1', attrs={"class": "pr-new-br"}).get_text()

            # PRODUCT SPECS
            product_spec_key_list = []
            product_spec_value_list = []
            product_spec_key = product_soup.findAll('div', attrs={"item-key"})
            product_spec_value = product_soup.findAll('div', attrs={'item-value'})
            
            for i in range(len(product_spec_key)):
                k = product_spec_key[i].get_text()
                k = k.replace(":", "")
                # key_headers = ["Ram (Sistem Belleği)", "Ekran Kartı Tipi", "Kapasite", "SSD Kapasitesi", "İşlemci Tipi", "İşlemci Modeli", "İşlemci Nesli", "Ekran Kartı", "Ekran Kartı Hafızası", "İşletim Sistemi", "Çözünürlük"]
                key_headers = ['İşlemci Tipi', 'Ram (Sistem Belleği)', 'SSD Kapasitesi', 'Kapasite', 'İşletim Sistemi', 'İşlemci Nesli', 'Ekran Kartı Hafızası', 'İşlemci Modeli', 'Ekran Kartı Tipi']
                if k in key_headers:
                    product_spec_key_list.append(k)
                    product_spec_value_list.append(product_spec_value[i].get_text())

            product['link'] = product_link
            product['category'] = product_category
            product['title'] = product_title
            product['information'] = product_info

            for i in range(len(product_spec_key_list)):
                product[product_spec_key_list[i]] = product_spec_value_list[i]

            # Write into Mongodb Database
            write_to_mongodb(product)

            # Index to Elasticsearch

            products.append(product)
            print(len(products))

    print("\nWRITING TO MONGODB ENDED...")
    print("\nCRAWLING ENDED...")
    return products


def infinite_rolling(START_PAGE):
    print("FINDING NUMBER OF SECTIONS...")
    r = requests.get(START_PAGE, headers=agent)
    soup = BeautifulSoup(r.content, "lxml")

    page_result = soup.find('div', attrs={"class": "dscrptn"})
    page_result = page_result.get_text()
    pos1 = page_result.find("için")
    pos2 = page_result.find("sonuç")
    product_num = int(page_result[pos1+5: pos2-1])

    if product_num % 24 == 0:
        pi = product_num // 24
    else:
        pi = (product_num // 24) + 1

    print("Number of sections...\n", pi)
    return pi

def main():
    START_PAGE = "https://www.trendyol.com/laptop"
    pi = infinite_rolling(START_PAGE)
    product = crawl(START_PAGE, pi)
    product_df = write_to_file(product)
    item_vectors = create_vectors(product_df)
    product_df['item_vectors'] = item_vectors
    print("Dataframe after vectors\n", product_df)
    index_to_elasticsearch(product_df)

if __name__ == "__main__":
    main()