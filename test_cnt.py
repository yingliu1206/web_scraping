#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
import pandas as pd
from urllib.parse import urlparse

if __name__ == '__main__':
    
    # read the charter school as a data frame
    charter2019_df = pd.read_csv('./scrapy/schools/schools/spiders/charter_school_URLs_2019.tsv', sep='\t')
    
    # get a list of domains from charter school urls
    domain_2019 = []
    for i in charter2019_df['URL_2019']:
        domain_origin = urlparse(i).netloc
        domain_2019.append(domain_origin)
    
    # connect to mongodb and extract scraped urls
    client = pymongo.MongoClient('mongodb://localhost:27000',
                                 username= 'admin',
                                 password='mdipass'
                                ) 
    db = client["schoolSpider"]
    col = 'text'
    
    collection = db[col]
    cursor = collection.find({},{"url":1})
    
    # get a list of domains from scraped urls
    db_url_domain = []
    for ducument in cursor:
        domain = urlparse(ducument['url']).netloc
        db_url_domain.append(domain)
    
    # check what urls have not been scrpaed
    remaining_domain = list(set(domain_2019) - set(db_url_domain))
    print(len(list(set(domain_2019))))
    print(len(remaining_domain))
