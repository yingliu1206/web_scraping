#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
import pandas as pd

if __name__ == '__main__':
    
    # connect to mongodb and extract scraped urls
    client = pymongo.MongoClient('mongodb://localhost:27000',
                                 username= 'admin',
                                 password='mdipass'
                                ) 
    db = client["schoolSpider"]
    col = 'text'
    
    collection = db[col]
    cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    
    df.to_csv('./scrapy/schools/schools/text.csv')
