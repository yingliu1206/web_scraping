#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo


if __name__ == '__main__':
    client = pymongo.MongoClient('mongodb://localhost:27000',
                                 username= 'admin',
                                 password='mdipass'
                                ) 
    db = client["schoolSpider"]
    
    # collection names in the database "schoolSpider"
    col = 'text'
    
    collection = db[col]
    cursor = collection.find({},{"url":1})
    print(type(cursor))
    print(len(cursor))
