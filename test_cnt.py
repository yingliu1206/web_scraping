import pymongo
import pandas as pd

if __name__ == '__main__':
    charter2019_df = pd.read_csv('./scrapy/schools/schools/spiders/charter_school_URLs_2019.tsv', sep='\t')
    print(charter2019_df, 6)
    
    client = pymongo.MongoClient('mongodb://localhost:27000',
                                 username= 'admin',
                                 password='mdipass'
                                ) 
    db = client["schoolSpider"]
    
    # collection names in the database "schoolSpider"
    col = 'text'
    
    collection = db[col]
    cursor = collection.find({},{"url":1})
    
    url_list = []
    for ducument in cursor:
        url_list.append(ducument['url'])
    
    print(len(url_list))
