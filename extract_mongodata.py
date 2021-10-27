#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
import gridfs
from bson import ObjectId 
from io import BytesIO 
from PIL import Image


if __name__ == '__main__':
    client = pymongo.MongoClient('mongodb://localhost:27017',
                                 username= 'admin',
                                 password='mdipass'
                                ) 
    db = client["schoolSpider"]
    
    # collection names in the database "schoolSpider"
    cols =db.list_collection_names()  
    
    # for text data
    for coll in cols:
        if coll == 'text':
            print("collection name is : ", coll)
            collection = db[coll]
            cursor = collection.find({})
            for document in cursor:
                print(document)
    
    # for images   
        elif coll == 'images.files' or coll == 'images.chunks':
            print("collection name is : ", coll)
            
            # retrieve data from gridfs
            fs = gridfs.GridFS(db, "images") 
                       
           # extract valid image files
            filenames = [filename for filename in db.images.files.find({},{'filename':1})]
            filenames = [y['filename'] for y in filenames]
            
            # a list for jpg/jpeg and png files respectively
            images_jpg = [x for x in filenames if x.endswith(('jpeg', 'jpg'))]
            images_png = [x for x in filenames if x.endswith(('png'))]
            
            # for jpg: get ids of valid image files accrodingly
            images_ids = []
            for i in range(len(images_jpg)):
                images_ids.append([str(id) for id in fs.find({'filename': images_jpg[i]}).distinct('_id')])
            images_ids = [item for sublist in images_ids for item in sublist]
            
            # remove duplicates
            images_ids = list(set(images_ids)) 
            
            # decode binary data of images
            for i in range(len(images_ids)):
                    im = Image.open(BytesIO(fs.get(ObjectId(images_ids[i])).read()))
                    im.save('%s.jpg' % (i+1))     
                    #im.show()
            
            # for png: get ids of valid image files accrodingly
            images_ids2 = []
            for i in range(len(images_png)):
                images_ids2.append([str(id) for id in fs.find({'filename': images_png[i]}).distinct('_id')])
            images_ids2 = [item for sublist in images_ids2 for item in sublist]
            
            # remove duplicates
            images_ids2 = list(set(images_ids2)) 
            
            # decode binary data of images
            for i in range(len(images_ids2)):
                    im = Image.open(BytesIO(fs.get(ObjectId(images_ids2[i])).read()))
                    im.convert('RGB')
                    im.save('%s.png' % (i+1+len(images_ids)))     
                    #im.show()
            
    # for files   
        elif coll == 'files.files' or coll == 'files.chunks':
            print("collection name is : ", coll)
            
            # retrieve data from gridfs
            fs = gridfs.GridFS(db, "files") 
        
            # extract valid pdf/docx files
            filenames = [filename for filename in db.files.files.find({},{'filename':1})]
            filenames = [y['filename'] for y in filenames]
            files_pdf = [x for x in filenames if x.endswith(('pdf'))]
            files_docx = [x for x in filenames if x.endswith(('docx'))]
            
            # for pdf: get ids of valid files accrodingly
            files_ids = []
            for i in range(len(files_pdf)):
                files_ids.append([str(id) for id in fs.find({'filename': files_pdf[i]}).distinct('_id')])
            files_ids = [item for sublist in files_ids for item in sublist]
            
            # remove duplicates
            files_ids = list(set(files_ids)) 
            
            for i in range(len(files_ids)):
                    bytes = bytearray(fs.get(ObjectId(files_ids[i])).read())
                    f = open('%s.pdf' % (i+1), 'wb')
                    f.write(bytes)
                    f.close()
            
            # for docx: get ids of valid files accrodingly
            files_ids2 = []
            for i in range(len(files_docx)):
                files_ids2.append([str(id) for id in fs.find({'filename': files_docx[i]}).distinct('_id')])
            files_ids2 = [item for sublist in files_ids2 for item in sublist]
            
            # remove duplicates
            files_ids2 = list(set(files_ids2)) 
            
            for i in range(len(files_ids2)):
                    bytes = bytearray(fs.get(ObjectId(files_ids2[i])).read())
                    f = open('%s.docx' % (i+1+len(files_ids)), 'wb')
                    f.write(bytes)
                    f.close()            
         
        else: print("The collection doesn't exits.")

