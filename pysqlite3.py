################################################
#
#  This tiny program opens a unique sqlite3 db 
#  and dumps it in to a MongoDB
#
################################################

import sqlite3
import pymongo
import datetime
from pymongo import MongoClient

# Creates or opens a file with a SQLite3 DB
try:
    dbsqlite = sqlite3.connect('../chromewaves.db')
except Exception as e:
    raise ee

# Get a cursor object
sqlcursor = dbsqlite.cursor()

#
# Opens a MongoDB connection, creates post and mp3 collections if they don't exist
#

# establish a connection to the database
try:
    dbmongo = MongoClient("localhost",27017)
except Exception as e:
    raise e

testdb = dbmongo.test

# open or create posts and mp3 collections
try:
    testdb.drop_collection("posts")
    testdb.create_collection("posts")
except Exception as e:
    print e
    
try:
    testdb.drop_collection("media")
    testdb.create_collection("media")
except Exception as e:
    print e
    
# 
# read in all from DB, one post at a time (incl. media) and store it in the new format in MongoDB
#
try:
    query = "SELECT pkey, ptitle, plink, pdesc, pcontent, ppubdate, pnumcomments FROM posts;"       
    sqlcursor.execute(query)
    
#  pattern is:
#    insert posts, minus media array
#    create a postkey to post._id lookup dictionary
#    update posts with unique tag (from cat table, postcatjoin)
#    insert mp3 with post._id reference
#    update posts._id media with each unique mp3 which has matching post._id
#    
    postkey_lookup = {}        
    posts = sqlcursor.fetchall()
    for post in posts:
        try:
            #insert post, careful of ppubdate type
            #create dictionary doc
            doc = {"postkey": post[0], "title": post[1], "url": post[2], "description": post[3], \
                "content": post[4], "date": datetime.datetime.fromtimestamp(post[5]), "numcomments": post[6]}    
            #print doc
            post_id = testdb.posts.insert(doc)
            #update postkey lookup dictionary
            postkey_lookup[post[0]] = post_id 
        except Exception as e:
            print e;
            
    print "=== retrieved and stored posts ==="
            
    #
    # read and update post categories
    #
    query = "SELECT postkey, ckey, ctitle FROM postcatjoin JOIN cat ON postcatjoin.catkey = cat.ckey;"
    sqlcursor.execute(query) 
    rows = sqlcursor.fetchall()

    for row in rows:      
        try:
            if row[0] in post_id_to_postkey.keys():
                testdb.posts.update({"_id": postkey_lookup[row[0]]}, { "$addToSet": {"tags": row[2]}})
            else:
                print "Couldn't find the key!"
        except Exception as e:
            print e


    print "=== retrieved and stored tags in posts; we now have a post/tag consistent set ==="
    
    #
    # find and store media
    #
    query = "SELECT mp3link, mp3length, mp3artist, mp3songtitle, postkey FROM mp3"
    sqlcursor.execute(query) 
    mp3s = sqlcursor.fetchall()	
    for mp3 in mp3s:
        try:
            doc = {"url": mp3[0], "length": mp3[1], "artist": mp3[2], "title": mp3[3], "appears_in": postkey_lookup[mp3[4]], "type": "mp3"}    
            mp3_id = testdb.media.insert(doc)
			#update posts with media link
            testdb.posts.update({"_id": postkey_lookup[mp3[4]]},{"$addToSet":{"media": mp3_id}})	
        except Exception as e:
            print e;    
            
    print "=== retrieved and stored mp3s, updated link back to posts, updated posts with links to media ==="
    print "=== document oriented DB construction complete! ==="
    
        
except Exception as e:
    print e       