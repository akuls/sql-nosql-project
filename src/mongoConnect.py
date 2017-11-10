from pymongo import MongoClient
import pprint
import os
import cPickle as p
import datetime as dt

xs_data = '../batch/xs_song_data/'
s_data = '../batch/s_song_data/'
m_data = '../batch/m_song_data/'
l_data = '../batch/l_song_data/'

xs_db = 'OneMillionXS'
s_db = 'OneMillionS'
m_db = 'OneMillionM'
l_db = 'OneMillionL'

def connect_to_DB(db_name): 
    client  =  MongoClient() 
    database = client[db_name]
    return client, database

def populate_mongo_schemas():

    populate_times = dict()
    
    times_xs = create_and_populate_collection(xs_data,xs_db)
    times_s = create_and_populate_collection(s_data,s_db)
    times_m = create_and_populate_collection(m_data,m_db)
    times_l = create_and_populate_collection(l_data,l_db)
    
    populate_times['1'] = float(sum(times_l))/10000
    
    
    time_10_docs = 0
    start_10 = 0
    end_10 = 10
    while end_10 <= len(times_l):
        time_10_docs +=sum(times_l[start_10:end_10])
        start_10+=10
        end_10+=10
    populate_times['10'] = float(time_10_docs)/1000
    
    time_100_docs = 0
    start_100 = 0
    end_100 = 100
    while end_100 <= len(times_l):
        time_100_docs +=sum(times_l[start_100:end_100])
        start_100+=100
        end_100+=100
    populate_times['100'] = float(time_100_docs)/100
   
    time_1000_docs = 0
    start_1000 = 0
    end_1000 = 1000
    while end_1000 <= len(times_l):
        time_1000_docs +=sum(times_l[start_1000:end_1000])
        start_1000+=1000
        end_1000+=1000
    populate_times['1000'] = float(time_1000_docs)/10

    populate_times['10000'] = sum(times_l)
    
    return populate_times
    


def create_and_populate_collection(schema_path,db_name):
    
    client, database = connect_to_DB(db_name)
    time_for_each_document = []
    listOfDirs = os.listdir(schema_path)
    for pickleFile in listOfDirs:
        if '.p' in pickleFile:
            masterDict = p.load(open(schema_path+pickleFile, 'rb'))
            
            #Dictionary of artistIdInfo
            artistIdInfo  = dict([('artist_id',masterDict['artist_id']),
                            ('artist_playmeid',float(masterDict['artist_playmeid'])),
                            ('artist_7digitalid',float(masterDict['artist_7digitalid'])),
                            ('artist_mbid',masterDict['artist_mbid'])])
            #print artistIdInfo

            #Dictionary of artistBasicInfo
            artistBasicInfo  = dict([('artist_name',masterDict['artist_name']),
                            ('artist_familiarity',float(masterDict['artist_familiarity'])),
                            ('artist_hotttnesss',float(masterDict['artist_hotttnesss'])),
                            ('artist_mbid',masterDict['artist_mbid']),
                            ('artist_id',masterDict['artist_id']),
                            ('artist_latitude',float(masterDict['artist_latitude'])),
                            ('artist_longitude',float(masterDict['artist_longitude'])),
                            ('artist_loction',masterDict['artist_location'])
                            ])
            #Dictionary of TrackID
            trackIdInfo  = dict([('track_id',masterDict['track_id']),
                            ('song_id',masterDict['song_id']),
                            ('audio_md5',masterDict['audio_md5']),
                            ('release_7digitalid',float(masterDict['release_7digitalid'])),
                            ('track_7digitalid',float(masterDict['track_7digitalid']))
                            ])
            
            #Dictionary of TrackBasicInfo
            trackBasicInfo  = dict([('track_id',masterDict['track_id']),
                            ('song_id',masterDict['song_id']),
                            ('release',masterDict['release']),
                            ('title',masterDict['title']),
                            ('duration',float(masterDict['duration'])),
                            ('year',float(masterDict['year']))
                            ])
            
            #Dictionary of TrackTechInfo
            trackTechInfo  = dict([('track_id',masterDict['track_id']),
                            ('song_id',masterDict['song_id']),
                            ('song_hotttnesss',float(masterDict['song_hotttnesss'])),
                            ('danceability',float(masterDict['danceability'])),
                            ('start_of_fade_out',float(masterDict['start_of_fade_out'])),
                            ('end_of_fade_in',float(masterDict['end_of_fade_in'])),
                            ('energy',float(masterDict['energy'])),
                            ('key',float(masterDict['key'])),
                            ('key_confidence',float(masterDict['key_confidence'])),
                            ('loudness',float(masterDict['loudness'])),
                            ('mode',float(masterDict['mode_confidence'])),
                            ('tempo',float(masterDict['tempo'])),
                            ('time_signature',float(masterDict['time_signature'])),
                            ('time_signature_confidence',float(masterDict['time_signature_confidence'])),
                            ('analysis_sample_rate',float(masterDict['analysis_sample_rate']))
                            ])
            
            #Make the final document to be inserted into the collection
            documentToInsert = dict([('artistIdInfo',artistIdInfo),
                             ('artistBasicInfo',artistBasicInfo),
                             ('trackIdInfo',trackIdInfo),
                             ('trackBasicInfo',trackBasicInfo),
                             ('trackTechInfo',trackTechInfo)
                            ])
                 
            #Insert into the collection
            start = dt.datetime.now()
            result = database.SongCollection.insert_one(documentToInsert)
            end = dt.datetime.now()
            time_for_each_document.append((end-start).microseconds/1000.0)

    return time_for_each_document

#print "Time taken to populate schemas"
#populate_times = populate_mongo_schemas()

#print populate_times
