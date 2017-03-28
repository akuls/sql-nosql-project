from pymongo import MongoClient
import pprint
import os
import cPickle as p
def connectToDB():
    global client 
    client  =  MongoClient()
    global database 
    database = client.OneMSongsSmall


def createAndPopulateCollection():
    
    listOfDirs = os.listdir('../song_data')
    for pickleFile in listOfDirs:
        if '.p' in pickleFile:
            masterDict = p.load(open('../song_data/'+pickleFile, 'rb'))
            
            #Dictionary of artistIdInfo
            artistIdInfo  = dict([('artist_id',masterDict['artist_id']),
                            ('artist_playmeid',float(masterDict['artist_playmeid'])),
                            ('artist_7digitalid',float(masterDict['artist_7digitalid'])),
                            ('artist_mbid',masterDict['artist_mbid'])])
            #print artistIdInfo

            #Dictionary of artistBasicInfo
            artistBasicInfo  = dict([('artist_name',masterDict['artist_name']),
                            ('artist_familiarity',masterDict['artist_familiarity']),
                            ('artist_hotttnesss',masterDict['artist_hotttnesss']),
                            ('artist_mbid',masterDict['artist_mbid']),
                            ('artist_id',masterDict['artist_id']),
                            ('artist_latitude',masterDict['artist_latitude']),
                            ('artist_longitude',masterDict['artist_longitude']),
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
                            ('duration',masterDict['duration']),
                            ('year',float(masterDict['year']))
                            ])
            
            #Dictionary of TrackTechInfo
            trackTechInfo  = dict([('track_id',masterDict['track_id']),
                            ('song_id',masterDict['song_id']),
                            ('song_hotttnesss',masterDict['song_hotttnesss']),
                            ('danceability',masterDict['danceability']),
                            ('start_of_fade_out',masterDict['start_of_fade_out']),
                            ('end_of_fade_in',masterDict['end_of_fade_in']),
                            ('energy',masterDict['energy']),
                            ('key',float(masterDict['key'])),
                            ('key_confidence',masterDict['key_confidence']),
                            ('loudness',masterDict['loudness']),
                            ('mode',float(masterDict['mode_confidence'])),
                            ('tempo',masterDict['tempo']),
                            ('time_signature',float(masterDict['time_signature'])),
                            ('time_signature_confidence',masterDict['time_signature_confidence']),
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
            result = database.SongCollection.insert_one(documentToInsert)

connectToDB()
createAndPopulateCollection()
