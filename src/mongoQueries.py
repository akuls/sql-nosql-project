import mongoConnect
import pprint
import datetime as dt

ITER_COUNT =100

def basic_selects(database):

    #All artists' ID info
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('artistIdInfo')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistId = sum(time_array)/ITER_COUNT
    print average_time_artistId
    
    #For artistBasicInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('artistBasicInfo')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_artistBasicInfo 
    #For trackBasicInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('trackBasicInfo')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo
    
    #For trackTechInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('trackTechInfo')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackTechInfo = sum(time_array)/ITER_COUNT
    print average_time_trackTechInfo
    
    #For trackIdInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('artistIdInfo')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackIdInfo = sum(time_array)/ITER_COUNT
    print average_time_trackIdInfo

client,database = mongoConnect.connectToDB()
basic_selects(database)
