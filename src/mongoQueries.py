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
    
    avg_select_time = (average_time_artistId+average_time_artistBasicInfo+average_time_trackBasicInfo+average_time_trackTechInfo+average_time_trackIdInfo)/5

    return avg_select_time

def conditional_selects(database):

    #All artists' ID info
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('artistIdInfo',{'$and':[{'artistIdnfo.artist_id':{'$exists': True,'$ne':None}},{'artistIdInfo.artist_mbid':{'$exists':True,'$ne': None}}]})
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    #print time_array
    average_time_artistId = sum(time_array)/ITER_COUNT
    print average_time_artistId
    
    #For artistBasicInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('artistBasicInfo',{'artistBasicInfo':{'artistBasicInfo.artist_familiarity':{'$gt':0.5}}})
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
        artist_id_info =  database.SongCollection.distinct('trackIdInfo',{'$and':[{'trackIdInfo.song_id':{'$ne':None}},{'trackIdInfo.track_id':{'$ne':None}}]})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackIdInfo = sum(time_array)/ITER_COUNT
    print average_time_trackIdInfo
    
    #For trackBasicInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('trackBasicInfo',{'trackBasicInfo.year':{'$gt':2000}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo

    
    #For trackIdInfo
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.distinct('trackTechInfo',{'$and':[{'trackTechInfo.song_hotttnesss':{'$gt':0}},{'trackTechInfo.song_hotttnesss':{'$ne':None}}]})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackTechInfo = sum(time_array)/ITER_COUNT
    print average_time_trackTechInfo
    
    avg_select_time = (average_time_artistId+average_time_artistBasicInfo+average_time_trackBasicInfo+average_time_trackTechInfo+average_time_trackIdInfo)/5
    
    return avg_select_time

def update(database):

    #All artists' ID info
    count =0
    time_array = []
    
    #Artist ID info update
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.update({'artistIdInfo.artist_mbid':{'$eq':None}},{'$set': {'artistIdInfo.artist_mbid':None}},False)
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistIdInfo = sum(time_array)/ITER_COUNT
    print average_time_artistIdInfo
    
    #Artist basic info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_basic_info =  database.SongCollection.update({'artistBasicInfo.artist_name':{'$eq':None}},{'$set': {'artistBasicInfo.artist_name':None}},False)
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_artistBasicInfo
    
    #Track ID info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        track_id_info =  database.SongCollection.update({'trackIdInfo.track7digitalid':{'$eq':None}},{'$set': {'trackIdInfo.track7digitalid':None}},False)
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackIdInfo = sum(time_array)/ITER_COUNT
    print average_time_trackIdInfo
    
    #Track basic info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        a_id_info =  database.SongCollection.update({'trackBasicInfo.title':{'$eq':None}},{'$set': {'trackBasicInfo.title':None}},False)
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo
    
    #Track techInfo update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.update({'trackTechInfo.song_hotttnesss':{'$eq':None}},{'$set': {'trackTechInfo.song_hotttnesss':None}},False)
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackTechInfo = sum(time_array)/ITER_COUNT
    print average_time_trackTechInfo
    
        
    avg_update_time = (average_time_artistIdInfo+average_time_artistBasicInfo+average_time_trackBasicInfo+average_time_trackTechInfo+average_time_trackIdInfo)/5

    return avg_update_time

def delete(database):

    #All artists' ID info
    count =0
    time_array = []
    
    #Artist ID info update
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.remove({'artistIdInfo.artist_mbid':{'$eq':None}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistIdInfo = sum(time_array)/ITER_COUNT
    print average_time_artistIdInfo
    
    #Artist basic info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_basic_info =  database.SongCollection.remove({'artistBasicInfo.artist_name':{'$eq':None}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_artistBasicInfo
    
    #Track ID info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        track_id_info =  database.SongCollection.remove({'trackIdInfo.track7digitalid':{'$eq':None}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackIdInfo = sum(time_array)/ITER_COUNT
    print average_time_trackIdInfo
    
    #Track basic info update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        a_id_info =  database.SongCollection.remove({'trackBasicInfo.title':{'$eq':None}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo
    
    #Track techInfo update
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_id_info =  database.SongCollection.remove({'trackTechInfo.song_hotttnesss':{'$eq':None}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackTechInfo = sum(time_array)/ITER_COUNT
    print average_time_trackTechInfo
    
        
    avg_update_time = (average_time_artistIdInfo+average_time_artistBasicInfo+average_time_trackBasicInfo+average_time_trackTechInfo+average_time_trackIdInfo)/5

    return avg_update_time

def basic_join(database): 
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_basic_info =  database.SongCollection.distinct('artistBasicInfo.artist_name')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_artistBasicInfo
    
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        track_basic_info =  database.SongCollection.distinct('trackBasicInfo.title')
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo
    
    #average_basic_join_time = (average_time_artistBasicInfo+average_time_trackBasicInfo)/2

    return [average_time_artistBasicInfo,average_time_trackBasicInfo]

def advanced_join(database): 
    
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_basic_info =  database.SongCollection.distinct('artistBasicInfo.artist_name',{'artistBasicInfo.artist_familiarity':{'$gt':0}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_artistBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_artistBasicInfo
    
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        #print artist_id_info
        track_basic_info =  database.SongCollection.distinct('trackBasicInfo.title',{'trackBasicInfo.duration':{'$gt':0}})
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_trackBasicInfo = sum(time_array)/ITER_COUNT
    print average_time_trackBasicInfo
    
    #average_basic_join_time = (average_time_artistBasicInfo+average_time_trackBasicInfo)/2

    return [average_time_artistBasicInfo,average_time_trackBasicInfo]


def aggregate_queries(database):    
    
    #Sum query
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        sum_aggregate_output = database.SongCollection.aggregate([{"$match": {"artistBasicInfo.artist_familiarity": { "$exists": True, "$ne": None }}},{ "$group": {"_id": "artistBasicInfo.artist_id","count": { "$sum":"$artistBasicInfo.artist_familiarity"}}}])
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_sum = sum(time_array)/ITER_COUNT
    print average_time_sum
    
    #Average query
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_aggregate_output = database.SongCollection.aggregate([{"$match": {"artistBasicInfo.artist_familiarity": { "$exists": True, "$ne": None }}},{ "$group": {"_id": "artistBasicInfo.artist_id","count": { "$avg":"$artistBasicInfo.artist_familiarity"}}}])
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_avg = sum(time_array)/ITER_COUNT
    print average_time_avg

    #Max query
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_aggregate_output = database.SongCollection.aggregate([{"$match": {"artistBasicInfo.artist_familiarity": { "$exists": True, "$ne": None }}},{ "$group": {"_id": "artistBasicInfo.artist_id","count": { "$max":"$artistBasicInfo.artist_familiarity"}}}])
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_max = sum(time_array)/ITER_COUNT
    print average_time_max
    
    #Min Query
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_aggregate_output = database.SongCollection.aggregate([{"$match": {"artistBasicInfo.artist_familiarity": { "$exists": True, "$ne": None }}},{ "$group": {"_id": "artistBasicInfo.artist_id","count": { "$min":"$artistBasicInfo.artist_familiarity"}}}])
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_min = sum(time_array)/ITER_COUNT
    print average_time_min
    
    #Count query
    count =0
    time_array = []
    while count < ITER_COUNT:
        start = dt.datetime.now()
        artist_aggregate_output = database.SongCollection.aggregate([{"$match": {"artistBasicInfo.artist_familiarity": { "$exists": True, "$ne": None }}},{ "$group": {"_id": "artistBasicInfo.artist_id","count":{'$sum':1}}}])
        #print artist_id_info
        end = dt.datetime.now()
        time_taken = end-start
        time_array.append((time_taken.microseconds)/1000.0)
        count+=1
    average_time_count = sum(time_array)/ITER_COUNT
    print average_time_count
    
    return average_time_sum,average_time_avg,average_time_min,average_time_max,average_time_count



    
def record_basic_times():
    
    select_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = basic_selects(database_xs)
    s_time = basic_selects(database_s)
    m_time = basic_selects(database_m)
    l_time = basic_selects(database_l)
    
    select_times['XS'] = xs_time
    select_times['S'] = s_time
    select_times['M'] = m_time
    select_times['L'] = l_time

    return select_times

def record_conditional_select_times(): 
    
    select_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = conditional_selects(database_xs)
    s_time = conditional_selects(database_s)
    m_time = conditional_selects(database_m)
    l_time = conditional_selects(database_l)
    
    select_times['XS'] = xs_time
    select_times['S'] = s_time
    select_times['M'] = m_time
    select_times['L'] = l_time
    
    return select_times

def record_update_times(): 
    
    update_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = update(database_xs)
    s_time = update(database_s)
    m_time = update(database_m)
    l_time = update(database_l)
    
    update_times['XS'] = xs_time
    update_times['S'] = s_time
    update_times['M'] = m_time
    update_times['L'] = l_time
    
    return update_times

    #Distinct artists





def record_delete_times(): 
    
    update_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = delete(database_xs)
    s_time = delete(database_s)
    m_time = delete(database_m)
    l_time = delete(database_l)
    
    update_times['XS'] = xs_time
    update_times['S'] = s_time
    update_times['M'] = m_time
    update_times['L'] = l_time
    
    return update_times

def record_basic_join_times():
    
    join_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = basic_join(database_xs)
    s_time = basic_join(database_s)
    m_time = basic_join(database_m)
    l_time = basic_join(database_l)
    
    join_times['XS'] = xs_time
    join_times['S'] = s_time
    join_times['M'] = m_time
    join_times['L'] = l_time

    return join_times

def record_advanced_join_times():
    
    join_times  = dict()
    client_xs,database_xs = mongoConnect.connect_to_DB(mongoConnect.xs_db)
    client_s,database_s = mongoConnect.connect_to_DB(mongoConnect.s_db)
    client_m,database_m = mongoConnect.connect_to_DB(mongoConnect.m_db)
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    xs_time = advanced_join(database_xs)
    s_time = advanced_join(database_s)
    m_time = advanced_join(database_m)
    l_time = advanced_join(database_l)
    
    join_times['XS'] = xs_time
    join_times['S'] = s_time
    join_times['M'] = m_time
    join_times['L'] = l_time

    return join_times

def record_aggregate_times():
    
    aggregate_times  = dict()
    client_l,database_l = mongoConnect.connect_to_DB(mongoConnect.l_db)

    sum_time,avg_time,min_time,max_time,count_time = aggregate_queries(database_l)
    #avg_time = advanced_join(database_s)
    #min_time = advanced_join(database_m)
    #max_time = advanced_join(database_l)
    
    aggregate_times['avg'] = avg_time
    aggregate_times['max'] = max_time
    aggregate_times['min'] = min_time
    aggregate_times['sum'] = sum_time
    aggregate_times['count'] = count_time

    return aggregate_times


if __name__ == '__main__':
    print record_conditional_select_times()
    print record_basic_times()
    print record_update_times()
    print record_delete_times()
    print record_basic_join_times()
    print record_advanced_join_times()
    print record_aggregate_times()
