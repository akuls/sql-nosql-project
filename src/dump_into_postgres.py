#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2
import utils
import pickle
import os
import sys
import time
import plotting as pt
import datetime as dt

CREATE_TABLES = ".\static\create_all_tables.txt"

XS_DATA = "..\data\\xs_song_data\\"
S_DATA = "..\data\s_song_data\\"
M_DATA = "..\data\m_song_data\\"
L_DATA = "..\data\l_song_data\\"

XS = "1m_xs"
S = "1m_s"
M = "1m_m"
L = "1m_l"

def run(cur, query):

	try:
		cur.execute(query)
		#print "Success on: ", query
	except:
		print "Failed on: ", query
		cur.close()
		sys.exit()
	pass

def build_tables(conn):

	cur = conn.cursor()

	with open(CREATE_TABLES, 'r') as f:
		content = f.readlines()
	content = [x.strip() for x in content]

	for command in content:
		try:
			table_beg = dt.datetime.now()
			cur.execute(command)
			table_end = dt.datetime.now()

			print "%s took %fmilliseconds " %(command, (table_end-table_beg).microseconds/1000.0 )
		except:
			print "Failed on: ", command
			cur.close()

	cur.close()
	pass

def fix_format(song):

	new_song = {}
	for key in song.keys():
		#print key, song[key]
		new_song[key] = "'" + song[key].replace('\'', '') + "'"
	return new_song
	pass

def time_queries(data, cur):

	one = 0.0
	one_c = 0
	one_beg = 0.0
	one_end = 0.0

	ten = 0.0 
	ten_c = 0
	ten_beg = 0.0

	hun = 0.0
	hun_c = 0
	hun_beg = 0.0

	thou = 0.0
	thou_c = 0
	thou_beg = 0.0

	ten_beg = hun_beg = thou_beg = dt.datetime.now()

	for query in data:
		
		one_beg = dt.datetime.now()

		run(cur, query)
		
		one_end = dt.datetime.now()
		
		one += (one_end - one_beg).microseconds
		one_c += 1
		

		if(one_c%10 == 0):
			ten += (one_end - ten_beg).microseconds
			ten_c += 1
			ten_beg = dt.datetime.now()

		if(one_c%100 == 0):
			hun += (one_end - hun_beg).microseconds
			hun_c += 1
			hun_beg = dt.datetime.now()

		if(one_c%1000 == 0):
			thou += (one_end - thou_beg).microseconds
			thou_c += 1
			thou_beg = dt.datetime.now()

	times = []
	times.append(float(one/float(one_c)))
	times.append(float(ten/float(ten_c)))
	times.append(float(hun/float(hun_c)))
	times.append(float(thou/float(thou_c)))
	times = [float(x/1000.0) for x in times]

	return times
	pass

def insert_into_all_tables(conn, artist_dict, track_id, track_basic, track_tech, sang, data_path):

	all_times = []
	print "Populating tables now"
	cur = conn.cursor()
	print "Average insertion time in milli seconds"

	'''
	Artist identity table and artist info needs to have the same unique number of artists/IDs. Hence, derive the basic info using the artist_dict
	'''
	one = 0.0
	one_c = 0
	one_beg = 0.0
	one_end = 0.0

	ten = 0.0 
	ten_c = 0
	ten_beg = 0.0

	hun = 0.0
	hun_c = 0
	hun_beg = 0.0

	thou = 0.0
	thou_c = 0
	thou_beg = 0.0

	one1 = 0.0
	one_beg1 = 0.0
	one_end1 = 0.0
	ten1 = 0.0
	ten_beg1 = 0.0
	hun1 = 0.0
	hun_beg1 = 0.0
	thou1 = 0.0
	thou_beg1 = 0.0

	ten_beg = hun_beg = thou_beg = dt.datetime.now()

	for key in artist_dict.keys():
		
		#Artist identity table
		one_beg = dt.datetime.now()

		run(cur, key)

		one_end = dt.datetime.now()

		one += (one_end - one_beg).microseconds
		one_c += 1
		
		if(one_c%10 == 0):
			ten += (one_end - ten_beg).microseconds
			ten_c += 1
			ten_beg = dt.datetime.now()

		if(one_c%100 == 0):
			hun += (one_end - hun_beg).microseconds
			hun_c += 1
			hun_beg = dt.datetime.now()

		if(one_c%1000 == 0):
			thou += (one_end - thou_beg).microseconds
			thou_c += 1
			thou_beg = dt.datetime.now()

	artist_id = []
	artist_id.append(float(one/float(one_c)))
	artist_id.append(float(ten/float(ten_c)))
	artist_id.append(float(hun/float(hun_c)))
	artist_id.append(float(thou/float(thou_c)))
	artist_id = [float(x/1000.0) for x in artist_id]

	one_c = 0
	ten_beg1 = hun_beg1 = thou_beg1 = dt.datetime.now()
	for key in artist_dict.keys():

		#Artist basic info table
		song = pickle.load(open(data_path+artist_dict[key], "r"))
		song = fix_format(song)

		query = "INSERT into artist_info VALUES(" + song['artist_id'] + "," + song['artist_mbid'] + "," + song['artist_name'] + "," + \
			song['artist_familiarity'] + "," + song['artist_hotttnesss'] + "," + song['artist_latitude'] + "," + song['artist_location'] + "," + song['artist_longitude'] + ")"
		
		one_beg1 = dt.datetime.now()
		run(cur, query)
		one_end1 = dt.datetime.now()

		one1 += (one_end1 - one_beg1).microseconds
		one_c += 1

		if(one_c%10 == 0):
			ten1 += (one_end1 - ten_beg1).microseconds
			ten_beg1 = dt.datetime.now()

		if(one_c%100 == 0):
			hun1 += (one_end1 - hun_beg1).microseconds
			hun_beg1 = dt.datetime.now()

		if(one_c%1000 == 0):
			thou1 += (one_end1 - thou_beg1).microseconds
			thou_beg1 = dt.datetime.now()

	artist_bas = []
	artist_bas.append(float(one1/float(one_c)))
	artist_bas.append(float(ten1/float(ten_c)))
	artist_bas.append(float(hun1/float(hun_c)))
	artist_bas.append(float(thou1/float(thou_c)))
	artist_bas = [float(x/1000.0) for x in artist_bas]

	print 'All artist data populated!'

	all_times.append(artist_id)
	all_times.append(artist_bas)
	print all_times
	'''
	Traverse the rest of the sets and insert into respective tables
	'''
	track_iden = time_queries(track_id, cur)
	track_bas = time_queries(track_basic, cur)
	track_tech = time_queries(track_tech, cur)
	all_times.append(track_bas)
	all_times.append(track_iden)
	all_times.append(track_tech)

	print 'All track data populated!'
	sang_times = time_queries(sang, cur)
	all_times.append(sang_times)

	print 'All sang data populated!'

	print all_times
	cur.close()
	pass

def populate_data(conn, data_path):

	'''
	Tables and their fields
	
	artist_identity(id text, playmeid int, digi_7id int, mbid text)
	
	artist_info(id text, mbid text, name text, familiarity float, hottness float, latitude float, location text, longitude float)
	
	track_identity(track_id text, song_id text, audio_md5 text, release_7digitalid int, track_7digitalid int)
	
	track_basic_info(track_id text, song_id text, release text, title text, duration float, year int)
	
	track_tech_info(track_id text, song_id text, song_hotttnesss float, danceability float, start_of_fade_out float, end_of_fade_in float, energy float, key int, \
	key_confidence float, loudness float, mode int, mode_confidence float, tempo float, time_signature int, time_signature_confidence float, analysis_sample_rate float)
	
	sang(id text, mbid	text, track_id text, song_id text)
	'''
	artist_dict = {}
	track_identity_set = set()
	track_basic_info_set = set()
	track_tech_info_set = set()
	sang_set = set()

	song_list = os.listdir(data_path)

	for song_name in song_list:
		#print song_name
		song = pickle.load(open(data_path+song_name, "r"))

		song = fix_format(song)

		#Artist identity
		artist_dict["INSERT into artist_identity VALUES(" + song['artist_id']+ "," + song['artist_playmeid'] + "," + song['artist_7digitalid'] + "," + song['artist_mbid'] + ")"] = song_name
		
		#Track identity
		track_identity_set.add("INSERT into track_identity VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['audio_md5'] + "," + song['release_7digitalid'] + "," + song['track_7digitalid'] + ")")
		
		#Track basic info
		track_basic_info_set.add("INSERT into track_basic_info VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['release'] + "," + song['title'] + "," + song['duration'] + "," + song['year'] + ")")

		#Track technical info
		track_tech_info_set.add("INSERT into track_tech_info VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['song_hotttnesss'] + "," + song['danceability'] + "," + song['start_of_fade_out'] + "," + song['end_of_fade_in'] + \
			"," + song['energy'] + "," + song['key'] + "," + song['key_confidence'] + "," + song['loudness'] + "," + song['mode'] + "," + song['mode_confidence'] + "," + song['tempo'] + "," + song['time_signature'] + \
			"," + song['time_signature_confidence'] + "," + song['analysis_sample_rate'] + ")")

		#Sang
		sang_set.add("INSERT into sang VALUES(" + song['artist_id']+ "," + song['artist_mbid'] + "," + song['track_id'] + "," + song['song_id'] + ")")

	insert_into_all_tables(conn, artist_dict, track_identity_set, track_basic_info_set, track_tech_info_set, sang_set, data_path)
	pass

def plot_basic_charts():

	create_table = ['32.84', '34.66', '29.73', '31.65', '32.99', '31.27']
	pt.my_plot(create_table, "Creation time", "Milliseconds", "CREATE query timings", "CREATE")


	insert = ['590', '630', '560', '640', '670', '660']
	pt.my_plot(create_table, "Insertion time", "Microseconds", "INSERT query timings", "INSERT")

	pass

def build_db(dbname, PATH):
	
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"

			build_tables(conn)
			print 'All tables created!'

			populate_data(conn, PATH)
			print 'All tables populated!'
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

def build_tables_and_populate_data():

	#build_db(XS, XS_DATA)
	#build_db(S, S_DATA)
	#build_db(M, M_DATA)
	build_db(L, L_DATA)
	# build_db(TEMP_DB, L_DATA)

if __name__ == '__main__':
	build_tables_and_populate_data()
	#plot_basic_charts()