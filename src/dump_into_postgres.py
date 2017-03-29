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

CREATE_TABLES = ".\static\create_all_tables.txt"
DATA = "..\data\song_data\\"

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
			table_beg = time.clock()
			cur.execute(command)
			table_end = time.clock()

			print "%s took %fs " %(command, (table_end-table_beg) )
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

def insert_into_all_tables(conn, artist_dict, track_id, track_basic, track_tech, sang):

	print "Populating tables now"
	cur = conn.cursor()
	print "Average insertion time in seconds"

	'''
	Artist identity table and artist info needs to have the same unique number of artists/IDs. Hence, derive the basic info using the artist_dict
	'''
	artist_table_beg = time.clock()
	for key in artist_dict.keys():
		
		#Artist identity table
		run(cur, key)

		#Artist basic info table
		song = pickle.load(open(DATA+artist_dict[key], "r"))
		song = fix_format(song)

		query = "INSERT into artist_info VALUES(" + song['artist_id'] + "," + song['artist_mbid'] + "," + song['artist_name'] + "," + \
			song['artist_familiarity'] + "," + song['artist_hotttnesss'] + "," + song['artist_latitude'] + "," + song['artist_location'] + "," + song['artist_longitude'] + ")"
		run(cur, query)
	artist_table_end = time.clock()

	print 'All artist data populated!'
	print 'Artist data: ', ((artist_table_end - artist_table_beg)*1000.0)/( float(len(artist_dict.keys()))*2.0 )
	'''
	Traverse the rest of the sets and insert into respective tables
	'''
	track_identity_beg = time.clock()
	for query in track_id:
		run(cur, query)
	track_identity_end = time.clock()
	print 'Track identity data: ', ((track_identity_end - track_identity_beg)*1000.0)/float(len(track_id))

	track_basic_beg = time.clock()
	for query in track_basic:
		run(cur, query)
	track_basic_end = time.clock()
	print 'Track identity data: ', ((track_basic_end - track_basic_beg)*1000.0)/float(len(track_basic))

	track_tech_beg = time.clock()
	for query in track_tech:
		run(cur, query)
	track_tech_end = time.clock()
	print 'Track technical data: ', ((track_tech_end - track_tech_beg)*1000.0)/float(len(track_tech))

	print 'All track data populated!'

	sang_beg = time.clock()
	for query in sang:
		run(cur, query)
	sang_end = time.clock()
	print 'Artist data: ', ((sang_end - sang_beg)*1000.0)/float(len(sang))

	print 'All sang data populated!'
	cur.close()
	pass

def populate_data(conn):

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

	song_list = os.listdir(DATA)

	for song_name in song_list:
		#print song_name
		song = pickle.load(open(DATA+song_name, "r"))

		song = fix_format(song)
		print song
		break

	# 	#Artist identity
	# 	artist_dict["INSERT into artist_identity VALUES(" + song['artist_id']+ "," + song['artist_playmeid'] + "," + song['artist_7digitalid'] + "," + song['artist_mbid'] + ")"] = song_name
		
	# 	#Track identity
	# 	track_identity_set.add("INSERT into track_identity VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['audio_md5'] + "," + song['release_7digitalid'] + "," + song['track_7digitalid'] + ")")
		
	# 	#Track basic info
	# 	track_basic_info_set.add("INSERT into track_basic_info VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['release'] + "," + song['title'] + "," + song['duration'] + "," + song['year'] + ")")

	# 	#Track technical info
	# 	track_tech_info_set.add("INSERT into track_tech_info VALUES(" + song['track_id']+ "," + song['song_id'] + "," + song['song_hotttnesss'] + "," + song['danceability'] + "," + song['start_of_fade_out'] + "," + song['end_of_fade_in'] + \
	# 		"," + song['energy'] + "," + song['key'] + "," + song['key_confidence'] + "," + song['loudness'] + "," + song['mode'] + "," + song['mode_confidence'] + "," + song['tempo'] + "," + song['time_signature'] + \
	# 		"," + song['time_signature_confidence'] + "," + song['analysis_sample_rate'] + ")")

	# 	#Sang
	# 	sang_set.add("INSERT into sang VALUES(" + song['artist_id']+ "," + song['artist_mbid'] + "," + song['track_id'] + "," + song['song_id'] + ")")

	# insert_into_all_tables(conn, artist_dict, track_identity_set, track_basic_info_set, track_tech_info_set, sang_set)
	pass

def build_tables_and_populate_data():

	conn = None
	try:
		conn = utils.connect_to_db('1m_song', 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"

			#build_tables(conn)
			print 'All tables created!'

			populate_data(conn)
			print 'All tables populated!'
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

if __name__ == '__main__':
	build_tables_and_populate_data()