#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2
import utils
import pickle
import os
import sys

CREATE_TABLES = ".\static\create_all_tables.txt"
DATA = "..\data\song_data\\"

def run(cur, query):

	try:
		cur.execute(query)
		print "Success on: ", query
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
			cur.execute(command)
		except:
			print "Failed on: ", command
			cur.close()

	cur.close()
	pass

def fix_format(song):

	new_song = {}
	for key in song.keys():
		#print key, song[key]
		new_song[key] = "'" + song[key] + "'"
	return new_song
	pass

def insert_into_all_tables(conn, song):
	
	song = fix_format(song)
	cur = conn.cursor()

	#Artist identity
	query = "INSERT into artist_identity VALUES(" + song['artist_id']+ "," + song['artist_playmeid'] + "," + song['artist_7digitalid'] + "," + song['artist_mbid'] + ")"
	run(cur, query)

	#Artist info

	#Track identity

	#Track basic info

	#Track technical info

	#Sang


	cur.close()
	pass

def populate_data(conn):

	song_list = os.listdir(DATA)

	for each_song in song_list:
		print each_song
		song = pickle.load(open(DATA+each_song, "r"))
		insert_into_all_tables(conn, song)
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
			populate_data(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

if __name__ == '__main__':
	build_tables_and_populate_data()