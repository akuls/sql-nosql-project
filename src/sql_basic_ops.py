import psycopg2
import utils
import pickle
import os
import sys
import time
import plotting as pt 

ITERATION_LIMIT = 10

def get_avg_query_time(query, cur):
	
	time_elapsed = 0.0

	for i in range(0, ITERATION_LIMIT):
		beg = time.clock()
		cur.execute(query)
		end = time.clock()
		time_elapsed += end-beg

	return (time_elapsed/float(ITERATION_LIMIT))*1000

def select_with_condition(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT * FROM artist_identity WHERE id IS NOT NULL AND mbid IS NOT NULL"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM artist_info WHERE familiarity>0.5"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_identity WHERE track_id IS NOT NULL AND song_id IS NOT NULL"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_basic_info WHERE year>2000"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_tech_info WHERE song_hotttnesss>0 AND song_hotttnesss != 'NaN'"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM sang where id IS NOT NULL AND mbid IS NOT NULL AND track_id IS NOT NULL AND song_id IS NOT NULL"
	times.append(get_avg_query_time(query, cur))

	return times
	pass

def basic_select(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT * FROM artist_identity"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM artist_info"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_identity"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_basic_info"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM track_tech_info"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT * FROM sang"
	times.append(get_avg_query_time(query, cur))

	return times

def select_times(conn):

	times = basic_select(conn)
	pt.my_plot(times, "TABLE NAME", "Time in ms", "Basic SELECT times", "basic_select.png")

	times = select_with_condition(conn)
	pt.my_plot(times, "TABLE NAME", "Time in ms", "Conditional SELECT times", "condition_select.png")

	pass

def update_times(conn):

	cur = conn.cursor()
	times = []

	query = "UPDATE artist_identity SET mbid=NULL WHERE mbid IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "UPDATE artist_info SET name=NULL WHERE name IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "UPDATE track_identity SET track_7digitalid=NULL WHERE track_7digitalid IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "UPDATE track_basic_info SET title=NULL WHERE title IS NULL"
	times.append(get_avg_query_time(query, cur))
	
	query = "UPDATE track_tech_info SET song_hotttnesss='NaN' WHERE song_hotttnesss='NaN'"
	times.append(get_avg_query_time(query, cur))

	query = "UPDATE sang SET mbid=NULL WHERE mbid IS NULL"
	times.append(get_avg_query_time(query, cur))

	pt.my_plot(times, "TABLE NAME", "Time in ms", "UPDATE times", "update.png")
	pass

def delete_times(conn):
	pass

def basic_queries():

	conn = None
	try:
		#conn = utils.connect_to_db('1m_song', 'postgres', 'localhost', 'akul')
		conn = utils.connect_to_db('1msongs_new', 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			select_times(conn)
			update_times(conn)
			#delete_times(conn)
			
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

if __name__ == '__main__':
	basic_queries()
