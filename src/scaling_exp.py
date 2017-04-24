import psycopg2
import utils
import pickle
import os
import sys
import time
import plotting as pt
import datetime as dt

XS = "1m_xs"
S = "1m_s"
M = "1m_m"
L = "1m_l"

ITERATION_LIMIT = 10

def get_avg_query_time(query, cur):
	
	time_elapsed = 0.0

	for i in range(0, ITERATION_LIMIT):
		beg = dt.datetime.now()
		cur.execute(query)
		end = dt.datetime.now()
		time_elapsed += (end-beg).microseconds

	return float((time_elapsed/float(ITERATION_LIMIT))/1000.0)

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

	return sum(times) / float(len(times))
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

	return sum(times) / float(len(times))

def select_times(conn):

	times = []

	times.append(basic_select(conn))
	times.append(select_with_condition(conn))

	return times
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

	return sum(times) / float(len(times))
	pass

def delete_times(conn):
	pass

def basic_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = select_times(conn)
			res.append(update_times(conn))
			#delete_times(conn)
			
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_data_basic_queries():

	xs_times = basic_queries(XS)
	s_times = basic_queries(S)
	m_times = basic_queries(M)
	l_times = basic_queries(L)

	print xs_times, s_times, m_times, l_times

def join_times(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT ab.name from artist_info ab, artist_identity ai WHERE ai.mbid = ab.mbid AND ai.id = ab.id AND ab.familiarity > 0 ORDER BY ab.familiarity DESC limit 50"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT tri.title from track_basic_info tri, track_identity ti WHERE tri.track_id = ti.track_id AND tri.song_id = ti.song_id AND tri.duration > 0 ORDER BY tri.duration ASC limit 50"
	times.append(get_avg_query_time(query, cur))

	return times
	pass

def join_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = join_times(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res


def scale_data_join_queries():

	xs_times = join_queries(XS)
	s_times = join_queries(S)
	m_times = join_queries(M)
	l_times = join_queries(L)

	print xs_times, s_times, m_times, l_times

if __name__ == '__main__':
	#scale_data_basic_queries()
	scale_data_join_queries()