import psycopg2
import utils
import pickle
import os
import sys
import time
import plotting as pt
import datetime as dt
import threading
import numpy as np

XS = "1m_xs"
S = "1m_s"
M = "1m_m"
L = "1m_l"

XSd = "1m_xsd"
Sd = "1m_sd"
Md = "1m_md"
Ld = "1m_ld"

final_times = []
ITERATION_LIMIT = 10
all_data = {}

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

def complex_join_times(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT ab.name from artist_info ab, artist_identity ai WHERE ai.mbid = ab.mbid AND ai.id = ab.id AND ab.familiarity > 0 ORDER BY ab.familiarity DESC limit 50"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT tri.title from track_basic_info tri, track_identity ti WHERE tri.track_id = ti.track_id AND tri.song_id = ti.song_id AND tri.duration > 0 ORDER BY tri.duration ASC limit 50"
	times.append(get_avg_query_time(query, cur))

	return times
	pass

def basic_joins(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT ab.name from artist_info ab, artist_identity ai WHERE ai.mbid = ab.mbid AND ai.id = ab.id limit 50"
	times.append(get_avg_query_time(query, cur))

	query = "SELECT tri.title from track_basic_info tri, track_identity ti WHERE tri.track_id = ti.track_id AND tri.song_id = ti.song_id limit 50"
	times.append(get_avg_query_time(query, cur))

	return times
	pass

def max_queries(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT MAX(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def min_queries(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT MIN(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def avg_queries(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT AVG(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def sum_queries(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT SUM(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def count_queries(conn):

	cur = conn.cursor()
	times = []

	query = "SELECT COUNT(ai.familiarity) from artist_info ai where ai.familiarity != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def delete_queries(conn):

	cur = conn.cursor()
	times = []

	query = "DELETE FROM artist_identity WHERE mbid IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "DELETE FROM artist_info WHERE name IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "DELETE FROM track_identity WHERE track_7digitalid IS NULL"
	times.append(get_avg_query_time(query, cur))

	query = "DELETE FROM track_basic_info WHERE title IS NULL"
	times.append(get_avg_query_time(query, cur))
	
	query = "DELETE FROM track_tech_info WHERE song_hotttnesss='NaN'"
	times.append(get_avg_query_time(query, cur))

	query = "DELETE FROM sang WHERE mbid IS NULL"
	times.append(get_avg_query_time(query, cur))

	return sum(times) / float(len(times))
	pass

def run_all_exp(conn, ID):
	
	res = []
	#res = select_times(conn)
	#res.append(update_times(conn))
	#res = complex_join_times(conn)
	#res = basic_joins(conn)
	#res = max_queries(conn)
	#res = min_queries(conn)
	#res = avg_queries(conn)
	#res = sum_queries(conn)
	res = count_queries(conn)
	#res = delete_queries(conn)

	final_times.append(res)

def create_threads_and_run(dbname, NUM_THREADS):
	
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			
			threads = []	
			#Create and start all threads
			for i in range(0, NUM_THREADS):
				process = threading.Thread(target=run_all_exp, args=(conn, i+1))
				process.start()
				threads.append(process)

			#Compile results together
			for process in threads:
				process.join()

			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	#print final_times

def build_all_data(db, NUM_THREADS):

	x = np.array(final_times)
	x = np.average(x, axis=0)

	if(db in all_data.keys()):
		all_data[db][NUM_THREADS] = x

	else:
		all_data[db] = {}
		all_data[db][NUM_THREADS] = x

	#print all_data

def test():
	for NUM_THREADS in [2, 4, 6, 8, 10, 12, 14]:
		
		print "Threads:", NUM_THREADS
		
		final_times = []
		create_threads_and_run(XSd, NUM_THREADS)
		build_all_data('XS', NUM_THREADS)
		
		final_times = []
		create_threads_and_run(Sd, NUM_THREADS)
		build_all_data('S', NUM_THREADS)
		
		final_times = []
		create_threads_and_run(Md, NUM_THREADS)
		build_all_data('M', NUM_THREADS)
		
		final_times = []
		create_threads_and_run(Ld, NUM_THREADS)
		build_all_data('L', NUM_THREADS)

	print all_data

if __name__ == '__main__':
	test()