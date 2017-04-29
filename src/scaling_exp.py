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

XSd = "1m_xsd"
Sd = "1m_sd"
Md = "1m_md"
Ld = "1m_ld"

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

def join_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			#res = complex_join_times(conn)
			res = basic_joins(conn)
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

def run_max_queries(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT MAX(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def max_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = run_max_queries(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_max_queries():
	
	print 'Max Queries'
	xs_times = max_queries(XS)
	s_times = max_queries(S)
	m_times = max_queries(M)
	l_times = max_queries(L)

	print xs_times, s_times, m_times, l_times


def run_min_queries(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT MIN(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def min_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = run_min_queries(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_min_queries():
	
	print 'Min Queries'
	xs_times = min_queries(XS)
	s_times = min_queries(S)
	m_times = min_queries(M)
	l_times = min_queries(L)

	print xs_times, s_times, m_times, l_times

def run_avg_queries(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT AVG(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def avg_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = run_avg_queries(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_avg_queries():
	
	print 'Min Queries'
	xs_times = avg_queries(XS)
	s_times = avg_queries(S)
	m_times = avg_queries(M)
	l_times = avg_queries(L)

	print xs_times, s_times, m_times, l_times

def run_sum_queries(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT SUM(ai.familiarity + ai.hottness) from artist_info ai where ai.familiarity != 'NaN' AND ai.hottness != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def sum_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = run_sum_queries(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_sum_queries():
	
	print 'Sum Queries'
	xs_times = sum_queries(XS)
	s_times = sum_queries(S)
	m_times = sum_queries(M)
	l_times = sum_queries(L)

	print xs_times, s_times, m_times, l_times

def run_count_queries(conn):
	
	cur = conn.cursor()
	times = []

	query = "SELECT COUNT(ai.familiarity) from artist_info ai where ai.familiarity != 'NaN'"
	times.append(get_avg_query_time(query, cur))
	
	return times
	pass

def count_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = run_count_queries(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def scale_count_queries():
	
	print 'Count Queries'
	xs_times = count_queries(XS)
	s_times = count_queries(S)
	m_times = count_queries(M)
	l_times = count_queries(L)

	print xs_times, s_times, m_times, l_times

def scale_aggregate_queries():

	#scale_max_queries()
	#scale_min_queries()
	#scale_avg_queries()
	scale_sum_queries()
	scale_count_queries()

def delete_queries(dbname):

	res = []
	conn = None
	try:
		conn = utils.connect_to_db(dbname, 'postgres', 'localhost', 'akul')

		if(conn == None):
			print "Connection to database failed. Please try again!"

		else:
			print "Connected to DB successfully!"
			res = delete_times(conn)
			conn.commit()

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res
	pass

def scale_delete_queries():

	print 'Delete Queries'
	xs_times = delete_queries(XSd)
	s_times = delete_queries(Sd)
	m_times = delete_queries(Md)
	l_times = delete_queries(Ld)
	
	print xs_times, s_times, m_times, l_times
if __name__ == '__main__':
	#scale_data_basic_queries()
	#scale_data_join_queries()
	#scale_aggregate_queries()
	scale_delete_queries()
