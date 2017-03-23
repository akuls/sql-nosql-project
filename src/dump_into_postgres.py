#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2

try:
    conn = psycopg2.connect("dbname='1m_song' user='postgres' host='localhost' password='akul'")
    print 'Yes, connected!'
except:
    print "I am unable to connect to the database"