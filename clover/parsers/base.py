### BASE CLASS FOR PARSING

# CONTAINS ALL PG UTILS 

import psycopg2
import os
import csv
import constants
import json

class Base(object):
	def __init__(self):
		self.data_path = constants.DATA_PATH
		self.spec_path = constants.SPEC_PATH 
		self.conn = psycopg2.connect("dbname = %s user = %s password = %s" % 
									 (constants.dbname, constants.user, constants.password))

	# simple function for executing postgres query 
	def execute_query(self, query):
		cur = self.conn.cursor()
		cur.execute(query)
		result = cur.fetchall() if cur.description else []
		cur.close()
		self.conn.commit()
		return result

	def archive(self, file_path):
		archive_path = constants.ARCH_PATH + file_path
		os.rename(file_path, archive_path)

	def close_conn(self):
		self.conn.close()

	def retrieve_column_metadata(self, name):
		query = """
		SELECT meta from SPECS where name = '%s';
		""" % name
		results = self.execute_query(query)
		return results[0][0]


