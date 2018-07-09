from base import Base
import os
import json 
import constants
import csv

class Spec(Base):
	
	def __init__ (self):
		super(Spec, self).__init__()
		# create specs table if it doesn't exist 
		query = """
			CREATE TABLE IF NOT EXISTS SPECS (
				name varchar(256) PRIMARY KEY,
				meta json 
			)
		"""
		self.execute_query(query)

	# load all spec files in directory
	def load_specs(self):
		for f in os.listdir(self.spec_path):
			if f.endswith('.csv'):
				name = f[:-4]
				spec_file_name = os.path.join(self.spec_path, f)
				columns = self.read_spec_file(spec_file_name)
				if not self.table_exists(name, columns):
					self.create_table(name, columns)
				self.archive(spec_file_name)
			else:
				print "found non csv file %s, skipping" % f
		return

	# create table given set of specs
	# name is the name of table
	# columns is list of dicts that represent column information
	def create_table(self, name, columns):
		column_str_list = []
		for column in columns:
			column_name = column.get("column name")
			data_type = column.get("datatype")
			column_str_list.append(column_name + " " + data_type)
		query = """
			CREATE TABLE %s (
			id serial PRIMARY KEY,
			dropped date, 
			%s
			)
		""" % (name, ",".join(column_str_list))
		self.execute_query(query)

		## add this to specs table for reference
		query = """
			INSERT INTO SPECS (name, meta)
			VALUES ('%s', '%s') 
		""" % (name, json.dumps(columns))
		self.execute_query(query)

	def table_exists(self, name, columns):
		query = """
			SELECT name, meta from SPECS where name = '%s'
		""" % name 
		result = self.execute_query(query)
		if result and len(result) > 0:
			db_meta = result[0][1]
			if db_meta != columns:
				return False 
		else:
			return False
		return True 

	def read_spec_file(self, filename):
		columns = []
		with open(filename, 'r') as f:
			columns = [row for row in csv.DictReader(f)]
		return columns


