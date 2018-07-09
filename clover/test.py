from parsers import Spec, Data
import unittest
import os
from mock import patch
from datetime import date
import psycopg2 
import constants

# test for spec uploading
class TestSpec(unittest.TestCase):
	
	@classmethod
	@patch.object(Spec, 'archive')
	def setUp(cls, mocked_archive):
		cls.spec = Spec()
		cls.spec.load_specs()

	# test that specs exists
	def test1(self):
		self.assertTrue(self.table_exists('specs'))

	def test2(self, table_name):
		query = """ 
		select exists(select * from information_schema.tables where table_name='%s') 
		""" % table_name
		return self.spec.execute_query(query)[0][0]

	# check that spec file is uploaded properly
	def test_spec_table_meta(self):
		test_table_meta = [
			('test1', [{"datatype": "TEXT", "width": "10", "column name": "name"},
					  {"datatype": "BOOLEAN", "width": "1", "column name": "valid"},
					  {"datatype": "INTEGER", "width": "3", "column name": "count"}]),
			('test2', [{"datatype": "TEXT", "width": "10", "column name": "name"},
					  {"datatype": "BOOLEAN", "width": "1", "column name": "valid"},
					  {"datatype": "TEXT", "width": "0", "column name": "nonexistentcolumn"}])]
		self.assertEquals(test_table_meta, self.spec.execute_query("SELECT * from specs order by name"))


# test for data loading 
class TestData(unittest.TestCase):
	# test that data is populated into table
	# test that data is correct schema
	# test that elements are mapped correctly
	# test that created 
	
	@classmethod
	@patch.object(Spec, 'archive')
	@patch.object(Data, 'archive')
	def setUpClass(cls, mock_func1, mock_func2):
		cls.spec = Spec()
		cls.spec.load_specs()
		data = Data()
		data.consume_data_files()

	def test1(self):
		table_entries = [
			(1, date(1993, 06, 03), 'Foonyor', True, 1),
			(2, date(1993, 06, 03), 'Barzane', False, -12),
			(3, date(1993, 06, 03), 'Quuxitude', True, 103),
			(4, date(1993, 06, 04), 'Bob', True, 1),
			(5, date(1993, 06, 04), 'Vance', False, -12),
			(6, date(1993, 06, 04), 'Steve', True, 103),
		]
		self.assertEquals(table_entries, self.retrieve_table_data('test1'))


	def retrieve_table_data(self, table_name):
		query = """
			SELECT * from %s order by id;
		""" % table_name
		return self.spec.execute_query(query)