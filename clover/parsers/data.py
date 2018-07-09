from base import Base 
import os 
import constants

# parser to read data from class  

class Data(Base):
	def __init__ (self):
		super(Data, self).__init__()
		# create specs table if it doesn't exist 

	# function to collect and read files
	def consume_data_files(self):
		for f in os.listdir(constants.DATA_PATH):
			if f.endswith('.txt'):
				name, date = f.split('_')
				date = date[:-4]
				file_name = os.path.join(constants.DATA_PATH, f)

				columns_meta = self.retrieve_column_metadata(name)
				column_name = [column_meta.get('column name')
									 for column_meta in
									 columns_meta] + [constants.DATE_DROPPED]
				for chunk in self.read_file(file_name, columns_meta):
					self.insert_chunk(chunk, name,
										   column_name, date)
				self.archive(file_name)


	# function to read file
	def read_file(self, filename, columns_data):
		lst = []
		with open(filename, 'r') as f:
			for row in f:
				lst.append(self.read_row(row, columns_data))
		return lst

	# function to read single row of a file
	def read_row(self, row, columns_data):
		col_vals = []
		i = 0
		for column_data in columns_data:
			width = int(column_data.get('width'))
			col_val = row[i:i+width].strip()
			i += width 
			col_vals.append(col_val)
		return col_vals

	def insert_chunk(self, row_list, table_name, table_column_names, file_date):
		table_column_names_str = ",".join(table_column_names)
		table_column_values_str = "'" + "','".join(row_list + [file_date]) + "'"
		query = """
		INSERT INTO %s (%s)
		VALUES (%s)
		""" % (table_name, table_column_names_str, table_column_values_str)
		self.execute_query(query)
