from pymongo import MongoClient
from pprint import pprint
import csv
import urllib.request
import py7zr
import os
import time

YEARS = ['2016', '2017', '2018', '2019', '2020']
RESULTS_FILENAME = 'Results.csv'
MAX_INSERT_ROWS = 15000
RESULT_HEADER = ['Region', 'Year', 'Mark']


def download(year):
	url = f'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO{year}.7z'
	try:
		with urllib.request.urlopen(url) as request:
			filesize = 0
			filename = f'data_{year}.7z'
			with open(filename, 'wb') as file:
				filesize += file.write(request.read())
		return filename
	except:
		print(f'Couldn\'t download file from {url}')


def extract(filename, year):
	with py7zr.SevenZipFile(filename, 'r') as archive:
		archive.extract(targets=[f'Odata{year}File.csv', f'OpenData{year}.csv'])
	if os.path.exists(f'Odata{year}File.csv'):
		os.rename(f'Odata{year}File.csv', f'OpenData{year}.csv')


def connect():
	client = MongoClient("mongodb+srv://main-user:1Qwerty@lab4.ixtvn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")
	db = client.zno_res
	return db


def get_data(year):
	print(f'Downloading data file for {year}')
	filename = download(year)
	print(f'Downloaded file {filename}. Extracting...')
	extract(filename, year)
	print(f'Extracted\n')


def insert():
	def get_encoding(year):
		if year in ['2017', '2018']:
			return 'utf-8-sig'
		else:
			return None

	def convert_type(value):
		if value=='null':
			return None
		try:
			res = float(value.replace(',', '.'))
			return res
		except:
			return value

	for year in YEARS:
		filename = f'OpenData{year}.csv'
		with open(filename, 'r', newline='', encoding=get_encoding(year)) as file:
			print(f'Processing file {filename}')
			data = csv.reader(file, delimiter=';', quotechar='"')
			header = list(map(str.lower, next(data)))
			num_of_rows = 0
			
			for j in range(MAX_INSERT_ROWS):
				row = next(data)
				num_of_rows += 1
				insert_dct = {}
				for i, col in enumerate(header):
					if row[i] != None:
						insert_dct[col] = convert_type(row[i])
				insert_dct["year"] = year
				db.zno_res.insert_one(insert_dct)


			print(f'Inserted {num_of_rows} rows from {filename}')


def select():
	find_result = db.zno_res.aggregate([
		{
			'$match' : {
				"year" : { '$in': ["2019", "2020"] },
				"engteststatus" : "Зараховано"
			}
		},
		{
			'$group' : {
				'_id': { 'region': '$regname', 'year': '$year' },
				'min': { '$min': '$engball100' }
			}
		},
		{
			'$sort' : { '_id.region': 1, '_id.year': 1 }
		}
	])
	
	data = []
	for el in find_result:
		row = [el["_id"]["region"], el["_id"]["year"], el["min"]]
		data.append(row)

	with open(RESULTS_FILENAME, 'w', newline='') as file:
		writer = csv.writer(file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(RESULT_HEADER)
		for row in data:
			writer.writerow(row)
	print(f"\nCreated file {RESULTS_FILENAME}")


if __name__ == "__main__":
	db = connect()

	for year in YEARS:
		if not os.path.exists(f'OpenData{year}.csv'):
			get_data(year)

	start = time.time()
	insert()
	duration = time.time()-start
	with open('Duration.txt', 'w') as file:
		file.write(f'Duration of inserting data from all the years is {duration}')

	select()