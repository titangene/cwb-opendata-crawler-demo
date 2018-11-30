import datetime

import pandas as pd
import numpy as np
import requests

CWB_URL = 'http://opendata.cwb.gov.tw/api/v1/rest/datastore/'
# 自動雨量站 資料項編號
DATA_ID = 'O-A0002-001'
AUTH_KEY = 'YOUR_AUTH_KEY'

def get_data_from_cwb(data_id, auth_key):
	CWB_API = '{}{}?Authorization={}&limit=3'.format(CWB_URL, DATA_ID, AUTH_KEY)
	r = requests.get(CWB_API)
	data = r.json()
	return data

def parse_json_to_dataframe(data):
	columns = ['stationId', 'locationName', 'lat', 'lon', 'obstime', 'ELEV', 'RAIN', 'MIN_10', 'HOUR_3', 'HOUR_6', 'HOUR_12', 'HOUR_24', 'NOW']
	df = pd.DataFrame(columns=columns)
	data_dict = {}
	locations = data['records']['location']
	row = -1
	for location in locations:
		row = row + 1
		data_dict['stationId'] = location['stationId']
		data_dict['locationName'] = location['locationName']
		data_dict['obstime'] = location['time']['obsTime']
		data_dict['lat'] = location['lat']
		data_dict['lon'] = location['lon']

		factors = location['weatherElement']
		for factor in factors:
			factor_name = factor['elementName']
			data_dict[factor_name] = factor['elementValue']

		parameters = location['parameter']
		for parameter in parameters:
			factor_name = parameter['parameterName']
			data_dict[factor_name] = parameter['parameterValue']

		for key in data_dict.keys():
			df.loc[row,key] = data_dict[key]

	return df

if __name__ == "__main__":
	json_data = get_data_from_cwb(DATA_ID, AUTH_KEY)
	df = parse_json_to_dataframe(json_data)

	current_datetime = datetime.datetime.now()
	df['reportTime'] = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

	save_file_name = 'data/{}_data.csv'.format(current_datetime.strftime('%Y-%m-%d_%H-%M-%S'))
	df.to_csv(save_file_name, encoding='utf-8', index=False)