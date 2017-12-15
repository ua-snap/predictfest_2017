# automate download of most recent CFSv2 Data Daily

def get_closest_value_1d( arr, v ):
    return arr[ (np.abs(arr - v)).argmin() ]

def parse_page( url, parser='html.parser' ):
	page = requests.get( url )
	return BeautifulSoup(page.text, parser)

def list_folder_names( url, href_regex ):
	soup = parse_page( url )
	links = soup.find_all('a', attrs={'href': href_regex})
	return np.array(list(set([ int(i.get('href').strip('/')) for i in links ])))

def list_file_names( url, variable ):
	soup = parse_page( url )
	links = soup.find_all('a')
	return [ i.get('href') for i in links if i.get('href').endswith('.grb2') and variable in i.get('href') ][0]

if __name__ == '__main__':
	import re, os, requests
	from bs4 import BeautifulSoup
	from datetime import datetime as dt
	import numpy as np

	output_path = '/workspace/Shared/Users/malindgren/predictfest/harvester'
	variables = ['tmp2m', 'prate']

	today = dt.now()
	url = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/{}/{}{}'.format(today.year,today.year,today.month) #,today.year,today.month,today.day

	# parse the {year}{month} landing page
	href_regex = re.compile("^{}".format(today.year))
	foldernames = list_folder_names( url, href_regex )

	# which value is the closest to our lookup value. --> currently it is the most recent day
	lookup_value = int('{}{}{}'.format(today.year,today.month,today.day))
	folder_name = str(get_closest_value_1d( foldernames, lookup_value ))

	# get the next level foldernames -- 6-hourly
	url = url + '/' + folder_name
	foldernames = list_folder_names( url, href_regex )

	# find the files we want and wget 'em
	for folder_name in foldernames:
		folder_url = url + '/{}'.format( folder_name )
		for variable in variables:
			out_path = os.path.join( output_path, str(folder_name)[:-2], str(folder_name), variable )
			if not os.path.exists( out_path ):
				os.makedirs( out_path )
			os.chdir( out_path )
			os.system( 'wget ' + folder_url + '/{}'.format( list_file_names( folder_url, variable ) ) )

# url = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/2017/201711/20171112/2017111200'
