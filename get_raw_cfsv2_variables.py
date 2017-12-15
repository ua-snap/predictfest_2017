# automate download of all available forecast years CFSv2 Data Daily
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

def run_get_filenames( year, base_url, output_path ):
	url = base_url+'/{}/{}{}'.format(year,year,month)

	# parse the {year}{month} landing page
	href_regex = re.compile("^{}".format(year))
	foldernames = list_folder_names( url, href_regex )

	# which value is the closest to our lookup value. --> currently it is the most recent day
	lookup_value = int('{}{}{}'.format(year,month,day))
	folder_name = str(get_closest_value_1d( foldernames, lookup_value ))

	# get the next level foldernames -- 6-hourly
	url = url + '/' + folder_name
	foldernames = list_folder_names( url, href_regex )
	commands = []
	# find the files we want and wget 'em
	for folder_name in foldernames:
		folder_url = url + '/{}'.format( folder_name )
		
		for variable in variables:
			out_path = os.path.join( output_path, str(folder_name)[:-2], str(folder_name), variable )
			if not os.path.exists( out_path ):
				os.makedirs( out_path )
			os.chdir( out_path )
			commands = commands + [(out_path, folder_url + '/{}'.format( list_file_names( folder_url, variable ) ))]
	return commands

def run_wget( args ):
	out_path, command = args
	os.chdir( out_path )
	return os.system( 'wget -q ' + command )

if __name__ == '__main__':
	import re, os, requests
	from functools import partial
	from bs4 import BeautifulSoup
	from datetime import datetime as dt
	import numpy as np
	import multiprocessing as mp

	output_path = '/workspace/Shared/Users/malindgren/predictfest/data/grib'
	group = 'cfsv2_forecast_ts_9mon'
	variables = ['tmp2m', 'prate']
	# information relating to the start day we want to harvest from each year.
	month = '08'
	day = '15'
	ncores = 15

	# list all the available years from the `cfsv2_forecast_ts_9mon` directory
	base_url = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon'
	years = list_folder_names( base_url, re.compile('\d+') )

	f = partial( run_get_filenames, base_url=base_url, output_path=os.path.join( output_path, group ) )
	pool = mp.Pool( ncores )
	out = pool.map( f, years )
	commands = [ j for i in out for j in i ]
	pool.close()
	pool.join()
	pool = None
	del pool
				
	pool = mp.Pool( ncores )
	out = pool.map( run_wget, commands )
	pool.close()
	pool.join()


# # below is what the link looks like...
# url = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/2017/201711/20171112/2017111200'
