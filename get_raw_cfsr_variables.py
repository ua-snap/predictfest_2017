# automate download of all available forecast years CFSv2 Data Daily
def get_closest_value_1d( arr, v ):
    return arr[ (np.abs(arr - v)).argmin() ]

def parse_page( url, parser='html.parser' ):
	page = requests.get( url )
	return BeautifulSoup(page.text, parser)

def list_folder_names( url, href_regex ):
	''' hack for working with CFSR '''
	soup = parse_page( url )
	links = soup.find_all('a', attrs={'href': href_regex})
	return np.array(list(set([ int(i.get('href').strip('/')) for i in links if '/data/cfsr-rfl-ts9' not in i.get('href') ])))

def list_file_names( url, month, day, starttime ):
	soup = parse_page( url )
	links = soup.find_all('a')
	return list(set([ i.get('href') for i in links if '{}{}{}'.format(month,day,starttime) in i.get('href') ]))

def run_get_filenames( month, day, starttime, variable, url, output_path ):
	# parse the {year}{month} landing page
	# href_regex = re.compile("^{}".format(year))
	href_regex = re.compile('\d+')
	foldernames = list_folder_names( url, href_regex )
	# slice to the starttimes we want based on month first
	foldernames = sorted([i for i in foldernames if str(i).endswith(month)])

	# # which value is the closest to our lookup value. --> currently it is the most recent day
	# lookup_value = int('{}{}{}'.format(year,month,day))
	# folder_name = str(get_closest_value_1d( foldernames, lookup_value ))

	# get the next level foldernames -- 6-hourly
	# url = url + '/' + folder_name
	# foldernames = list_folder_names( url, href_regex )
	commands = []
	out_commands = []
	# find the files we want and wget 'em
	for folder_name in foldernames:
		folder_url = url + '/{}'.format( folder_name )
		commands = commands + [ folder_url + '/' + fn for fn in list_file_names( folder_url, month, day, starttime )]
		dates = [ command.split('.')[-3] for command in commands ]

		for dt,command in zip(dates, commands):
			out_path = os.path.join( output_path, str(dt)[:-4], str(dt), variable )

			if not os.path.exists( out_path ):
				os.makedirs( out_path )

			os.chdir( out_path )
			out_commands = out_commands + [(out_path, command)]
	return out_commands

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
	group = 'cfsr-rfl-ts9'
	variables = ['tmp2m', 'prate']
	# information relating to the start day we want to harvest from each year.
	month = '08'
	day = '14'
	starttime = '00' # one of 4x daily 00,
	ncores = 15

	# list all the available months/years from the `cfsr-rfl-ts9` directory
	base_url = 'https://nomads.ncdc.noaa.gov/data/cfsr-rfl-ts9'
	for variable in variables:
		url = base_url + '/{}'.format( variable )
		commands = run_get_filenames( month, day, starttime, variable, url, os.path.join(output_path, group) )
					
		pool = mp.Pool( ncores )
		out = pool.map( run_wget, commands )
		pool.close()
		pool.join()


# # below is what the link looks like...
# url = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/2017/201711/20171112/2017111200'
