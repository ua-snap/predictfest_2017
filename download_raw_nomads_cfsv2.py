# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # DOWNLOAD THE DATA FROM NOMADS FOR THE PREDICTFEST
# # # --> simple script building paths from knowns and downloading with wget.
# # #     no easy way to download with https, this is lowest friction.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
import os

base_path = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_GRIB/cfsv2_forecast_ts_9mon/forecast'
years = list(range(2011,2017+1))

# ensemble members:
members = [ '{}081500', '{}081506', '{}081512', '{}081518' ]

for year in years:
	for member in members:
		# base urls to grab the same data for each year...
		base_url_prate = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/{}/{}08/{}0815/'+member+'/prate.01.'+member+'.daily.grb2'
		
		base_url_tmp2m = 'https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_ts_9mon/{}/{}08/{}0815/'+member+'/tmp2m.01.'+ member +'.daily.grb2'
		out_path = os.path.join( base_path, 'prate', member.format('year-') )
		if not os.path.exists(out_path):
			os.makedirs(out_path)
		os.chdir( out_path )
		os.system( 'wget {}'.format( base_url_prate.format( year, year, year, year, year ) ) )

		out_path = os.path.join( base_path, 'tmp2m', member.format('year-') )
		if not os.path.exists(out_path):
			os.makedirs(out_path)
		os.chdir( out_path )
		os.system( 'wget {}'.format( base_url_tmp2m.format( year, year, year, year, year ) ) )


# # # DOWNLOAD THE HINDCAST FILES... CFSv2 -- DIFFERENT DIRECTORY AND DIFFERENT FOLDER HIERARCHY...
import os

base_path = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_GRIB/hindcast'
years = list(range(1982,2010+1))
members = ['00','06','12','18']

for year in years:
	for member in members:
		# base urls to grab the same data for each year...
		# ftp://nomads.ncdc.noaa.gov/CFSRR/cfsr-rfl-ts9 <-- this may work if other site is down...
		base_url_prate = 'https://nomads.ncdc.noaa.gov/data/cfsr-rfl-ts9/prate/{}08/prate.{}0814'+member+'.time.grb2'
		
		base_url_tmp2m = 'https://nomads.ncdc.noaa.gov/data/cfsr-rfl-ts9/tmp2m/{}08/tmp2m.{}0814'+member+'.time.grb2'
		out_path = os.path.join( base_path, 'prate', 'year-0814'+member )
		if not os.path.exists(out_path):
			os.makedirs(out_path)
		os.chdir( out_path )
		os.system( 'wget {}'.format( base_url_prate.format( year, year ) ) )

		out_path = os.path.join( base_path, 'tmp2m', 'year-0814'+member )
		if not os.path.exists(out_path):
			os.makedirs(out_path)
		os.chdir( out_path )
		os.system( 'wget {}'.format( base_url_tmp2m.format( year, year ) ) )

