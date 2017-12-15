# # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # PREPARE THE RAW CFSv2 GRIB FOR PREDICTFEST
# # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def prepare_cfsv2( ds ):
	''' take GRIB2 CFSv2 dset and average the 4x daily to daily '''

	time_var = 'forecast_time0'

	# slice data to _complete_ 4x daily days...
	days_index = [ i.days for i in ds[time_var].to_index() ]
	arr = np.array(zip(*np.unique(days_index, return_counts=True)))
	begin_end = arr[ np.where( arr[:,1] != 4 ), 1 ].flatten().tolist()

	if len(begin_end) == 1:
		
		if arr[ np.where( arr[:,1] != 4 ), 0].flatten() > 15: # just some smallish number to use
			begin, end = + [None] + begin_end
			ds = ds.isel( forecast_time0=slice(begin,-end) )
		else:
			begin, end = begin_end + [None]
			ds = ds.isel( forecast_time0=slice(begin,end) )
	else:
		begin, end = begin_end
		ds = ds.isel( forecast_time0=slice(begin,-end) )

	# resample to 1x daily from 4x dailies...
	return ds.resample( 'D', dim=time_var, how='mean' )

def update_dates( ds, year, variable, start_date ):
	''' put real dates in the files for ease-of-use'''
	start_date = start_date.format( year )
	time, lon, lat = ds[variable].shape
	ds['forecast_time0'] = pd.date_range( start_date, periods=time )
	return ds

def slice_to_overlapping_dates( ds, year ):
	''' slice to begin at Sept 1st -- per Peter Bienik '''
	return ds.sel( forecast_time0=slice('{}-09-01'.format(year), '{}-05-31'.format(year+1) ) )

def run( fn, out_fn, year, variable, start_date ):
	''' pre-process the CFSv2 GRIB2 data and write to NetCDF'''
	# open the dataset
	ds = xr.open_dataset( fn, engine='pynio' )

	# prepare the data 
	ds = prepare_cfsv2( ds )
	ds = update_dates( ds, year, variable, start_date )
	ds = slice_to_overlapping_dates( ds, year )

	# dump it back out to disk
	dirname, basename = os.path.split( out_fn )
	try:
		if not os.path.exists( dirname ):
			os.makedirs( dirname )
	except:
		pass

	encoding = ds[ variable ].encoding
	encoding.update( zlib=True, complevel=5, contiguous=False, chunksizes=None, dtype='float32' )
	ds[ variable ].encoding = encoding

	ds.to_netcdf( out_fn, mode='w', format='NETCDF4' )

	# clean up (forcibly)
	ds.close()
	ds = None
	del ds
	
	return out_fn

def main( x ):
	return run( *x )

if __name__ == '__main__':
	import xarray as xr
	# import pynio # if you dont have this installed it wont work... USE CONDA!
	import os, glob
	import numpy as np
	import pandas as pd
	import multiprocessing as mp

	# groups = [ 'forecast', 'hindcast' ]
	# base_path = '/workspace/Shared/Users/malindgren/predictfest/data/grib'
	base_path = '/workspace/Shared/Users/malindgren/predictfest/data/grib/cfsr-rfl-ts9'
	# output_path = '/workspace/Shared/Users/malindgren/predictfest/data/netcdf'
	output_path = '/workspace/Shared/Users/malindgren/predictfest/data/netcdf/cfsr-rfl-ts9'
	ncores = 7
		
	# # 8/15 (forecast) is real start, but +1day for dropping day0 non-full 4x daily
	# start_date = '{}-08-16'
	# 8/14 (hindcast) is real start, but +1day for dropping day0 non-full 4x daily
	start_date = '{}-08-15'

	# we are currently only working with tmp2m / prate 
	var_lookup = { 'prate':'PRATE_P0_L1_GGA0',
					'tmp2m':'TMP_P0_L103_GGA0'}

	# update the paths if needed... this is currently not needed
	bp = os.path.join( base_path )
	op = os.path.join( output_path )

	# setup the args
	files = [ os.path.join(r,fn) for r,s,files in os.walk( bp ) for fn in files if fn.endswith( '.grb2' ) ]
	out_files = [ fn.replace(bp,op).replace('.grb2', '.nc') for fn in files ]
	years = [ int(os.path.basename(fn).split('.')[-3][:4]) for fn in files ]
	variables = [ var_lookup[os.path.basename(fn).split('.')[0]] for fn in files ]
	start_dates = [start_date]*len(files) # single date replicated for ease of use.
	args = zip(files, out_files, years, variables, start_dates)

	# run it with workers...
	pool = mp.Pool( ncores )
	done = pool.map( main, args )
	pool.close()
	pool.join()

	# force cleanup
	pool = None
	del pool



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # AVERAGE THE ENSEMBLE MEMBERS -- PER PETER BIENIK
# # base_path = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF'
# output_path_ensemble = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_EnsembleMean'
# base_path = output_path # temporary hack
# variables = ['prate','tmp2m']
# try:
# 	members = [ 'year-081500', 'year-081506', 'year-081512', 'year-081518' ]
# except:
# 	members = [ 'year-081400', 'year-081406', 'year-081412', 'year-081418' ]
# 	pass

# for variable in variables:
# 	df = pd.DataFrame({ member:sorted(glob.glob( os.path.join( base_path, variable, member, '*.nc' ))) for member in members })

# 	for rownum, row in df.iterrows():
# 		fn = os.path.basename( row.tolist()[0] )
# 		out_fn = os.path.join( output_path_ensemble, variable, fn.replace('.nc','_ensemble_mean.nc').replace('081500', '0815') )
# 		print( out_fn )

# 		dirname, basename = os.path.split( out_fn )
# 		if not os.path.exists( dirname ):
# 			os.makedirs( dirname )

# 		ds = sum([ xr.open_dataset(fn) for fn in row.tolist()]) / 4

# 		# encoding = ds[ var_lookup[variable] ].encoding
# 		# chunksize = ds[ variable ].shape
# 		# encoding.update( zlib=True, complevel=5, chunksizes=None )

# 		ds.to_netcdf( out_fn, mode='w', format='NETCDF4_CLASSIC' )

# 		# force cleanup
# 		del ds
# 		ds = None



# # # LEAP YEARS LIST: -- REFERENCE
# 2004, 2008, 2012, 2016, 2020, 
# 2024, 2028, 2032, 2036, 2040, 2044, 
# 2048, 2052, 2056, 2060, 2064, 2068, 
# 2072, 2076, 2080, 2084, 2088, 2092, 2096

# SOME INFORMATIONS ABOUT THE FILES...
# forecast_shape = (1157, 190, 384)
# hindcast_shape = (1281, 190, 384)

# # SHARE TEST FILES:
# /workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/forecast/prate/year-081500/prate.01.2011081500.daily.nc
# /workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/hindcast/prate/year-081400/prate.2010081400.time.nc
