# PREDICTFEST

def compute_interaction( tmp_fn, pr_fn, min_tmp, min_pr, begin_date=None, end_date=None ):
    ''' compute interactions between thresholds of the tmp2m and prate from CFSv2 '''
    import xarray as xr
    # setup and convert variable units
    tmp = xr.open_mfdataset( tmp_fn )['TMP_P0_L103_GGA0'] - 273.15
    pr = xr.open_mfdataset( pr_fn )['PRATE_P0_L1_GGA0'] * 86400
    
    # sep = ds.sel( forecast_time0=slice(begin_date, end_date) )

    out = np.zeros_like( tmp.data )
    out[ (tmp <= min_tmp) & (pr >= min_pr)] = 1

    lons, lats = np.meshgrid(tmp['lon_0'].data, tmp['lat_0'].data)
    time = tmp['forecast_time0'].to_index()
    time.name = 'time'
    grouper = [ '{}-{}'.format(i.month, i.year) for i in tmp.forecast_time0.to_index() ]

    ds = xr.Dataset( {'derived':(['time','x', 'y'], out)},
                    coords={'lon': (['x', 'y'], lons),
                            'lat': (['x', 'y'], lats),
                            'time':time },
                    attrs=tmp.attrs )
    return ds

if __name__ == '__main__':
    import xarray as xr
    import numpy as np
    import os, glob
    import pandas as pd

    base_path = '/atlas_scratch/malindgren/ML_DATA/PredictFEST/CFSv2_NetCDF'
    # base_path = '/atlas_scratch/malindgren/PredictFEST/CFSv2_NetCDF'
    variables = [ 'tmp2m','prate' ]
    path_lookup = { variable:os.path.join(base_path, 'hindcast', variable, '081418' ) for variable in variables }

    min_tmp = -1.1
    min_pr = 0.254
    # tmp_fn = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/forecast/tmp2m/081500/tmp2m.01.2011081500.daily.nc'
    # pr_fn = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/forecast/prate/081500/prate.01.2011081500.daily.nc'
    begin_date = '09-01-1982'
    end_date = '09-30-1982'

    # FILE PATHS 
    # pr_paths = sorted( glob.glob( os.path.join( path_lookup['prate'], '*.nc') ))
    pr_fn = os.path.join( path_lookup['prate'], '*.nc')
    tmp_fn = os.path.join( path_lookup['tmp2m'], '*.nc')

    counts = compute_interaction( tmp_fn, pr_fn, min_tmp, min_pr, begin_date=None, end_date=None )

    out = []
    dates = []
    for year, yr_grp in counts.groupby( 'time.year' ):
        for month, mon_grp in yr_grp.groupby('time.month'):
            dates.append( '{}-{}'.format(year, month) )

    new_times = [ pd.Timestamp(i) for i in dates ]
    final = xr.concat( out, dim='time' )
    final['time'] = pd.DatetimeIndex( new_times )

    done = final.groupby('time.month').apply( lambda x: np.mean(x, axis=0))    
    done.to_netcdf( os.path.join( base_path, 'outputs', 'nevents_climatology_cfsv2.nc'), format='NETCDF4' )


    # ds = xr.open_mfdataset( tmp_fn )
    # times = [ (i.year, i.month) for i in ds.forecast_time0 ]

    # # time = counts.time
    # counts['grouper'] = (('time'), grouper)
    # counts2 = counts.groupby( 'grouper' )

    # # ds.coords['grouper'] = (('x', 'y'), lat)

    # month_counts = counts.groupby( 'time.month' ).apply(lambda x: np.sum(x, axis=0))
    


    # # # OPEN THE HINDCAST DATA FILES:
    # # pr = xr.open_mfdataset( os.path.join( path_lookup['pr'], '*.nc'), auto_close=True )
    # # tmp = xr.open_mfdataset( os.path.join( path_lookup['tmp'], '*.nc'), auto_close=True )


    # # # RESAMPLE TO MONTHLIES
    # # pr_mon = pr.resample( 'M', dim='forecast_time0', how='' )
    # # tmp_mon = 

    # # d1 = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/hindcast/tmp2m/081400'
    # # d2 = '/workspace/Shared/Users/malindgren/predictfest/CFSv2_NetCDF/forecast/tmp2m/081500'
    # # years = [list(range(1982, 2010+1)), [2011, 2012]]

    # # # grab the past files from the hindcast...
    # # d1 = sorted( glob.glob( os.path.join(d1, '*.nc') ) )
    # # d2 = sorted( glob.glob( os.path.join(d2, '*.nc') ) )
    # # d2 = [ fn for fn in d2 if '2011' in fn or '2012' in fn ]
    
    # # all_files = d1+d2

    # # hold = [ xr.open_dataset(fn, autoclose=True).isel()['TMP_P0_L103_GGA0'] for fn in all_files ]

    # # temp_variable = 'TMP_P0_L103_GGA0'
    # # precip_variable = 'PRATE_P0_L1_GGA0'


    # # # Septembers:
    # # min_temp = -1.11111
    # # min_snow = 0.254 



    # # how many snow event 