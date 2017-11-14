# PREDICTFEST

def compute_interaction( tmp, pr, min_tmp, min_pr, time_dim='forecast_time0' ):
    ''' compute interactions between thresholds of the tmp2m and prate from CFSv2 '''
    
    # build an output array and fill it using threshold conditions
    out = np.zeros_like( tmp.data )
    out[ (tmp <= min_tmp) & (pr >= min_pr)] = 1

    # mesh the lat/lons
    lons, lats = np.meshgrid(tmp['lon_0'].data, tmp['lat_0'].data)
    time = tmp[time_dim].to_index()
    time.name = 'time'
    grouper = [ '{}-{}'.format(i.month, i.year) for i in tmp[time_dim].to_index() ]

    # build a new IN MEMORY NetCDF
    ds = xr.Dataset( {'derived':(['time','x', 'y'], out)},
                    coords={'lon': (['x', 'y'], lons),
                            'lat': (['x', 'y'], lats),
                            'time':time },
                    attrs=tmp.attrs )
    return ds

def wrap_compute_interaction( tmp, pr, min_tmp, min_pr ):
    # READ IN DATA AND CONVERT UNITS
    tmp = xr.open_mfdataset( tmp )['TMP_P0_L103_GGA0'] - 273.15
    pr = xr.open_mfdataset( pr )['PRATE_P0_L1_GGA0'] * 86400
    return compute_interaction( tmp, pr, min_tmp, min_pr )

def transform_from_latlon(lat, lon):
    lat = np.asarray(lat)
    lon = np.asarray(lon)
    trans = Affine.translation(lon[0], lat[0])
    scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
    return trans * scale
    

if __name__ == '__main__':
    import os, glob
    import matplotlib
    matplotlib.use('agg')
    from matplotlib import pyplot as plt
    import xarray as xr
    import numpy as np
    import pandas as pd
    from affine import Affine

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

    # LIST FILES 
    pr_files = sorted( glob.glob( os.path.join( path_lookup['prate'], '*.nc') ) )
    tmp_files = sorted( glob.glob( os.path.join( path_lookup['tmp2m'], '*.nc') ) )

    all_files = zip( tmp_files, pr_files )
    hold = [ wrap_compute_interaction( tmp, pr, min_tmp, min_pr ) for tmp, pr in all_files ]
    counts = [ ds.groupby('time.month').apply(lambda x: np.sum(x, axis=0)) for ds in hold ]
    done = sum( counts ) / len(counts)
    lons = hold[0].lon.data
    lats = hold[0].lat.data
    done.coords['lon'] = (('x', 'y'), lons)
    done.coords['lat'] = (('x', 'y'), lats)

    # # ANOTHER WAY TO COMPUTE THE SAME THING
    # counts = wrap_compute_interaction( tmp_files, pr_files, min_tmp, min_pr )

    # out = []
    # dates = []
    # for year, yr_grp in counts.groupby( 'time.year' ):
    #     for month, mon_grp in yr_grp.groupby('time.month'):

    #         dates = dates + ['{}-{}'.format(year, month)]
    #         out = out + [mon_grp.apply(lambda x: np.sum(x, axis=0))]

    # new_times = [ pd.Timestamp(i) for i in dates ]
    # final = xr.concat( out, dim='time' )
    # final['time'] = pd.DatetimeIndex( new_times )

    # done = final.groupby('time.month').mean( axis=0 )
    # #END OTHE WAY TO DO IT

    # CROP TO A BETTER DOMAIN FOR AK / CANADA 
    cropped = done.where((done.lon > 150) & (done.lon < 270) & (done.lat > 35) & (done.lat < 80), drop=True)
    cropped.to_netcdf( os.path.join( base_path, 'outputs', 'akcan_nevents_climatology_cfsv2.nc'), format='NETCDF4' )


    # PLOT IT UP
    cropped.derived.plot(x='lon', y='lat', col='month', col_wrap=3)
    plt.savefig( os.path.join( base_path, 'outputs', 'akcan_nevents_climatology_cfsv2.png') )
    plt.close()


    # FAIRBANKS PIXEL EXTRACTION -- BBOLTON
    # fbx = (64.8164, -147.8635) # greenwich
    fbx = (212.1365, 64.8164) # pacific?

    def get_closest_value_1d( arr, v ):
        return arr[ (np.abs(arr - v)).argmin() ]

    profile = cropped.where( (cropped.lon == get_closest_value_1d(cropped.lon.data.ravel(), 212.1365)) & (cropped.lat == get_closest_value_1d(cropped.lat.data.ravel(), 64.8164), drop=True) )
    a = transform_from_latlon(np.unique(lats.data), np.unique(lons.data))
    col, row = ~a * fbx

    ind = np.where((cropped.lon == get_closest_value_1d(cropped.lon.data.ravel(), 212.1365)) & \
                    (cropped.lat == get_closest_value_1d(cropped.lat.data.ravel(), 64.8164)))

    fbx_derived = cropped.derived.data[ ..., int(ind[0]), int(ind[1])]



# # # # WORK AREA REMOVE WHEN DONE
    # ds = xr.open_mfdataset( tmp_fn )
    # times = [ (i.year, i.month) for i in ds.forecast_time0 ]

    # # time = counts.time
    # counts['grouper'] = (('time'), grouper)
    # counts2 = counts.groupby( 'grouper' )

    # # ds.coords['grouper'] = (('x', 'y'), lat)

    # month_counts = counts.groupby( 'time.month' ).apply(lambda x: np.sum(x, axis=0))

        # # READ IN DATA AND CONVERT UNITS
    # tmp = xr.open_mfdataset( tmp_files )['TMP_P0_L103_GGA0'] - 273.15
    # pr = xr.open_mfdataset( pr_files )['PRATE_P0_L1_GGA0'] * 86400

    
    # lat_slice = (75, 40)
    # lon_slice = (0, 127)

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