# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# PREDICTFEST -- 2017
# make climatology (28-year avg) counts for predicted years over 
#   the 9month period.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def compute_interaction( tmp, pr, min_tmp, min_pr, time_dim='forecast_time0' ):
    ''' compute interactions between thresholds of the tmp2m and prate from CFSR '''
    
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

    base_path = '/workspace/Shared/Users/malindgren/predictfest/data/netcdf/cfsr-rfl-ts9'
    output_path = os.path.join( '/workspace/Shared/Users/malindgren/predictfest/data', 'outputs', 'cfsr-rfl-ts9' )
    variables = [ 'tmp2m','prate' ]
    starttime = '00'
    # path_lookup = { variable:os.path.join(base_path, 'hindcast', variable, '081418' ) for variable in variables }

    min_tmp = -1.1
    min_pr_list = [ 2.54, 7.62, 12.7 ] # [1,3,5] #inches threshold

    # LIST FILES 
    tmp_files, pr_files = [sorted([ os.path.join(r,fn) for r,s,files in os.walk( base_path ) for fn in files if variable in fn and fn.endswith('.nc') and starttime+'.time' in fn ]) for variable in variables ]
    # pr_files = sorted( glob.glob( os.path.join( path_lookup['prate'], '*.nc') ) )
    # tmp_files = sorted( glob.glob( os.path.join( path_lookup['tmp2m'], '*.nc') ) )

    all_files = list(zip( tmp_files, pr_files ))

    for min_pr in min_pr_list:
        hold = [ wrap_compute_interaction( tmp, pr, min_tmp, min_pr ) for tmp, pr in all_files ]
        lons = hold[0].lon.data
        lats = hold[0].lat.data
        counts = [ ds.groupby('time.month').apply(lambda x: np.sum(x, axis=0)) for ds in hold ]

        # variability metrics
        stdev = [ ds.groupby('time.month').apply(lambda x: np.std(x, axis=0)) for ds in hold ]
        ds = xr.concat(counts, dim='month')
        stdev = ds.groupby('month').apply( np.std, axis=0 )
        stdev.coords['lon'] = (('x', 'y'), lons)
        stdev.coords['lat'] = (('x', 'y'), lats)

        done = sum( counts ) / len(counts)
        done.coords['lon'] = (('x', 'y'), lons)
        done.coords['lat'] = (('x', 'y'), lats)

        if not os.path.exists( output_path ):
            os.makedirs( output_path )

        cropped = done.where((done.lon > 180) & (done.lon < 250) & (done.lat > 40) & (done.lat < 80), drop=True)
        cropped.to_netcdf( os.path.join( output_path, 'akcan_nevents_climatology_cfsr_{}.nc').format(str(min_pr).replace('.', '_')), format='NETCDF4' )
        # done.to_netcdf( os.path.join( output_path, 'global_nevents_climatology_cfsr_{}.nc').format(str(min_pr).replace('.', '_')), format='NETCDF4' )
        
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


        # PLOT IT UP
        cropped.derived.plot(x='lon', y='lat', col='month', col_wrap=3)
        plt.savefig( os.path.join( output_path, 'akcan_nevents_climatology_cfsr_{}.png').format(str(min_pr).replace('.', '_')) )
        plt.close()

        # PLOT THE STDEV
        stdev_cropped = stdev.where((stdev.lon > 180) & (stdev.lon < 250) & (stdev.lat > 40) & (stdev.lat < 80), drop=True)
        stdev_cropped.derived.plot(x='lon', y='lat', col='month', col_wrap=3)
        plt.savefig( os.path.join( output_path, 'akcan_stdev_nevents_climatology_cfsr_{}.png').format(str(min_pr).replace('.', '_')) )
        plt.close()



    # # FAIRBANKS PIXEL EXTRACTION -- BBOLTON
    # # fbx = (64.8164, -147.8635) # greenwich
    # fbx = (212.1365, 64.8164) # pacific?

    # def get_closest_value_1d( arr, v ):
    #     return arr[ (np.abs(arr - v)).argmin() ]

    # profile = cropped.where( (cropped.lon == get_closest_value_1d(cropped.lon.data.ravel(), 212.1365)) & (cropped.lat == get_closest_value_1d(cropped.lat.data.ravel(), 64.8164), drop=True) )
    # a = transform_from_latlon(np.unique(lats.data), np.unique(lons.data))
    # col, row = ~a * fbx

    # ind = np.where((cropped.lon == get_closest_value_1d(cropped.lon.data.ravel(), 212.1365)) & \
    #                 (cropped.lat == get_closest_value_1d(cropped.lat.data.ravel(), 64.8164)))

    # fbx_derived = cropped.derived.data[ ..., int(ind[0]), int(ind[1])]
