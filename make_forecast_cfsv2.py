# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# PREDICTFEST -- 2017
# make forecast counts for predicted years over the 9month period
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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
    import multiprocessing as mp
    from functools import partial

    base_path = '/workspace/Shared/Users/malindgren/predictfest/data/netcdf/cfsv2_forecast_ts_9mon'
    variables = [ 'tmp2m','prate' ]
    starttime = '00'
    begin = 2011
    end = 2017

    min_tmp = -1.1
    min_pr_list = [ 2.54, 7.62, 12.7 ] # [1,3,5] #inches threshold

    # LIST FILES -- [ORDER SHOULD BE TMP,PR]
    tmp_files, pr_files = [sorted([ os.path.join(r,fn) for r,s,files in os.walk( base_path ) for fn in files if variable in fn and fn.endswith('.nc') and starttime+'.daily' in fn ]) for variable in variables ]

    all_files = list(zip( tmp_files, pr_files ))

    for min_pr in min_pr_list:
        years = list( range( begin, end+1 ) )
        hold = [ wrap_compute_interaction( tmp, pr, min_tmp, min_pr ) for tmp, pr in all_files ]
        lons = hold[0].lon.data
        lats = hold[0].lat.data
        counts = [ ds.groupby('time.month').apply(lambda x: np.sum(x, axis=0)) for ds in hold ]
        for year, ds in zip( years, counts ):
            ds.coords['lon'] = (('x', 'y'), lons)
            ds.coords['lat'] = (('x', 'y'), lats)
            # crop to a reasonable extent
            cropped = ds.where((ds.lon > 180) & (ds.lon < 250) & (ds.lat > 45) & (ds.lat < 80), drop=True)
            out_fn = os.path.join( '/workspace/Shared/Users/malindgren/predictfest/data', 'outputs', 'cfsv2_forecast_ts_9mon', 'akcan_nevents_forecast_cfsv2_{}_{}_{}.nc').format(str(min_pr).replace('.', '_'), year, starttime ) 
            dirname = os.path.dirname( out_fn )
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            cropped.to_netcdf( out_fn, format='NETCDF4' )
            cropped.close()
            del cropped
