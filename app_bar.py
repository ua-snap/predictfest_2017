# # # # # # # # # # # # # # # # # # # # 
# DASH APP SKELETON FOR PREDICTFEST
# # # # # # # # # # # # # # # # # # # # 

import dash, os, glob, json
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr

app = dash.Dash(__name__)
server = app.server
# server.secret_key = os.environ['SERVER_SECRET_KEY']
server.secret_key = 'SECRET-SNAP-KEY'
app.config.supress_callback_exceptions = True
app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
mapbox_access_token = 'pk.eyJ1IjoiZWFydGhzY2llbnRpc3QiLCJhIjoiY2o4b3J5eXdwMDZ5eDM4cXU4dzJsMGIyZiJ9.a5IlzVUBGzJbQ0ayHC6t1w'


# # # OPEN UP SOME DATASETS:
# forecasts
forecast_lookup = {year:xr.open_dataset('./data/akcan_nevents_forecast_cfsv2_7_62_{}_00.nc'.format(year))['derived'] for year in range(2011, 2018)}

# clims
clim_counts = xr.open_dataset( './data/akcan_nevents_climatology_cfsr_7_62.nc' )['derived']
clim_deviation = xr.open_dataset( './data/akcan_nevents_climatology_cfsr_7_62_stdev.nc' )['derived']
lons = clim_counts.lon.data.ravel()
lats = clim_counts.lat.data.ravel()
# ROTATE! 
lons[ lons > 180 ] = lons[ lons > 180 ] - 360

# years = range(1982, 2011+1)
# clim_std = xr.open_dataset( )
shapes = './data/Alaska_Albers_ESRI_WGS84.geojson'


# THIS NEEDS TO CHANGE OR REALLY JUST GO AWAY AND WE 
#  JUST SHOW A ROUGH OUTLINE OF THE DATA EXTENT FOR SELECTION
map_traces = [ go.Scattermapbox(
            lat=[64.8164],
            lon=[-147.8635],
            mode='markers+text',
            marker=go.Marker(size=12, color='rgb(140,86,75)'),
            text=['Fairbanks International Airport'],
            hoverinfo='text' ) ] + [ go.Scattermapbox(
            lat=lats.ravel(),
            lon=lons.ravel(),
            text=['{}-{}'.format(i,j) for i,j in zip(lons, lats)],
            mode='markers',
            marker=go.Marker(size=5, color='rgb(140,86,75)') ) ]

mapbox_config = dict(accesstoken=mapbox_access_token,
                        bearing=0,
                        pitch=0,
                        zoom=2,
                        center=dict(lat=64,
                                    lon=-147),
                        layers=[ dict( sourcetype='geojson',
                                        source=json.loads(open(shapes,'r').read()),
                                        type='fill',
                                        color='rgba(163,22,19,0.1)',
                                        below=0 )]
                        )

map_layout = go.Layout( autosize=True,
                    hovermode='closest',
                    mapbox=mapbox_config,
                    showlegend=False,
                    margin = dict(l = 0, r = 0, t = 0, b = 0)
                    )

map_figure = go.Figure( dict(data=map_traces, layout=map_layout) )
 
title_markdown = '''
#### CFSv2 Derived Monthly Snow Events
'''

bottom_markdown = '''
###### >1" Snow Total Counts
'''

# NOW LETS BUILD THE HTML NEEDED TO DISPLAY THE PAGE
app.layout = html.Div([
            html.Div([
                html.H3(html.Div([html.Span(dcc.Markdown( children=title_markdown ), style={'color':'#0489B1', 'font-family':'Calibri'})], className='row')),
                html.Div([ 
                    html.Div([ html.Label('Choose Year', style={'font-weight':'bold'}),
                                     dcc.Dropdown( id='year-dropdown',
                                        options=[ {'label':str(year), 'value':year} for year in range( 2011, 2018 ) ],
                                        value=2017,
                                        multi=False,
                                        )] ),
                    ]),
                html.Div([
                html.Div([
                    html.Div([ dcc.Graph( id='main-graph' ) ], className='eight columns'),
                    html.Div([ dcc.Graph( id='main-map', figure=map_figure )], className='four columns')
                    ], className='row')]),
                ]),
            # html.Div([dcc.Markdown( children=bottom_markdown )])
            ])

def get_closest_value_1d( arr, v ):
    return arr[ (np.abs(arr - v)).argmin() ]

def get_data( x,y, dat ):
    ''' extact the array of values beneath the given xy coordinate.'''
    ind = np.where( (dat.lon == get_closest_value_1d(dat.lon.data.ravel(), x)) & (dat.lat == get_closest_value_1d(dat.lat.data.ravel(), y)) )
    return ( list(dat.month.data), list(dat.data[ ..., int(ind[0]), int(ind[1])]))

@app.callback(Output('main-graph','figure'), [Input('main-map','hoverData'),Input('year-dropdown', 'value')])
def update_graph( clickdata, year ):
    print(clickdata)
    print('year {}'.format(year))
    if clickdata is not None:
        lo,la = (clickdata['points'][0]['lon'], clickdata['points'][0]['lat'])
        if lo < 0:
            lo = lo + 360
            print('lo:{}'.format(lo))
        months, dat = get_data( lo, la, clim_counts )
        months, dat_std = get_data( lo, la, clim_deviation )
        months_re_sort_idx = [ int(np.where( np.array(months).astype(int) == i )[0]) for i,j in zip([9,10,11,12,1,2,3,4,5],['Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May'])]
        print(months_re_sort_idx)
        dat = np.array(dat)[months_re_sort_idx].tolist()

        forecast = forecast_lookup[ int(year) ]
        months, forecast_dat = get_data( lo, la, forecast )
        months_re_sort_idx = [ int(np.where( np.array(months).astype(int) == i )[0]) for i,j in zip([9,10,11,12,1,2,3,4,5],['Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May'])]
        forecast_dat = np.array(forecast_dat)[months_re_sort_idx].tolist()

        months = ['Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May']

        return {'data':[ go.Bar( x=months, 
                        y=dat, 
                        name='28-year Avg', 
                        error_y=dict(
                                type='data',
                                symmetric=False,
                                array=dat_std,
                                arrayminus=np.zeros_like(dat_std),
                                visible=True ),

                        # line=dict(color='black', width=2 ),
                        # mode='lines'
                        ),
                        go.Bar( x=months,
                            y=forecast_dat,
                            name=str(year), 
                            # line=dict(color='red', width=2 ),
                            # mode='lines',
                            )],
                        # go.Scatter( x=months,
                        #     y=forecast_dat,
                        #     name=str(year), 
                        #     line=dict(color='red', width=2 ),
                        #     mode='lines' ) ],
                'layout':{'title':'Snow Total Counts >1"', 'xaxis':dict(title='Months'), 'yaxis':dict(title='Number of Snow Events')} }

@app.callback(
    Output('click-data', 'children'),
    [Input('main-map', 'clickData')])
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


if __name__ == '__main__':
    app.run_server( debug=True )



