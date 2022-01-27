import streamlit as st

import altair as alt

import seaborn as sns

# import plotly.express as px

import numpy as np
import pandas as pd
import datetime as dt
from timeit import default_timer as timer
import calendar
from datetime import date, timedelta,time
import xlrd
import openpyxl

import dask
import dask.dataframe as dd
#
# import config
# import simplejson
# import os
#
from bs4 import BeautifulSoup
import requests
import re
#
from zipfile import ZipFile, BadZipfile, is_zipfile
from io import BytesIO

from streamlit import caching

import io

st.set_page_config('Pricing data',layout='wide')


st.title('Australia NEM electricity spot price history')

col1,col2,col3 = st.columns(3)
with col1:
    st.image('https://media.istockphoto.com/photos/bulbs-and-money-picture-id1305968783?b=1&k=20&m=1305968783&s=170667a&w=0&h=V0sdlfRSf0gBRy2aKmm4CLUukL7HylxhP-ekHcsTvqI=')
with col2:
    st.image('https://media.istockphoto.com/photos/electricity-prices-picture-id1304219166?b=1&k=20&m=1304219166&s=170667a&w=0&h=YMkEEB7_9IgQ0XqYah3SUnaeUjRC0_WxkAQhYKPUpbY=')
with col3:
    st.image('https://media.istockphoto.com/photos/light-bulb-with-many-coins-on-the-table-picture-id1186035072?b=1&k=20&m=1186035072&s=170667a&w=0&h=6KJwEYeJjzISR-26ifZbc8hyHt0HYNubDzorVv3c1RI=')

# Get historical data - updated every 7 days

@st.cache(suppress_st_warning=True,allow_output_mutation=True,ttl=604800)
def gethistory():
    path = 'https://nemweb.com.au'
    url = 'https://nemweb.com.au/Reports/Archive/Public_Prices/'

    #List of zip files

    r= requests.get(url)

    soup = BeautifulSoup(r.content,'lxml')

    links = soup.find_all('a')
    ziplist = [path+link['href'] for link in links if 'zip' in link['href']]

    start = timer()
    bigdf = pd.DataFrame()

    i=1

    #Get total number of files

    totalfiles = 395

    st.markdown('This process will take around 3 minutes to complete.')

    placeholder = st.empty()
    progholder = st.empty()
    mybar = st.progress(0)

    # For each zip

    for zip in ziplist:
        r = requests.get(zip)
        z = ZipFile(io.BytesIO(r.content))
        filelist = z.namelist()

        for file in filelist:

            dayfile = z.open(file)
        # df = dd.read_csv(file,usecols=['SETTLEMENTDATE','REGIONID','RRP'],header=1,compression='zip',dtype={'RRP':'object'})
            df = pd.read_csv(dayfile,usecols=['SETTLEMENTDATE','REGIONID','RRP'],header=1,compression='zip',dtype={'RRP':'object'})
            bigdf = pd.concat([bigdf,df])

            with placeholder:
                st.write('File #{0} complete '.format(i)+'/ '+str(totalfiles)+'.')
            with progholder:
                pct_complete = '{:,.2%}'.format(i/totalfiles)
                st.write(pct_complete,' complete.' )
                try:
                    mybar.progress(i/totalfiles)
                except:
                    mybar.progress(1)
            i=i+1

    bigdf = bigdf.dropna()
    bigdf = bigdf[bigdf['SETTLEMENTDATE']!='SETTLEMENTDATE']
    bigdf.columns = ['Time','State','Price']
    bigdf['Price'] = pd.to_numeric(bigdf['Price'],errors='coerce')
    bigdf['Time'] = pd.to_datetime(bigdf['Time'],errors='coerce')

    end = timer()

    with st.expander('Show data head'):
        st.write(bigdf.head(20))

    st.write('Actual completion time was '+str('{:,.2f}'.format(end - start))+' seconds.')

    return bigdf

bigdf = gethistory()

# bigdf = pd.read_csv('bigdf.csv').iloc[:,1:]

# bigdf.to_csv('bigdf.csv')


########################## TO DO #################################
# DONE Apply the negative price interval formatting to the whole table, not to columns
# DONE Put big file into cache to gain time (update every month)
# Look for a Dask DataFrame solution (optional)
# Add metrics on prices: number of negative intervals, average, daytime / whole day, distrib across the day...
# DONE Add cool pictures
########################## TO DO #################################



# Get current data

@st.cache(suppress_st_warning=True,allow_output_mutation=True,ttl=7200)
def getprices():

    path = 'https://nemweb.com.au'
    url = 'https://nemweb.com.au/Reports/Current/Public_Prices/'

    r= requests.get(url)

    #List of zip files

    soup = BeautifulSoup(r.content,'lxml')

    links = soup.find_all('a')
    ziplist = [path+link['href'] for link in links if 'zip' in link['href']]

    df = dd.read_csv(ziplist,usecols=['SETTLEMENTDATE','REGIONID','RRP'],header=1,compression='zip',dtype={'RRP':'object'})
    df = df.compute()
    df = df.dropna()
    df = df[df['SETTLEMENTDATE']!='SETTLEMENTDATE']
    df.columns = ['Time','State','Price']
    df['Price'] = pd.to_numeric(df['Price'],errors='coerce')
    df['Time'] = pd.to_datetime(df['Time'],errors='coerce')

    return df


@st.cache(suppress_st_warning=True,allow_output_mutation=True)
def consoldata():
    df = getprices()
    df = pd.concat([bigdf,df])
    df = df.drop_duplicates()

    df = df.sort_values('Time',ascending=False)
    df['Year'] = pd.DataFrame(df['Time']).apply(lambda x: x.dt.year)
    df['Month'] = pd.DataFrame(df['Time']).apply(lambda x: x.dt.month)
    df['Day'] = pd.DataFrame(df['Time']).apply(lambda x: x.dt.day)
    df['Time of Day'] = df['Time'].dt.strftime('%H:%M')
    periodnb = df['Time of Day'].sort_values(ascending = True)
    periodnb = periodnb.unique().tolist()
    df['Period Number'] = df['Time of Day'].apply(lambda x:periodnb.index(x)+1)
    df.set_index('Time',inplace=True)
    prices = df.pivot_table(index='Time',columns=['Year','Month','State'],values='Price')
    return df,prices

df,prices = consoldata()



####################################################################

st.header('Proportion of price intervals below threshold')

with st.form(key='thresholds'):
    col1,col2 = st.columns(2)
    with col1:
        thres = st.number_input('Please enter threshold',min_value=None,max_value=None,value=0)
    with col2:
        daytime = st.checkbox('Daytime only')

    submit_button = st.form_submit_button(label='Submit')



negatives = pd.DataFrame(prices.where(prices<thres).count()/prices.count())
# st.write(negatives.style.format('{:.2%}'))

negatives.reset_index(inplace=True)
negatives.columns = ['Year','Month','State','Pct Negative']
negatives = negatives.pivot_table(index='State',columns=['Year','Month'],values='Pct Negative')

cm = sns.light_palette('red',as_cmap=True)

st.write(negatives.style.background_gradient(cmap=cm,axis=None).format('{:.2%}'))

prices = df.pivot_table(index='Time',columns='State',values='Price')


distrib = prices.describe([0.01,0.025,0.05,0.1,0.25,0.3,0.4,0.5,0.6,0.75,0.9,0.95,0.975,0.99])



st.header('Price distribution')

st.write(distrib)

def convert_df(df):
    return df.to_csv().encode('utf-8')

csv = convert_df(distrib)

st.download_button(label = 'Download price history as csv',data=csv,file_name='price_history.csv',mime='text/csv')

####################################################################

st.subheader('Price history')


freqlist = ['5min','30min','1H','3H','6h','12H','D','W','M','Q']
statelist = ['All','NSW1','QLD1','VIC1','SA1','TAS1']

with st.form(key='myform'):

    col1,col2 = st.columns(2)
    with col1:
        selectfreq = st.selectbox('Select frequency',freqlist,index=4)
        daytime = st.checkbox('Daytime only 8:00 to 18:00')
    with col2:
        selectstate = st.selectbox('Select state',statelist,index=0)
    submit_button = st.form_submit_button(label='Submit')



if selectstate != 'All':
    df = df[df['State']==selectstate]

if daytime:
    df = df[(df['Period Number']>96)&(df['Period Number']<218)]

df = df.groupby('State').resample(selectfreq).mean().reset_index()


highlight = alt.selection(type='interval',bind='scales',encodings=['x','y'])
fig = alt.Chart(df).mark_line().encode(alt.X('Time:T'),alt.Y('Price:Q',scale=alt.Scale(domain=(-100,200))),color='State:N',tooltip=[
      {"type": "quantitative", "field": "Price"},
      {"type": "temporal", "field": "Time"},
      {"type": "nominal", "field": "State"}]).add_selection(highlight)
st.altair_chart(fig,use_container_width=True)


#

st.subheader('Price distribution')

with st.form(key='myform2'):
    binnumb = st.number_input('Enter bin number',min_value=0,value=2000,key=0)
    submit_button = st.form_submit_button(label='Submit')


if selectstate == 'All':
    highlight = alt.selection(type='interval',bind='scales',encodings=['x','y'])
    fig = alt.Chart(df).transform_joinaggregate(total='count(*)').transform_calculate(pct='1/datum.total').mark_bar(opacity=0.5).encode(alt.X('Price:Q',scale=alt.Scale(domain=(-100,200)),bin=alt.BinParams(maxbins=binnumb)),alt.Y('sum(pct):Q',stack=None,axis=alt.Axis(format='%')),color='State:N',tooltip=[
          {"type": "quantitative", "field": "Price"}]).add_selection(highlight)
    st.altair_chart(fig,use_container_width=True)
else:
    highlight = alt.selection(type='interval',bind='scales',encodings=['x','y'])
    fig = alt.Chart(df).transform_joinaggregate(total='count(*)').transform_calculate(pct='1/datum.total').mark_bar(opacity=0.7).encode(alt.X('Price:Q',scale=alt.Scale(domain=(-100,200)),bin=alt.BinParams(maxbins=binnumb)),alt.Y('sum(pct):Q',axis=alt.Axis(format='%')),color='State:N',tooltip=[
          {"type": "quantitative", "field": "Price"}]).add_selection(highlight)
    st.altair_chart(fig,use_container_width=True)

#Average prices per time

st.subheader('Price distribution across daily time intervals')

pricesbytime = df.groupby(by=['State','Period Number'])['Price'].mean()
pricesbytime = pd.DataFrame(pricesbytime)
pricesbytime.reset_index(inplace = True)

with st.form(key='myform3'):
    binnumb = st.number_input('Enter bin number',min_value=0,value=50,key=1)

    submit_button = st.form_submit_button(label='Submit')


if selectstate =='All':
    fig = alt.Chart(pricesbytime).mark_bar(opacity=0.5).encode(alt.X('Period Number:N',bin=alt.BinParams(maxbins=binnumb)),alt.Y('Price:Q',scale=alt.Scale(domain=(0,200)),stack=None),color='State:N',tooltip=[
          {"type": "quantitative", "field": "Price"},
          {"type": "nominal", "field": "State"},
          {"type": "nominal", "field": "Period Number"}]).add_selection(highlight)
    st.altair_chart(fig,use_container_width=True)
else:
    fig = alt.Chart(pricesbytime).mark_bar(opacity=0.7).encode(alt.X('Period Number:N',bin=alt.BinParams(maxbins=binnumb)),alt.Y('Price:Q',scale=alt.Scale(domain=(0,200))),color='State:N',tooltip=[
          {"type": "quantitative", "field": "Price"},
          {"type": "nominal", "field": "State"},
          {"type": "nominal", "field": "Period Number"}]).add_selection(highlight)
    st.altair_chart(fig,use_container_width=True)



###### End Filter prices ####
