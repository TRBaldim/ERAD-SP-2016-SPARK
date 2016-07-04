'''
Created on 03/07/2016

@author: thiago
'''

import time

import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter
import matplotlib.dates as mdates
from datetime import datetime

def plotData(rddData):
    
    toSecs = lambda d: (d - datetime(1970, 1, 1)).total_seconds()
        
    dates = [i[0] for i in rddData]
    temps = [i[1] for i in rddData]
    
    years = YearLocator()   
    months = MonthLocator() 
    yearsFmt = DateFormatter('%Y')
    
    fig, ax = plt.subplots()
    ax.plot_date(dates, temps, '-')
    
    locator = mdates.MinuteLocator(byminute=[0, 30])
    locator.MAXTICKS = 1500
    
    ax.xaxis.set_major_locator(years)
    ax.xaxis.set_major_formatter(yearsFmt)
    ax.xaxis.set_minor_locator(years)
    ax.autoscale_view()
    
    temperatures = lambda x: '%.2f' % x
    
    #ax.fmt_xdata = DateFormatter('%Y-%m-%d')
    ax.fmt_ydata = temperatures
    ax.grid(True)   
    
    fig.autofmt_xdate()
    plt.show()