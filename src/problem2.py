import pandas as pd
import datetime as dt
import numpy as np
import os,csv
import glob

yield_files = glob.glob("../wx_data/*.txt")
output_file = '../answers/YearlyAverages.out'
if os.path.exists(output_file):
    try:
        os.remove(output_file) #delete if file already exists
    except OSError:
        pass

def get_consolidated_data(station_file):
    
    station_df = pd.read_table(station_file, sep = "\t", header=None, skipinitialspace=True, names=["DATE","MAX","MIN","PPT"])
    station_df["YEAR"] = pd.to_datetime(station_df["DATE"], format = "%Y%m%d").dt.year # get the filename and year as two columns
    station_df["FILE"] = os.path.basename(station_file)
    station_df = station_df.replace(-9999,np.nan) #helps in finding average
    station_consolidated_df = station_df.groupby(['FILE','YEAR']).agg({'MAX': 'mean','PPT':'sum','MIN':'mean'})
    station_consolidated_df = station_consolidated_df.fillna(-9999.00).round(2) #fill in missing values with specified NaN and round to 2 decimal
    return station_consolidated_df

for station_file in yield_files:
    df = get_consolidated_data(station_file)      #get consolidated data frame after grouping and aggregating
    with open(output_file,'a') as f:
        df.to_csv(f, sep = '\t', header = False)
