import pandas as pd
import datetime as dt
import numpy as np
import os,csv
import glob
import argparse

def get_consolidated_data(station_file):
    
    station_df = pd.read_table(station_file, sep = "\t", header=None, skipinitialspace=True, names=["DATE","MAX","MIN","PPT"])
    station_df["YEAR"] = pd.to_datetime(station_df["DATE"], format = "%Y%m%d").dt.year # get the filename and year as two columns
    station_df["FILE"] = os.path.basename(station_file)
    station_df = station_df.replace(-9999,np.nan) #helps in finding average
    station_consolidated_df = station_df.groupby(['FILE','YEAR']).agg({'MAX': 'mean','PPT':'sum','MIN':'mean'})
    station_consolidated_df = station_consolidated_df.fillna(-9999.00).round(2) #fill in missing values with specified NaN and round to 2 decimal
    return station_consolidated_df

def main():

    parser = argparse.ArgumentParser(description='weather program2 ')
    parser.add_argument('-i','--input', help='Input Directory path. eg: path/to/input/files/',required=True)
    parser.add_argument('-o','--output', help='Output filename name. eg:path/to/output/filename.out',required=True)
    args =  parser.parse_args()
    if not os.path.isdir(args.input):
        print "INPUT DIRECTORY ' {} ' NOT PRESENT".format(args.input)
        exit()
    yield_files = glob.glob(args.input + "*.txt")

    if os.path.exists(args.output):
        try:
            os.remove(args.output) #delete if file already exists
        except OSError:
            pass

    for station_file in yield_files:
        df = get_consolidated_data(station_file)      #get consolidated data frame after grouping and aggregating
        with open(args.output,'a') as f:
            df.to_csv(f, sep = '\t', header = False)
    print "OUTPUT WRITTEN TO--->",args.output
    return

if __name__ == '__main__':
    main()

