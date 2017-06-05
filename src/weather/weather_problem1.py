import pandas as pd
import glob, os
import csv
import argparse

def calculate_number_of_days(file_name):

    missing = -9999
    weather_df = pd.read_table(file_name, sep = "\t", header=None, skipinitialspace=True, names=["DATE","MAX","MIN","PPT"])
    return len(weather_df[(weather_df['MAX']!= missing) & (weather_df['MIN']!= missing) & (weather_df['PPT'] == missing)])

def main():

    parser = argparse.ArgumentParser(description='program1 ')
    parser.add_argument('-i','--input', help='Input Directory path. eg: path/to/input/files/',required=True)
    parser.add_argument('-o','--output', help='Output filename name. eg:path/to/output/filename.out',required=True)
    args =  parser.parse_args()
    if not os.path.isdir(args.input):
        print "INPUT DIRECTORY ' {} ' NOT PRESENT".format(args.input)
        exit()
    yield_files = glob.glob(args.input + "*.txt")
    yield_list = []
    file_names = []

    for yfile in yield_files:
        file_names.append(os.path.basename(yfile))  #get only the filename
        days = calculate_number_of_days(yfile)      #get the number of days according to the given condition
        yield_list.append(days)
    
    with open(args.output, 'w') as f: #write into the file by zipping both the lists
        writer = csv.writer(f,delimiter="\t")
        writer.writerows(zip(file_names, yield_list))
        print "OUTPUT WRITTEN TO-->",args.output

if __name__ == '__main__':
    main()

