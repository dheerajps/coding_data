import pandas as pd
import glob, os
import csv

yield_files = glob.glob('../wx_data/*.txt')
yield_list = []
file_names = []
sorted(yield_files)
missing = -9999

def calculate_number_of_days(file_name):
    
    weather_df = pd.read_table(file_name, sep = "\t", header=None, skipinitialspace=True, names=["DATE","MAX","MIN","PPT"])
    return len(weather_df[(weather_df['MAX']!= missing) & (weather_df['MIN']!= missing) & (weather_df['PPT'] == missing)])

for yfile in yield_files:
    file_names.append(os.path.basename(yfile))  #get only the filename
    days = calculate_number_of_days(yfile)      #get the number of days according to the given condition
    yield_list.append(days)

f = open('../answers/MissingPrcpData.out', 'w') #write into the file by zipping both the lists
try:
    writer = csv.writer(f,delimiter="\t")
    writer.writerows(zip(file_names, yield_list))
finally:
    f.close()
