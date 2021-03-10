import os
import shutil
import time
import glob

files = glob.glob('../datasets/stock_data/*')
for f in files:
    os.remove(f)

initial_files = glob.glob('../datasets/original_stock_data/*_2017.csv')

for f in initial_files:
    shutil.copy(f, '../datasets/stock_data/')

input('Press any keys to continue')

deleted_files = glob.glob('../datasets/stock_data/*')

for f in deleted_files:
    os.remove(f)

new_file = glob.glob('../datasets/original_stock_data/*_2018.csv')[0]

shutil.copy(new_file, '../datasets/stock_data/')


