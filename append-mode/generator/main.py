import os
import shutil
import time
import glob

files = glob.glob('../datasets/stock_data/*')
for f in files:
    os.remove(f)

files = glob.glob('../datasets/original_stock_data/*')

for f in files:
    shutil.copy(f, '../datasets/stock_data/')
    time.sleep(20)















