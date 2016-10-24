from itertools import islice
from retrieve_index import get_index

# get the master index for the third quarter of 2016
# the first time you run this, it will take a long time to download the data
# every subsequent time, it will use the cached data
index = get_index(2016, 3, 'master')
for i, row in zip(range(0, 10), index):
    print(row['Company Name'])
