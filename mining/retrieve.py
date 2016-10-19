from os.path import isfile
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from re import sub
from itertools import islice
from datetime import datetime

# ftp://ftp.sec.gov/edgar/daily-index/2016/QTR3/
# ftp://ftp.sec.gov/edgar/full-index/2016/QTR3/
# ftp://ftp.sec.gov/edgar/monthly/

indexes = {'company', 'master', 'form'}

def download_index_uncompressed(year, qtr, index, filename):
    '''Download the uncompressed .idx file. This takes a very long time'''

    # download the index file
    url = 'ftp://ftp.sec.gov/edgar/full-index/{year}/QTR{qtr}/{index}.idx'
    raw_file = urlopen(url.format(**locals())).read()
    with open(filename, 'wb') as outfile:
        outfile.write(raw_file)

def download_index(year, qtr, index, filename):
    '''Download the zip file and extract the .idx file from it'''

    # download the zip file
    url = 'ftp://ftp.sec.gov/edgar/full-index/{year}/QTR{qtr}/{index}.zip'
    raw_compressed_file = urlopen(url.format(**locals())).read()

    # unzip the file
    with ZipFile(BytesIO(raw_compressed_file), 'a') as index_zip:
        # extract {index}.idx where {index} gets replaced with
        # 'company', 'master', or 'form'
        with index_zip.open('{index}.idx'.format(**locals())) as index_file:
            with open(filename, 'wb') as outfile:
                for line in index_file:
                    outfile.write(line)

def normalize(line):
    '''Returns an list of elements found on each line (uppercased)'''
    line = line.decode() # turns binary string into ascii string
    line = line.strip() # removes trailing newline and leading spaces
    line = sub(' {2,}', '|', line) # 'a    b    c' -> 'a|b|c'
    return line.split('|') # 'a|b|c' -> ['a', 'b', 'c']

types = {
    # name_of_type: funciton_which_converts_to_that_type
    'Form Type': str,
    'Company Name': str,
    'CIK': int,
    'Date Filed': lambda s: datetime.strptime('2016-09-08', '%Y-%m-%d').date(),
    'Filename': str,
}
aliases = {
    'File Name': 'Filename'
}

def parse_index(filename):
    '''Reads the filename and parses it

(provided the file came from download_index or download)'''
    with open(filename, 'rb') as index_file:
        # the headings occur in a different order based on what index you are
        # looking at (eg. FORM TYPE comes first in the form.idx)
        heading_line = next(islice(index_file, 8, 9)) # get the 8th line
        col_headings = normalize(heading_line)
        col_headings = [word if word not in aliases else aliases[word] for word in col_headings ]
        next(index_file) # skip the line with dashes
        for line in index_file:
            elems = normalize(line)
            # convert type of elem using the function associated with its column heading
            yield {heading: types[heading](elem) for heading, elem in zip(col_headings, elems)}

cache_path = 'data/{year}_QTR{qtr}_{index}.idx'
def get_index(year, qtr, index):
    '''Download the given index and cache it to disk.
If a cached copy is available, use that instead.
Caches are stored in data/ directory

    year: a string or int 4-digit year
    qtr: a string or int between 1 and 4
    index: either 'company', 'master', or 'form'
    filename: save the result as the given filename
    See ftp://ftp.sec.gov/edgar/full-index/2016/QTR3/'''

    if index not in indexes:
        raise ValueError('Cannot download the index {index}, must be {indexes!s}'.format(**locals()))
    year = int(year)
    qtr = int(qtr)
    if not (1 <= qtr <= 4):
        raise ValueError('Quarter must be between 1 and 4')

    filename = cache_path.format(**locals())
    if not isfile(filename):
        download_index(year, qtr, index, filename)
    yield from parse_index(filename)

__all__ = ['get_index']
