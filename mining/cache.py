from urllib.request import urlopen
from os import mkdir
from os.path import join, isdir, isfile

CACHE_DIR = 'edgar-downloads/'

def download(path):
    '''Download a copy of a file and cache it.
    If the file has already been downloaded into the cache, use that instead.
    You are responsible for closing the file.'''
    try:
        return cache_get(path)
    except NotFound:
        cache_put(path, download_no_cache(path))
        return download_cached(path)

def download_no_cache(path):
    '''Download a file without attempting to read or write from the cache'''
    print('cache.py: downloading {path}'.format(**locals()))
    print('cache.py: done downloading {path}'.format(**locals()))
    url_path = 'ftp://ftp.sec.gov/' + path
    return urlopen(url_path.format(**locals())).read()

def get(path):
    '''Attempt to retrieve file from cache, raising NotFound if not found.
    You are responseible for closing the file, if it is returned'''
    if not isdir(CACHE_DIR):
        mkdir(CACHE_DIR)
    cache_path = join(CACHE_DIR, path.replace('/', '__'))
    if isfile(cache_path):
        return open(cache_path, 'rb')
    else:
        raise NotFound('Unable to find {path}'.format(**locals()))

def put(path, file):
    '''Store file in the cache for path'''
    if not isdir(CACHE_DIR):
        mkdir(CACHE_DIR)
    cache_path = join(CACHE_DIR, path.replace('/', '__'))
    with open(cache_path, 'wb') as outfile:
        for line in file:
            outfile.write(line)

class NotFound(Exception):
    pass