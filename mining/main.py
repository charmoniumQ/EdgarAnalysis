from cache import download
from itertools import islice
from retrieve_index import get_index
from retrieve_10k import SGML_to_files, get_risk_factors

form_index = get_index(2016, 3, 'form')
while next(form_index)['Form Type'] != '10-K':
    pass
print("Press enter for another risk factor. Press 'q' to quit.")
while input() == '':
    index_info = next(form_index)
    path = index_info['Filename']
    print('Risk factors for ' + index_info['Company Name'])
    print('-'*70)
    print(get_risk_factors(path)[:400])
    print('\n')
