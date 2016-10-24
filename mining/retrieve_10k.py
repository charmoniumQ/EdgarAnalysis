from os import mkdir
from os.path import join
from re import finditer
from cache import download

def parse_SGML(SGML):
    # matches <FILENAME> anything </
    # or <FILENAME>  anything \n
    # followed by <TEXT> contesnts </TEXT>
    # or <TEXT>\nContents\n</Text>
    # where contents are taken with the trailing newline (but not leading one)
    file_pattern = b'(?s)<FILENAME>(.*?)[\n<].*?<TEXT>\n?(.*?)</TEXT>'
    contents = {}
    for match in finditer(file_pattern, SGML):
        filename = match.group(1)
        file_contents = match.group(2)
        contents[filename] = file_contents
    return contents
    # test on ~/Downloads/thing/0001437749-16-038804.txt

def extract_to_disk(directory, dct):
    mkdir(directory)
    for fname, contents in dct.items():
        dfname = join(directory, fname)
        with open(dfname, 'wb') as f:
            f.write(contents)

if __name__ == '__main__':
    a = download('edgar/data/1503518/0001047469-16-015101.txt')
    c = parse_SGML(a.read())
    a.close()
    extract_to_disk(b'output', c)
