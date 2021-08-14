import os
from glob import glob
from Normalizer import Normalizer

ROOT_DIR = os.path.abspath(os.curdir)
OUTPUT_DIR = ROOT_DIR + '/cases/'
PARSED_SUCCESS_DIR = OUTPUT_DIR + 'parsed/success/'


def get_all_files(folder_name):
    return [f for f in glob(folder_name + "/*.json")]


def get_names(files):
    names = []
    for file in files:
        s = file.split("/")
        n = s[len(s)-1]
        names.append(n)
    return names


def get_cases(path_):
    files = get_all_files(path_)
    return get_names(files)


def normalize():
    print('enricher::normalize')
    enricher = Normalizer('settings.json')
    cases = get_cases(PARSED_SUCCESS_DIR)
    for case in cases:
        enricher.Enrich(case)
    # enricher.Enrich('1339-12-1.json')
    # pass

