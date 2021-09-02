import json
import os
from pathlib import Path
from platform import system
from glob import glob

# normalizer
NAMING_SW_CSV = os.path.join(os.path.dirname(__file__), 'resources', 'naming_stopwords.csv')
SW_AFTER_FILTER_CSV = os.path.join(os.path.dirname(__file__), 'resources', 'stopwords_after_filter.csv')
COMMON_TITLES_CSV = os.path.join(os.path.dirname(__file__), 'resources', 'common_titles.csv')
COURT_JUDGES = os.path.join(os.path.dirname(__file__), 'resources', 'israel_court_judges.txt')
# classifier
NGRM_JSON = os.path.join(os.path.dirname(__file__), 'resources', 'category_to_ngram.json')
CLASSIFIERS = os.path.join(os.path.dirname(__file__), 'resources', 'models')

# return current path if didn't got oldPath and remove N folders from the end
def get_path(old_path=None, n=0, end_sep=True):
    curr_path = Path().parent.absolute() if old_path is None else old_path  # get curr path in not provided
    split_path = str(curr_path).split('/')  # split path to folders
    n = -n if n > 0 else len(split_path)  # fix N for proper slice
    new_path = f"/".join(split_path[:n])  # rejoin wanted folders into path
    return new_path + "/" if end_sep else new_path  # path + sep if true else path


# input - if dirName is string create folder at current path else create all the path
def create_dir(dir_name):
    try:
        if not os.path.exists(dir_name):  # Create target Directory if don't exist
            os.mkdir(dir_name)
            # logger.info(f"Creating dir with the name: {dir_name}")
    except FileNotFoundError as _:
        n = 1 if system() == 'Windows' else 2  # in case system is not windows - splitPath will have sep at the end
        create_dir(get_path(dir_name, n=n))  # create parent target folder
        create_dir(dir_name)  # create target folder


def get_all_files(folder_name):
    return [f for f in glob(folder_name + "/*.json")]


def get_names(files):
    names = []
    for file in files:
        s = file.split("/")
        n = s[len(s) - 1]
        names.append(n)
    return names


def get_cases(path_):
    files = get_all_files(path_)
    return get_names(files)


def file_exists(file_name):
    if os.path.isfile(file_name):
        return True
    return False


def save_data(data, file_name=None, file_path=None):
    with open(file_path + file_name, 'w') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)
