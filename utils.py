import os
from glob import glob

ROOT_DIR = os.path.abspath(os.curdir)
OUTPUT_DIR = ROOT_DIR + '/cases/'
PARSED_SUCCESS_DIR = OUTPUT_DIR + 'parsed/success/'
NORMALIZED_SUCCESS_DIR = OUTPUT_DIR + 'normalized/success/'
NORMALIZED_FAILED_DIR = OUTPUT_DIR + 'normalized/failed/'
CSV_PATH = ROOT_DIR + '/legal_personal.csv'


# input - if dirName is string create folder at current path else create all the path
def create_dir(dir_name):
    try:
        if not os.path.exists(dir_name):  # Create target Directory if don't exist
            os.mkdir(dir_name)
            # logger.info(f"Creating dir with the name: {dir_name}")
    except FileNotFoundError as _:
        n = 1 if os.system() == 'Windows' else 2  # in case system is not windows - splitPath will have sep at the end
        create_dir(os.get_path(dir_name, n=n))  # create parent target folder
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
    if os.path.exists(file_name):
        return True
    return False

    # for elem in os.listdir(file_name):
    #     if os.path.basename(elem) == 'legal_personal.csv':
    #         return True
    #
    # return False
