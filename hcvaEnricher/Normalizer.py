import os
import csv
import json
import queue
import string
import re
import traceback
import jellyfish
import pandas as pd
from pathlib import Path
from itertools import repeat
import hcvaEnricher.utils as utils
from hcvaEnricher.Enricher import Enricher


"""

The normalizer is in charge of normalizing the attorney and judges names
It receives as a parameter the files names and the current elk connection
It uses the determine method in order to determine whether a given name exists on the database or not - 
    if the name exists - it returns the name after normalization and advance occurrences by one
    if the name does not exists: it adds the name before normalization and after and then advance the occurrences
Important notice : the functioned mentioned above is NOT and Injective function !! it is very likely that a couple of 'before' names will lead to same 'after'

"""


class Normalizer(Enricher):

    def __init__(self, settings, csv_path: str = None):
        super().__init__(settings.PARSED_SUCCESS_DIR, settings.NORMALIZED_SUCCESS_DIR)
        self.legal_dictionary = dict()
        self.counter = 0
        self.error_counter = 0
        self.initialize_normalizer(csv_path)

    def get_flag(self, csv_path: str):
        # implement non existing initialization logic here:
        # 0 - with existing files on local drive
        # 1 - with existing files on elk db
        # 2 - with no existing verdicts at all
        if csv_path and utils.file_exists(csv_path):
            return 'csv'
        else:
            return 'empty'

    def initialize_normalizer(self, csv_path: str):
        flag = self.get_flag(csv_path)
        if flag == 'csv':
            with open(csv_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file, delimiter=',')
                before = [row[0] for row in reader]
                after = [row[1] for row in reader]
                self.legal_dictionary = dict(zip(before, after))
        elif flag == 'elk':
            pass
        elif flag == 'empty':
            pass
        else:
            raise ValueError('invalid initializing parameter. must be of type: csv, elk or empty')

    def make_doc_details_values_list(self, input_directory: str, output_directory: str):
        """
        Pulls all of the naming existing on the verdicts jsons and stores them in sets.
        Get the verdicts location as input directory and the output folder which will
        store the sets as txt file with the names seperated by '*'.
        :param input_directory - path as a string
        :param output_directory - path as a string
        :returns all the sets created one by one.
        """
        # set initialization
        judge_set = set()
        petitioner_set = set()
        defendse_set = set()
        petitioner_attorney_set = set()
        defendse_attorney_set = set()

        # populating the sets
        for verdict in os.listdir(input_directory):
            with open(verdict, "r", encoding="utf-8") as verdict_file:
                verdict_json = json.load(verdict_file)

                judges = verdict_json["Doc Details"]["לפני"]
                if judges is not None:
                    for judge in judges:
                        judge_set.add(judge)

                petitioners = verdict_json["Doc Details"]["העותר"]
                if petitioners is not None:
                    for petitioner in petitioners:
                        petitioner_set.add(petitioner)

                defense = verdict_json["Doc Details"]["המשיב"]
                if defense is not None:
                    for defendee in defense:
                        defendse_set.add(defendee)

                petitioner_attorneys = verdict_json["Doc Details"]["בשם העותר"]
                if petitioner_attorneys is not None:
                    for petitioner_attorney in petitioner_attorneys:
                        petitioner_attorney_set.add(petitioner_attorney)

                defendse_attorney = verdict_json["Doc Details"]["בשם המשיב"]
                if defendse_attorney is not None:
                    for def_attorney in defendse_attorney:
                        defendse_attorney_set.add(def_attorney)

        # creating the files
        cur_path = output_directory + "/" + 'judges.txt'
        with open(cur_path, "w", encoding="utf-8") as judges_file:
            for judge in judge_set:
                judges_file.write(judge + "*")

        cur_path = output_directory + "/" + 'petitioner.txt'
        with open(cur_path, "w", encoding="utf-8") as petitioner_file:
            for petitioner in petitioner_set:
                petitioner_file.write(petitioner + "*")

        cur_path = output_directory + "/" + 'defense.txt'
        with open(cur_path, "w", encoding="utf-8") as defense_file:
            for defense in defendse_set:
                defense_file.write(defense + "*")

        cur_path = output_directory + "/" + 'petitioner_attorneys.txt'
        with open(cur_path, "w", encoding="utf-8") as petitioner_attorney_file:
            for petitioner_attorney in petitioner_attorney_set:
                petitioner_attorney_file.write(petitioner_attorney + "*")

        cur_path = output_directory + "/" + 'defendse_attorney.txt'
        with open(cur_path, "w", encoding="utf-8") as defendse_attorney_file:
            for defendse_attorney in defendse_attorney_set:
                defendse_attorney_file.write(defendse_attorney + "*")

        return judge_set, petitioner_set, defendse_set, petitioner_attorney_set, defendse_attorney_set

    def create_naming_df(self, path: str):
        """
        Read txt file with names serperated by '*'.
        Populating a pandas dataframe with the names as well as the role, for example:
        אסתר חיות - judges
        :param path - the txt file path
        :returns the newly created df
        """
        # check if path exists
        if os.path.exists(path):
            # initialize lists
            names = list()
            roles = list()

            # iterate on the files
            for file in os.listdir(path):
                # populate lists
                with open(file, "r", encoding="utf-8") as cur_file:
                    # variable contains the number of names on the examined role
                    before_assignment_count = len(names)

                    # read the whole file
                    cur_str = cur_file.read()

                    # get only file name without the format
                    cur_role = Path(file).name.split(".")[0]

                    # append all the names to the current list
                    names = names + (cur_str.split("*"))
                    cur_list = list()

                    # calculate how many names exists on the current iteration and extend roles list accoridngly
                    cur_list.extend(repeat(cur_role, (len(names) - before_assignment_count)))
                    roles = roles + cur_list
                    print(str.format("{0} - {1}", cur_role, str(len(names) - before_assignment_count)))

                    # populate the df with the resulting lists
            df = pd.DataFrame()
            df["Before"] = names
            df["Role"] = roles
            return df
        else:
            return None

    def eliminate_words_corresponds_to_regex_list(self, rgx_list, replace_with_that_str: str,
                                                  input_text: str):
        """
        :param rgx_list - list of patterns
        :param replace_with_that_str - str to replace when match exists
        :param input_text - text to look on for patterns
        :returns the new text after subtracting the matches
        """
        new_text = input_text
        for rgx_match in rgx_list:
            new_text = re.sub(rgx_match, replace_with_that_str, new_text)
        return new_text

    def apply_regex_rules_on_naming_csv(self, replace_with_this_str: str, input_name: str, csv_path: str):
        """
        Adds to every name hebrew prefixes that might exists on the data.
        This method is very expansive and not recommended when not necessary.
        :param replace_with_this_str - the string to be replaced with
        :param input_name - name string
        :param csv_path - the csv that contains the names to be subtracted from each and every matched column
        :returns the element after subtracting matched words
        """
        # initalization
        updated_name = input_name
        names_list = list()
        rgx_list = list()

        # open the csv file with the words to eliminate
        with open(csv_path, "r", encoding='utf-8') as csv_file:
            names_list = csv.reader(csv_file, delimiter=',')
            names = [row[0] for row in names_list]

            # add selcted prefixes
            for name in names:
                rgx_list.append(str.format("ו?" + name))
                rgx_list.append(str.format("ל?" + name))
                rgx_list.append(str.format("כ?ש?" + name))
                rgx_list.append(str.format("ה?" + name))
                rgx_list.append(str.format("ב?" + name))
                rgx_list.append(str.format("מ?" + name))
                rgx_list.append(str.format("ש?" + name))
                rgx_list.append(str.format("כ?" + name))
                rgx_list.append(name)

                if len(name.split()) > 1:
                    rgx_list.append(str.format(name.split()[0] + '-' + name.split()[1]))

                if len(name.split('-')) > 1:
                    rgx_list.append(str.format(name.split('-')[0] + ' ' + name.split('-')[1]))

            for rgx_match in rgx_list:
                # take only specific words - not substrings, \\b is the word border.
                pattern = re.compile(str.format("\\b{0}\\b", rgx_match))
                updated_name = re.sub(pattern, replace_with_this_str, input_name)

        after_elimination = ' '.join(updated_name.split())
        return after_elimination

    def eliminiate_empty_and_duplicates(self, df, column: str):
        """
        eliminates empty rows and duplicated rows of a specific column of the df
        :param df - Data Frame to work on
        :param column - column name
        :returns the df after the percedure
        """
        new_df = df.copy()
        # new_df[new_df[column].astype(bool)]
        print(str.format('After eliminating empty {0}', new_df.shape))
        new_df.drop_duplicates(subset=column, keep=False, inplace=True)
        print(str.format('After clearing duplicates {0}', new_df.shape))
        new_df.reset_index(drop=True, inplace=False)
        return new_df

    def is_identical(self, input_word: str, input_collection, mismatch_count):
        """
        check if specified word has mismatch_count amount of chars difference from any of the words in the collection
        :param input_word - string
        :param input_collection - collection that stores strings to check upon
        :param mismatch_count - the maximal difference between the input word and the matched one
        :returns True for a match and False for a mismatch
        """
        for item in input_collection:
            if jellyfish.levenshtein_distance(input_word, item) <= mismatch_count:
                return True
        return False

    def eliminate_naming_single(self, text: str, path: str):
        """
        Eliminates all names match that has been on the csv file
        :param text - the string to work on
        :param path - the csv file path
        :returns the string after eliminating all matched words
        """
        with open(path, "r", encoding="utf-8") as csv_name:
            csv_names = csv.reader(csv_name, delimiter=',')
            names = [row[0] for row in csv_names]
            after_names = ' '.join([word for word in str(text).split() if word not in names])

        return after_names

    def eliminate_naming_multi(self, input_names: list, path: str):
        """
        Eliminates all names match that has been on the csv file
        :param input_names - a list containing the strings to work on
        :param path - the csv file path
        :returns the new list after eliminating all matched words from the original strings
        """
        new_list = list()
        with open(path, "r", encoding="utf-8") as csv_name:
            csv_names = csv.reader(csv_name, delimiter=',')
            names = [row[0] for row in csv_names]
            for name in input_names:
                after_names = ' '.join([word for word in str(name).split() if word not in names])
                if after_names != '':
                    new_list.append(after_names)

        return new_list

    def eliminate_unwanted_chars_single(self, text: str):
        """
        That method is in charge of some of the pre-process precedures:
        1. eliminates pharenthesis
        2. eliminate id numbers
        3. minimize spaces on last names
        :param text - the string we work on
        :returns the text after the process.
        """
        for word in str(text).split(' '):
            if word.find("(") != -1 or word.find(")") != -1:
                text = text.replace(word, "")
            if word.find('.') != -1:
                for char in word:
                    if char in string.digits:
                        text = text.replace(word, "")
                        break

        text = '-'.join(text.split(' - '))
        text = ' '.join([word for word in text.split() if word != ' '])
        return text

    def eliminate_unwanted_chars_multi(self, list_of_names):
        """
        That method is in charge of some of the pre-process precedures:
        1. eliminates pharenthesis
        2. eliminate id numbers
        3. minimize spaces on last names
        :param list_of_names - a list that contain the string we work on
        :returns new list after the process.
        """
        new_list = list()

        for name in list_of_names:
            new_name = self.eliminate_unwanted_chars_single(name)
            new_list.append(new_name)

        return new_list

    def split_df_to_proper_name_and_multiname(self, df, column: str):
        """
        splits given df to two:
        first part is names that has maximal value of 3, first name and two last names or less.
        second part are names that containes more than 3 strings
        :param df - the data frame to work on
        :param column - the string describing the desired column
        :returns the original df as the first value and the new one as the second,
        when the original now contains only "good" values and the second only "problematic" ones
        """
        # initialization
        df.reset_index(inplace=True, drop=True)
        indexes = list()
        new_values = list()

        # drop all the rows of the specified column
        df = df.dropna(subset=[column])

        # iterate on each row as string
        for i, presplit in enumerate(df[column].astype(str)):

            # split and check how many strings exists
            values = presplit.split(' ')
            if len(values) > 3:
                # append the index and the whole string
                indexes.append(i)
                new_values.append(presplit)

        # copy all the values that has been found as "problematic" ones to a new df
        new_df = df.iloc[indexes, :].copy()

        # populate the new df with the values
        new_df[column] = new_values

        # drop the problematic indexes from the original df and reset indexes on both of the dfs
        df.drop(df.index[indexes], inplace=True)
        df.reset_index(inplace=True, drop=True)
        new_df.reset_index(inplace=True, drop=True)

        return df, new_df

    def organize_df_names(self, df, column: str):
        """
        deals with names rows that contain more the 3 strings
        the mechanism handles two secnarios sperately:
        1. when then amount of strings is precisely 3,
           it fixes the first string to be the first name and both second and third to be last name seperated by '-'
        2. even amount of strings - iterate and add couple of strings as first name and last name
        assumptions:
        all names has already passed threw the preprocess - thus, last name that contains '-' is present without any spaces.
        there will be misidentified strings on that process, it relies on the parsed data.
        :param df - the data frame to work on
        :param column - the string describing the column
        :returns the same df after the process
        """
        # initialization
        new_values = list()
        indexes = list()

        # iterate on each row as string
        for i, presplit in enumerate(df[column].astype(str)):
            values = ' '.join(presplit.split()).split()

            # if the len is precisely 3, fix first name and two last names seperated by '-'
            if len(values) == 3:
                full_name = str()
                first_name = values[0]
                values.remove(values[0])
                last_name = '-'.join(values)
                if first_name.find('-') == -1:
                    full_name = str.format('{0} {1}', first_name, last_name)
                else:
                    full_name = str.format('{0} {1}', last_name, first_name)

                df.at[i, column] = full_name

            # if the len > 2 and even, iterate and assign couples until the string is empty
            if len(values) > 2 and len(values) % 2 == 0:
                for idx in range(int(len(values) // 2)):
                    splitted = str()
                    if values[0].find('-') == -1:
                        splitted = str.format("{0} {1}", values[2 * idx], values[(2 * idx) + 1])

                    # that scenario tries to handle cases when last names comes before first ofir-buzaglo itai instead of itai ofir-buzaglo
                    else:
                        splitted = str.format("{0} {1}", values[2 * idx + 1], values[(2 * idx)])

                    # if the index equal to 0 the series already exists, just assign
                    if idx == 0:
                        df.loc[i, column] = splitted

                    # if the index is greated than 0, hence we must append new series.
                    elif idx > 0:
                        cur_series = pd.Series(df.loc[i, :])
                        cur_series.at[column] = splitted
                        df.append(cur_series)

        return df

    def count_normalized_names(self, df, path: str, roles):
        """
        counts all occurences of the names exists on the data frame after the preprocess in the whole available verdicts derictory
        assumptions:
        1. the right form of the name is majorly present
        :param df - the data frame to work on
        :param path - the path that holds all the vericts jsons available
        :param roles: a list of roles to be examined. Attention! use precisely json scheme name here, for example: העותר
        :returns the df after counters assignment
        """
        # initialization
        df["Count_Full_Name"] = 0
        df["Count_First_Name"] = 0
        df["Count_Last_Name"] = 0

        # count the amount of files present
        only_files = next(os.walk(path))[2]
        total = len(only_files)

        # iterate on the files
        for count, file in enumerate(os.scandir(path)):
            with open(file, "r", encoding='utf-8') as json_file:
                verdict_json = json.load(json_file)
                for role in roles:

                    # check for match and advance counter accordingly.
                    # look for a match between all the roles given

                    cur_verdict = ' '.join(verdict_json["Doc Details"][role])
                    for idx, row in enumerate(df['Full_Name'].astype(str)):
                        if row in cur_verdict:
                            df.at[idx, 'Count_Full_Name'] += 1
                            cur_verdict = cur_verdict.replace(row, '')
                        if not cur_verdict.strip():
                            break

                    cur_verdict = ' '.join(verdict_json["Doc Details"][role])
                    for idx, row in enumerate(df['First_Name'].astype(str)):
                        if row in cur_verdict:
                            df.at[idx, 'Count_First_Name'] += 1
                            cur_verdict = cur_verdict.replace(row, '')
                        if not cur_verdict.strip():
                            break

                    cur_verdict = ' '.join(verdict_json["Doc Details"][role])
                    for idx, row in enumerate(df['Last_Name'].astype(str)):
                        if row in cur_verdict:
                            df.at[idx, 'Count_Last_Name'] += 1
                            cur_verdict = cur_verdict.replace(row, '')
                        if not cur_verdict.strip():
                            break

            # print each 10000 files
            if count % 10000 == 0:
                print(str.format("{0}/{1}", count, total))

        return df

    def tidy_split(self, df, column, sep='|', keep=False):
        # credit: https://github.com/cognoma/genes/blob/721204091a96e55de6dcad165d6d8265e67e2a48/2.process.py#L61-L95
        """
        Split the values of a column and expand so the new DataFrame has one split
        value per row. Filters rows where the column is missing.

        Params
        ------
        df : pandas.DataFrame
            dataframe with the column to split and expand
        column : str
            the column to split and expand
        sep : str
            the string used to split the column's values
        keep : bool
            whether to retain the presplit value as it's own row

        Returns
        -------
        pandas.DataFrame
            Returns a dataframe with the same columns as `df`.
        """
        indexes = list()
        new_values = list()
        df = df.dropna(subset=[column])
        for i, presplit in enumerate(df[column].astype(str)):
            values = presplit.split(sep)
            if keep and len(values) > 1:
                indexes.append(i)
                new_values.append(presplit)
            for value in values:
                indexes.append(i)
                new_values.append(value)
        new_df = df.iloc[indexes, :].copy()
        new_df[column] = new_values
        new_df.reset_index(inplace=True, drop=True)
        return new_df

    def append_first_and_last_name(self, df, column: str):
        """
        splits full name to first and last name
        :param df - the data frame to work on
        :param column - string describing the column name
        :returns the df with new 'First_Name' and 'Last_Name' columns
        """
        # initalization
        indexes = []
        first_names = []
        last_names = []

        # iterate on each row and split, if first string contains '-',
        # thus, it is last name and therefore we need to change posiotion
        for idx, name in enumerate(df[column].astype(str)):
            splitted = name.split()
            if len(splitted) == 2:
                indexes.append(idx)
                if splitted[0].find('-') == -1:
                    first_names.append(splitted[0])
                    last_names.append(splitted[1])
                else:
                    first_names.append(splitted[1])
                    last_names.append(splitted[0])

        # addign values to newly created column
        df.loc[indexes, 'First_Name'] = first_names
        df.loc[indexes, 'Last_Name'] = last_names

        print(str.format('Successfully splitted {0} names', len(indexes)))

    def fix_judge_names(self, full_names, txt_path: str):
        """
        looks for a match with the supreme court judges txt file
        :param full_names - a list of strings of the full names after the process
        :param txt_path - a string with the txt file path
        :returns True when the name exists, False when the name does not exists
        """
        if len(full_names) > 0:
            fixed_list = list()
            flag = 0
            for full_name in full_names:

                # apostrophe damges severly the process due to names such as ג'ורג'

                name_without_apostrophe = ''.join([c for c in full_name if c not in ['’', '\'']])

                if name_without_apostrophe != None:
                    split_full_name = name_without_apostrophe.split()
                    if len(split_full_name) >= 2:
                        # input name first and last
                        first_name = split_full_name[0]
                        last_name = split_full_name[1]
                        with open(txt_path, 'r') as text_file:
                            judges_list = text_file.read().splitlines()
                            for cur in judges_list:
                                split_cur_name = cur.split()
                                if len(split_cur_name) == 2:
                                    # current iteration first and last names
                                    cur_first_name = split_cur_name[0]
                                    cur_last_name = split_cur_name[1]

                                    # look for char to char equality
                                    if cur_first_name == first_name and cur_last_name == last_name:
                                        fixed_list.append(cur)
                                        flag = 1
                                        break

                                    # look for '-' as last name delimiter
                                    if cur_last_name.find('-') != -1:
                                        if len(cur_last_name.split('-')) == 2:
                                            cur_last_name_first = cur_last_name.split('-')[0]
                                            cur_last_name_second = cur_last_name.split('-')[1]
                                            if cur_first_name[0] == first_name[0] and (
                                                    jellyfish.levenshtein_distance(cur_last_name_first,
                                                                                   last_name) <= 1 or jellyfish.levenshtein_distance(
                                                    cur_last_name_second, last_name) <= 1):
                                                fixed_list.append(cur)
                                                flag = 1
                                                break

                                    # look for last name equality and one char first name equality
                                    if cur_first_name[0] == first_name[0] and cur_last_name == last_name:
                                        fixed_list.append(cur)
                                        flag = 1
                                        break

                                    # look for last name one string apart differentiation and first name first char equality
                                    if cur_first_name[0] == first_name[0] and jellyfish.levenshtein_distance(cur_last_name,last_name) <= 1:
                                        fixed_list.append(cur)
                                        flag = 1
                                        break

                                # handle 3 spaces delimited names
                                elif len(split_cur_name) == 3:
                                    cur_first_name = split_cur_name[0]
                                    cur_last_name_first = split_cur_name[1]
                                    cur_last_name_second = split_cur_name[2]

                                    if cur_first_name[0] == first_name[0] and (jellyfish.levenshtein_distance(cur_last_name_first,last_name) <= 1 or jellyfish.levenshtein_distance(cur_last_name_second,last_name) <= 1):
                                        fixed_list.append(cur)
                                        flag = 1
                                        break
                        if flag == 0:
                            fixed_list.append(full_name)
                        flag = 0

            return fixed_list

    def clean_single_name_single(self, name: str):
        """
        clean names when multiple '-' character is present.
        that issue may be solved adjusting the parser's work
        :param name: a string represents the name to clean
        :return: cleaned string
        """
        new_name = str()

        if str(name).find('-') != -1:
            count = 0
            for char in name:
                if char == '-':
                    count += 1

            if count != 1:
                left = str()
                right = str()
                left = name.split('-')[0]
                right = name.split('-')[1]
                left = ''.join([char for char in str(left) if char not in string.punctuation])
                right = ''.join([char for char in str(right) if char not in string.punctuation])
                new_name = str.format('{0}-{1}', left, right)
        else:
            new_name = ''.join([char for char in str(name) if char not in string.punctuation])

        return new_name

    def clean_single_name_multi(self, names):
        """
        clean names when multiple '-' character is present.
        that issue may be solved adjusting the parser's work
        :param names: a list of strings that represents the names to clean
        :return: the list with the cleaned names
        """
        new_names = list()

        for name in names:
            new_names.append(self.clean_single_name_single(name))

        return new_names

    def pre_process_not_legal(self, names):
        processed_names = self.eliminate_unwanted_chars_multi(names)
        return processed_names

    def split_by_char(self, names, char: str):
        splitted = list()
        for name in names:
            flag = False
            name_one = str()
            name_two = str()

            if name.find(char) != -1:
                name_one = ' '.join(name.split(char)[0].split())
                name_two = ' '.join(name.split(char)[1].split())
                flag = True

            if flag:
                splitted.append(name_one)
                splitted.append(name_two)
            else:
                splitted.append(name)

        return splitted

    def organize_name(self, names):
        new_values = list()
        for name in names:
            values = ' '.join(name.split()).split()
            flag = False
            if len(values) == 3:
                first_name = values[0]
                values.remove(values[0])
                last_name = '-'.join(values)
                full_name = str.format('{0} {1}', first_name, last_name)
                new_values.append(full_name)
                flag = True

            elif len(values) > 2 and len(values) % 2 == 0:
                for idx in range(int(len(values) // 2)):
                    splitted = str.format("{0} {1}", values[2 * idx], values[(2 * idx) + 1])
                    new_values.append(splitted)
                flag = True

            if not flag:
                new_values = names

            new_values = [self.determine_order(name) for name in new_values]

        return new_values

    def determine_order(self, full_name: str):
        """
        determines if specified full name is in the correct order: (first name) (last_name)
        full_name - string to be evaluated
        returns the string after order is set
        """

        if full_name != None:
            # split the string
            values = full_name.split()
            if len(values) > 1:
                first = values[0]
                last = values[1]
                # check if the first name contains '-' - that means it is last name in the wrong position
                if first.find('-') != -1:
                    new_str = f'{last} {first}'
                else:
                    new_str = f'{first} {last}'

                return new_str
            else:
                return full_name
        return 'unknown'

    def pre_process_legal(self, names):
        names_after_eliminating_unwanted_chars = self.eliminate_unwanted_chars_multi(names)
        names_after_first_split = self.split_by_char(names_after_eliminating_unwanted_chars, ';')
        names_after_second_split = self.split_by_char(names_after_first_split, ',')

        names_after_naming = self.eliminate_naming_multi(names_after_second_split, utils.COMMON_TITLES_CSV)
        names_after_stopwords = self.eliminate_naming_multi(names_after_naming, utils.SW_AFTER_FILTER_CSV)

        names_ready_for_for_first_and_last_name = self.organize_name(names_after_stopwords)
        after_single_clean_list = self.clean_single_name_multi(names_ready_for_for_first_and_last_name)

        return after_single_clean_list

    def write_normalized_values_to_json(self, verdict_json, dest_path, input_list, new_role_key:str):
        """
               adds the normalized key with the new values to the json and write it to destination
               verdict_json - the input verdict json
               dest_path - the string of the destination directory
               input_list - the normalized values strings in a list
               new_role_key - the key string that will be added to the json
               """

        new_verdict_json = verdict_json

        new_verdict_json['Doc Details'][new_role_key] = input_list
        output = self.output_path+dest_path
        with open(output, 'w', encoding='utf-8') as json_to_write:
            json.dump(new_verdict_json, json_to_write, ensure_ascii=False)

    def get_verdict_id(self, verdict_path:str):
        """
        extracts and returns the verdict id
        :param verdict_path: full path of the verdict json
        :return: the verdict id as string
        """
        with open(verdict_path, 'r', encoding='utf-8') as json_file:
            verdict = json.load(json_file)
            verdict_id = verdict['_id']
        return verdict_id

    def normalize(self, case_path: str):
        """
        takes the json path and path all the representatives names through the procedure
        case_path - the verdict path string
        """
        try:
            input_joined_path = self.input_path + case_path
            with open(input_joined_path, 'r', encoding='utf-8') as json_file:
                verdict_json = json.load(json_file)

                petitioners = verdict_json["Doc Details"]["העותר"]
                defense = verdict_json["Doc Details"]["המשיב"]
                petitioner_attorneys = verdict_json["Doc Details"]["בשם העותר"]
                defense_attorneys = verdict_json["Doc Details"]["בשם המשיב"]
                judges = verdict_json["Doc Details"]["לפני"]

                cleaned_petitioners = self.pre_process_not_legal(petitioners)
                cleaned_defense = self.pre_process_not_legal(defense)
                cleaned_petitioners_attorneys = self.pre_process_legal(petitioner_attorneys)
                cleaned_defense_attorneys = self.pre_process_legal(defense_attorneys)
                cleaned_judges = self.pre_process_legal(judges)
                fixed_judges = self.fix_judge_names(cleaned_judges, utils.COURT_JUDGES)
                self.add_to_legal_dictionary(judges, fixed_judges)

                verdict_json["Doc Details"]["העותר מנורמל"] = cleaned_petitioners
                verdict_json["Doc Details"]["המשיב מנורמל"] = cleaned_defense
                verdict_json["Doc Details"]["בשם העותר מנורמל"] = cleaned_petitioners_attorneys
                verdict_json["Doc Details"]["בשם המשיב מנורמל"] = cleaned_defense_attorneys
                verdict_json["Doc Details"]["לפני מנורמל"] = fixed_judges

                self.counter += 1
                self.dump_dictionary()

                return verdict_json

        except Exception as e:
            self.error_counter += 1
            traceback.print_tb(e.__traceback__)

    def add_to_legal_dictionary(self, befores, afters):
        """
        adds the new normalized name to the object dictionary
        :param befores: the name before normalization
        :param afters: the name after normalization
        """
        if befores != None and afters != None:
            if len(befores) == len(afters):
                for before, after in zip(befores, afters):
                    if before not in self.legal_dictionary.keys():
                        self.legal_dictionary[before] = after

    def dump_dictionary(self):
        try:
            with open('legal_personal.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for k, v in self.legal_dictionary.items():
                    writer.writerow([k, v])
        except IOError:
            print("I/O error")

    def run(self):
        while True:
            try:
                cur = self.get_job_from_queue()
                self.normalize(cur)
            except queue.Empty as empty:
                pass
                # add logging here

    def enrich(self, file_path):
        return self.normalize(file_path)
