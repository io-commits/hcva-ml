import pandas as pd
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import re
import os
import json
import csv
import string
import pickle
from hcvaEnricher import utils, Enricher


class Classifier(Enricher):

    def __init__(self, settings: str):
        super().__init__(settings)
        self._models, self._tfidfs = self.load_classifiers(settings.CLASSIFIERS)
        self._category_to_ngram = self.load_category_to_ngram(utils.NGRM_JSON)

    def determine_models_path(self, settings_file_path:str):
        """
        loads the settings file and extracts the models path
        :param settings_file_path: settings file passed to the constructor
        :return: the path as string
        """
        with open(settings_file_path,'r') as json_file:
            settings = json.load(json_file)
            enrichers = settings['Enrichers']
        # complete after fixing the wanted initialization

    def load_category_to_ngram(self, json_path: str):
        """
        loads the category to ngram dictionary
        :param json_path: the dictionary location as json file
        :return: a dictionary python object
        """
        with open(json_path, 'r') as file:
            json_file = json.load(file)
            scanned_dict = json_file['Categories']
            return scanned_dict

    @property
    def models(self):
        return self._models

    @models.setter
    def models(self, value):
        if self._models is None:
            self._models = dict()
        else:
            raise PermissionError('You are not allowed to set the models dictionary after initialization')

    @property
    def tfidfs(self):
        return self._tfidfs

    @tfidfs.setter
    def tfidfs(self, value):
        if self._tfidfs is None:
            self._tfidfs = dict()
        else:
            raise PermissionError('You are not allowed to set the tfidfs dictionary after initialization')

    @property
    def category_to_ngram(self):
        return self._category_to_ngram

    @category_to_ngram.setter
    def category_to_ngram(self, value):
        if self._category_to_ngram is None:
            self._category_to_ngram = dict()
        else:
            raise PermissionError('You are not allowed to set the tfidfs dictionary after initialization')


    def check_if_key_exists(self, input_key, input_collection):
        """
        checks if key exists on the collection
        :param input_key - the key to search on the collection
        :param input_collection - the collection
        :returns True if key exists, else False
        """
        key_found = False
        for key in input_collection.keys():
            if key == input_key:
                key_found = True
                break

        return key_found

    def get_sub_categories_count(self, input_category: str, input_path: str):
        """
        checks how many subcategories exists to an input category.
        :param input_category - a string describes the category
        :param input_path - the verdicts files path
        :returns a dictionary which has the category names as keys and the occurences count as values
        """
        category_dict = dict()

        # iterate on the files on the input path
        for scanned_file in os.scandir(input_path):
            # look for directories only due to the fact that all the extracted verdicts
            # are already have been set to the right directory tree
            if scanned_file.is_dir() == True:
                # split the directory name by ' - ' - that is how the justice department
                # have been decided to seperate the category and the sub category
                dir_name = os.path.basename(scanned_file)
                splitted_category = str(dir_name).split(" - ")
                # check the len of the splitted catregory
                # there are two main representations, one sub or two sub
                if splitted_category[0] == input_category:
                    if len(splitted_category) == 3:
                        for first_sub in os.scandir(scanned_file):
                            if self.check_if_key_exists(splitted_category[1] + " - " + splitted_category[2], category_dict):
                                category_dict[splitted_category[1] + " - " + splitted_category[2]] += 1
                            else:
                                category_dict[splitted_category[1] + " - " + splitted_category[2]] = 1
                    else:
                        for first_sub in os.scandir(scanned_file):
                            if self.check_if_key_exists(splitted_category[1], category_dict):
                                category_dict[splitted_category[1]] += 1
                            else:
                                category_dict[splitted_category[1]] = 1

        category_dict[input_category + " Total"] = sum(category_dict.values())
        return category_dict

    def get_categories_count(self, input_path: str):
        """
        counts the quantity of main categories
        :param input_path - a string holds the path to the root folder
        :returns a dictionary with the names of the
         categories and the verdicts present for each and every of them
        """
        # initalization
        category_dict = dict()
        # iterate on the folder
        for scanned_file in os.scandir(input_path):
            # look for directories only
            if scanned_file.is_dir() == True:
                # look for the main category only, for example: 'Civil'
                dir_name = os.path.basename(scanned_file)
                splitted_category = str(dir_name).split(" - ")
                cur_category = splitted_category[0]
                # advance counter according to the findings
                for sub_cat in os.scandir(scanned_file):
                    if self.check_if_key_exists(cur_category, category_dict):
                        category_dict[cur_category] += 1
                    else:
                        category_dict[cur_category] = 1

        return category_dict

    def print_dict(self, input_dict):
        """
        prints given dictionary
        :param input_dict - dictionary
        """
        for key in input_dict:
            print(key + " : " + str(input_dict[key]))

    def create_train_test_of_specified_verdict_category(self, input_category: str, input_path: str, desired_test_size):
        """
        based on the root directory tree, this method will make a data frame for
        every category which has the exact amount of matched verdicts vs non matched.
        for instance - 1000 'Civil' verdicts vs 1000 mixture of 'Administrative'
        , 'Constitutional' and so on.
        :param input_category - a string that holds the category name -
         Attention! this name must be identical to the name present on the root directory
        :param input_path - a string that holds the root directory path
        :param desired_test_size - a number between 0-1 that corresponds to the
         precentage of test data that will be created and returned.
         needless to say that will fix the train precentage as well.
        :returns a data frame for the train and test data for the category and non category
         for instance, 1000 Civil verdicts will yield a 70%-30% division.
        :returns a data frame with only id and verdict text as well.
        keep in mind that a verdict can, and will, classify as two categories if it exists on both folders
        """
        # dataframes initialization
        verdicts_train_df_input_category = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        verdicts_test_df_input_category = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        verdicts_train_df_not_input_category = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        verdicts_test_df_not_input_category = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        temp_df_cat = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        temp_df_not_cat = pd.DataFrame(columns=["Veredict_ID", "Verdict_Text", "Category"])
        df_tuple = [temp_df_cat, temp_df_not_cat]

        # tuple initialization
        # index 1 is the category df, 0 is the opposite
        for file in os.scandir(input_path):
            if file.is_dir():
                dir_name = str(os.path.basename(file)).split(" - ")[0]
                cur_cat = 0
                if dir_name == input_category:
                    cur_cat = 1
                for verdict in os.scandir(file):
                    if verdict.is_dir():
                        for sub_folder in os.scandir(verdict):
                            df_tuple[cur_cat] = df_tuple[cur_cat].append(self.make_temp_df(sub_folder, cur_cat),
                                                                         ignore_index=True)
                    else:
                        df_tuple[cur_cat] = df_tuple[cur_cat].append(self.make_temp_df(verdict, cur_cat), ignore_index=True)

        # making train-test split from the pre initialized tuple
        verdicts_train_df_input_category, verdicts_test_df_input_category = train_test_split(df_tuple[1],
                                                                                             test_size=desired_test_size)
        verdicts_train_df_not_input_category, verdicts_test_df_not_input_category = train_test_split(df_tuple[0],
                                                                                                     test_size=desired_test_size)

        # adjusting the size of each df
        if verdicts_train_df_input_category.shape[0] > verdicts_train_df_not_input_category.shape[0]:
            verdicts_train_df_input_category = verdicts_train_df_input_category.sample(
                verdicts_train_df_not_input_category.shape[0])
        else:
            verdicts_train_df_not_input_category = verdicts_train_df_not_input_category.sample(
                verdicts_train_df_input_category.shape[0])

        if verdicts_test_df_input_category.shape[0] > verdicts_test_df_not_input_category.shape[0]:
            verdicts_test_df_input_category = verdicts_test_df_input_category.sample(
                verdicts_test_df_not_input_category.shape[0])
        else:
            verdicts_test_df_not_input_category = verdicts_test_df_not_input_category.sample(
                verdicts_test_df_input_category.shape[0])

        # populating the returned df
        returned_train_df = verdicts_train_df_input_category.append(verdicts_train_df_not_input_category,
                                                                    ignore_index=True)
        returned_test_df = verdicts_test_df_input_category.append(verdicts_test_df_not_input_category,
                                                                  ignore_index=True)

        # returning the full dfs and the id-text only variable as well
        return returned_train_df, \
               returned_test_df, \
               returned_train_df.drop(["Veredict_ID", "Verdict_Text"],axis=1), \
               returned_test_df.drop(["Veredict_ID", "Verdict_Text"], axis=1)

    def make_temp_df(self, path: str, cur_cat: str):
        """
        makes temporary df which holds the current extracted verdict and summary
        :param path - a string that holds the verdict path
        :param cur_cat - a string the holds the current category
        :returns a data frame that corresponds to the main data frame structre
        """
        cur_verdict, cur_id = self.get_verdict_summary_and_id(path)
        return pd.DataFrame([[cur_id, cur_verdict, cur_cat]], columns=["Veredict_ID", "Verdict_Text", "Category"])

    def clean_text_no_stopwords(self, text: str):
        """
        eliminates stopwords only
        :param text - a string to work on
        :returns a cleaned text without punctuation
        """
        text_splitted_to_chars = [char for char in text if char not in string.punctuation]
        joined = ''.join(text_splitted_to_chars)
        return joined.split()

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

    def clean_text(self, text: str, stopwords_path: str, naming_path: str):
        """
        cleans given text
        eliminates all matched occurences found on the naming and stopwords files from the given string
        applies hebrew prefixes when searching
        :param text - the given text string
        :param stopwords_path - the path of the stopwords file
        :param naming_path - the path of the common namings file
        :returns the text after elimination
        """
        text_splitted_to_chars = [char for char in text if char not in string.punctuation]
        joined = ''.join(text_splitted_to_chars)

        text_splitted_to_chars = [char for char in text_splitted_to_chars if char not in string.digits]
        joined = ''.join(text_splitted_to_chars)

        after_stopwords = self.apply_regex_rules_on_naming_csv('', joined, stopwords_path)
        after_naming = self.apply_regex_rules_on_naming_csv('', after_stopwords, naming_path)

        return after_naming

    def check_best_n_gram_for_each_category(self, input_path, categories, n_grams, dest_path):
        """
        checks the best ngram for every category
        tries all of values stored on 'n_grams' dict
        after the precedure dumps the results text to file on the dest_apth
        :param input_path - a string of the path where the ready to clasiify verdict are present and ordered.
        :param cateogires - a list of strings that represents the category names
        :param n_grams - a list of round numbers that will be applied as test input for the vectorizer
        :param dest_path - a string of the destination path - on that path the results will be written
        """
        for cat in categories:
            path = dest_path + '/' + cat + '.txt'
            with open(path, 'w', encoding='utf-8') as file:
                for n_gram_val in n_grams:
                    train, test, y_train, y_test = self.create_train_test_of_specified_verdict_category(cat, input_path)
                    model_series = []
                    vec = TfidfVectorizer(ngram_range=(n_gram_val, n_gram_val))
                    transformed = vec.fit_transform(train["Verdict_Text"])
                    model = MultinomialNB().fit(transformed, y_train.to_numpy(dtype=float).ravel())
                    test_transformed = vec.transform(test["Verdict_Text"])
                    y_test_predict = model.predict(test_transformed)
                    file.write(str.format('\nn_gram val : {0} \n{1}\n', n_gram_val,
                                          classification_report(y_test.to_numpy(dtype=float).ravel(), y_test_predict)))

    def train_and_dump_model_and_tfidf_vectorizer(self, category: str, input_path: str, model_destination_path: str,
                                                  tfidf_destination_path: str, desired_n_gram, test_size):
        """
        trains a specific category model and with a specific n-gram and dump it to the destination
        :param category - a string that holds the current category
        :param input_path - a string that holds the root folder of the categories-ordered data
        :param destination_path - a string that holds the path of the dumped model
        :param desired_n_gram - a number with the optimal n_gram for that category
        """
        train, test, y_train, y_test = self.create_train_test_of_specified_verdict_category(category, input_path, test_size)
        vec = TfidfVectorizer(ngram_range=(desired_n_gram, desired_n_gram))
        transformed = vec.fit_transform(train["Verdict_Text"])
        model = MultinomialNB().fit(transformed, y_train.to_numpy(dtype=float).ravel())

        with open(model_destination_path, 'wb') as model_file:
            pickle.dump(model, model_file)

        with open(tfidf_destination_path, 'wb') as tfidf_file:
            pickle.dump(vec, tfidf_file)

    def automate_train_based_on_specific_ngram(self, categories, input_path: str, destination_path: str):
        """
        uses the ngram check that has been done for each category and applies the best ngram for each category
        then dumps the model to the destination folder
        :param categoires - a list of strings that holds the categories
        :param input_path - a string of the root readytoclassify folder
        :param destination_path - a string of the destination folder
        """
        count = 1
        try:

            for category in categories:

                print(str(count) + ' / ' + str(len(categories)))

                if category == 'Administrative':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 9, 0.3)

                elif category == 'Civil':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 9, 0.3)

                elif category == 'Constitutional':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 4, 0.3)

                elif category == 'Criminal':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 6, 0.3)

                elif category == 'Family':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 8, 0.3)

                elif category == 'International law':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 9, 0.3)

                elif category == 'Labor and Employment':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 7, 0.3)

                elif category == 'National security, military, and the territories':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 2, 0.3)

                elif category == 'Religious':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 8, 0.3)

                elif category == 'Social security, Health Insurance, Pension':
                    self.train_and_dump_model_and_tfidf_vectorizer(category, input_path,
                                                              destination_path + '/' + category + '.pkl',
                                                              destination_path + '/' + category + '.tfidf', 7, 0.3)

                count += 1
        except Exception as e:
            print(e)

    def load_classifiers(self, input_path: str):
        """
        loads the classifier from the given path and returns a dictionary with the category name as key
        and the model itself as the value
        :param input_path - a string the represnets the models folder
        :returns string-model and string-tfidf dictionary
        """
        models_dict = dict()
        tfidf_dict = dict()

        for file in os.scandir(input_path):
            if os.path.isfile(file):
                with open(file, 'rb') as pickle_file:
                    cur_model = pickle.load(pickle_file)
                    if os.path.basename(file).find('.pkl') != -1:
                        current_category = os.path.basename(file).split('.pkl')[0]
                        models_dict[current_category] = cur_model

                    if os.path.basename(file).find('.tfidf') != -1:
                        current_category = os.path.basename(file).split('.tfidf')[0]
                        tfidf_dict[current_category] = cur_model

        return models_dict, tfidf_dict

    def classify(self, verdict_summary: str):
        """
        passing the input verdict string as the prediction input for all of the pre-trained models
        the highest probability will be the verdict category
        :param verdict_summary - a string of the verdict summary
        :returns - the chosen category string
        """
        category_to_score = dict()
        score_to_category = dict()

        for category, model in zip(self.models.keys(), self.models.values()):
            cur_vectorizer = self.tfidfs[category]
            transformed = cur_vectorizer.transform([verdict_summary])
            cur_prob = model.predict_proba(transformed)
            value = cur_prob[0][1]
            category_to_score[category] = value
            score_to_category[value] = category

        max_prob = max(category_to_score.values())
        return score_to_category[max_prob]

    def enrich(self, file_path):
        input_joined_path = self.input_path + '/' + file_path
        with open(input_joined_path, "r", encoding="utf-8") as json_file:
            verdict_json = json.load(json_file)
            verdict_summary = verdict_json["Doc Details"]["סיכום"]
            category = self.classify(verdict_summary)
            verdict_json["Doc Details"]['Category'] = category

            return verdict_json
