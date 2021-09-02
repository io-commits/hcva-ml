import traceback
import hcvaEnricher.utils as utils
from hcvaEnricher.Classifier import Classifier
from hcvaEnricher.Normalizer import Normalizer

def normalize(settings):
    print('enricher::normalize')
    utils.create_dir(settings.NORMALIZED_SUCCESS_DIR)
    utils.create_dir(settings.NORMALIZED_FAILED_DIR)
    normalizer = Normalizer(settings, utils.COMMON_TITLES_CSV)
    cases = utils.get_cases(settings.PARSED_SUCCESS_DIR)
    for case in cases:
        e = None
        try:
            e = normalizer.enrich(case)
            utils.save_data(e, case, settings.NORMALIZED_SUCCESS_DIR)
        except Exception as err:
            print(err)
            traceback.print_exc()
            utils.save_data(e, case, settings.NORMALIZED_FAILED_DIR)


def classify(settings):
    print('enricher::classifier')
    utils.create_dir(settings.CLASSIFIED_SUCCESS_DIR)
    utils.create_dir(settings.CLASSIFIED_FAILED_DIR)
    classifier = Classifier(settings)
    cases = utils.get_cases(settings.NORMALIZED_SUCCESS_DIR)
    cases.extend(utils.get_cases(settings.NORMALIZED_FAILED_DIR))
    for case in cases:
        e = None
        try:
            e = classifier.enrich(case)
            utils.save_data(e, case, settings.CLASSIFIED_SUCCESS_DIR)
        except Exception as err:
            print(err)
            traceback.print_exc()
            utils.save_data(e, case, settings.CLASSIFIED_FAILED_DIR)


def enricher(settings):
    while True:
        print('running enricher')
        normalize(settings)
        classify(settings)
        print('enricher finished')
        utils.delay(10)
