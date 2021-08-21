import hcva.parser.enricher.utils as utils
from hcva.parser.enricher.Normalizer import Normalizer


def normalizer():
    print('enricher::normalize')
    utils.create_dir(utils.NORMALIZED_SUCCESS_DIR)
    utils.create_dir(utils.NORMALIZED_FAILED_DIR)
    enricher = Normalizer(utils.LEGAL_PERSONAL_CSV)
    cases = utils.get_cases(utils.PARSED_SUCCESS_DIR)
    for case in cases:
        e = None
        try:
            e = enricher.enrich(case)
            utils.save_data(e, case, utils.NORMALIZED_SUCCESS_DIR)
        except Exception as err:
            print(err)
            utils.save_data(e, case, utils.NORMALIZED_FAILED_DIR)
