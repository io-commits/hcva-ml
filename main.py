from hcva.parser.enricher.Normalizer import Normalizer
import utils


def normalizer():
    print('enricher::normalize')
    utils.create_dir(utils.NORMALIZED_SUCCESS_DIR)
    utils.create_dir(utils.NORMALIZED_FAILED_DIR)
    enricher = Normalizer(utils.CSV_PATH)
    cases = utils.get_cases(utils.PARSED_SUCCESS_DIR)
    for case in cases:
        e = enricher.enrich(case)
        utils.save_case(e, case)
