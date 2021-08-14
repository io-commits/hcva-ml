from hcva.parser.enricher.Normalizer import Normalizer
from utils import create_dir, get_cases, PARSED_SUCCESS_DIR, NORMALIZED_SUCCESS_DIR, NORMALIZED_FAILED_DIR, CSV_PATH


def normalize():
    print('enricher::normalize')
    create_dir(NORMALIZED_SUCCESS_DIR)
    create_dir(NORMALIZED_FAILED_DIR)
    enricher = Normalizer(CSV_PATH)
    cases = get_cases(PARSED_SUCCESS_DIR)
    for case in cases:
        enricher.enrich(case)
