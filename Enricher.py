import os
from queue import Queue
from utils import PARSED_SUCCESS_DIR, NORMALIZED_SUCCESS_DIR

"""
A class that is in charge of enriching existing data
It acts as the base class of all current and future enrichment classes.
The enricher demands a 'settings.json'
"""


class Enricher:

    def __init__(self):
        Enricher.input_path = PARSED_SUCCESS_DIR
        Enricher.output_path = NORMALIZED_SUCCESS_DIR
        Enricher._jobs_queue = Queue()
        Enricher.previous = None
        Enricher.next = None

    def enrich(self, file_path):
        raise NotImplementedError('This method must be overridden !')

    def add_job_to_queue(self, job):
        Enricher._jobs_queue.put(job)

    @property
    def _jobs_queue(self):
        return Enricher._jobs_queue

    @property
    def input_path(self):
        return Enricher.input_path

    @input_path.setter
    def input_path(self, setter_input_path):

        if not os.path.exists(setter_input_path):
            raise FileNotFoundError(f'path {setter_input_path} does not exists')

        if not os.path.isdir(setter_input_path):
            raise NotADirectoryError(f'path {setter_input_path} is not a directory')

        Enricher.input_path = setter_input_path

    @property
    def output_path(self):
        return Enricher.output_path

    @output_path.setter
    def output_path(self, setter_output_path):

        if not os.path.exists(setter_output_path):
            raise FileNotFoundError(f'path {setter_output_path} does not exists')

        if not os.path.isdir(setter_output_path):
            raise NotADirectoryError(f'path {setter_output_path} is not a directory')

        Enricher.output_path = setter_output_path

    @_jobs_queue.setter
    def _jobs_queue(self, value):
        raise PermissionError('You are not allowed to set the Queue')

    @property
    def previous(self):
        return Enricher.previous

    @previous.setter
    def previous(self, value):
        Enricher.previous = value

    @property
    def next(self):
        return Enricher.next

    @next.setter
    def next(self, value):
        Enricher.next = value

    def __len__(self):
        return Enricher._jobs_queue.qsize()
