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
        self.input_path = PARSED_SUCCESS_DIR
        self.output_path = NORMALIZED_SUCCESS_DIR
        self._jobs_queue = Queue()
        self.previous = None
        self.next = None

    def enrich(self, file_path):
        raise NotImplementedError('This method must be overridden !')

    def add_job_to_queue(self,job):
        self._jobs_queue.put(job)

    @property
    def _jobs_queue(self):
        return self._jobs_queue

    @property
    def input_path(self):
        return self.input_path

    @input_path.setter
    def input_path(self, setter_input_path):

        if os.path.exists(setter_input_path) == False:
            raise FileNotFoundError(f'path {setter_input_path} does not exists')

        if os.path.isdir(setter_input_path) == False:
            raise NotADirectoryError(f'path {setter_input_path} is not a directory')

        self.input_path = setter_input_path

    @property
    def output_path(self):
        return self.output_path

    @output_path.setter
    def output_path(self, setter_output_path):

        if os.path.exists(setter_output_path) == False:
            raise FileNotFoundError(f'path {setter_output_path} does not exists')

        if os.path.isdir(setter_output_path) == False:
            raise NotADirectoryError(f'path {setter_output_path} is not a directory')

        self.output_path = setter_output_path

    @_jobs_queue.setter
    def _jobs_queue(self, value):
        raise PermissionError('You are not allowed to set the Queue')

    @property
    def previous(self):
        return self.previous

    @previous.setter
    def previous(self, value):
        self.previous = value

    @property
    def next(self):
        return self.next

    @next.setter
    def next(self, value):
        self.next = value

    def __len__(self):
        return self._jobs_queue.qsize()
