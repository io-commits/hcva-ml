import os
from queue import Queue

"""
An abstract class that is in charge of enriching existing data
It acts as the base class of all current and future enrichment classes.
The enricher demands a 'settings.json'
"""


class Enricher:

    def __init__(self, input_path, output_path):
        Enricher.input_path = input_path
        Enricher.output_path = output_path
        Enricher.jobs_queue = Queue()
        Enricher.previous = None
        Enricher.next = None

    def enrich(self, file_path):
        raise NotImplementedError('This method must be overridden !')

    def add_job_to_queue(self, job):
        Enricher.jobs_queue.put(job)

    def get_job_from_queue(self):
        return self.jobs_queue.get()

    @property
    def jobs_queue(self):
        return Enricher.jobs_queue

    @jobs_queue.setter
    def jobs_queue(self, value):
        if Enricher._jobs_queue is None:
            Enricher._jobs_queue = value
        else:
            raise PermissionError('You are not allowed to set the Queue')

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

        Enricher._output_path = setter_output_path

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
        return Enricher.jobs_queue.qsize()
