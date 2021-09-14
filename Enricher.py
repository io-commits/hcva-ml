import pandas as pd
import numpy as np
import json
from queue import Queue
import os

"""

An abstract class that is in charge of enriching existing data
It acts as the base class of all current and future enrichment classes.

The enricher demands a 'settings.json'



"""


class Enricher:

    def __init__(self, settings_file_path='settings.json'):

        with open(settings_file_path, 'r') as json_file:
            dictionary = json.load(json_file)
        self._jobs_queue = Queue()
        self._input_path = dictionary["Enrichers"]["input_path"]
        self._output_path = dictionary["Enrichers"]["output_path"]
        self._previous = None
        self._next = None

    def enrich(self,file_path:str):
        raise NotImplementedError('This method must be overridden !')

    def add_job_to_queue(self, job:str):
        self.jobs_queue.put(job)

    def get_job_from_queue(self):
        return self.jobs_queue.get()

    @property
    def jobs_queue(self):
        return self._jobs_queue

    @jobs_queue.setter
    def jobs_queue(self, value):
        if self._jobs_queue is None:
            self._jobs_queue = value
        else:
            raise PermissionError('You are not allowed to set the Queue')

    @property
    def input_path(self):
        return self._input_path

    @input_path.setter
    def input_path(self, setter_input_path:str):

        if not os.path.exists(setter_input_path):
            raise FileNotFoundError(f'path {setter_input_path} does not exists')

        if not os.path.isdir(setter_input_path):
            raise NotADirectoryError(f'path {setter_input_path} is not a directory')

        self._input_path = setter_input_path

    @property
    def output_path(self):
        return self._output_path

    @output_path.setter
    def output_path(self, setter_output_path:str):

        if os.path.exists(setter_output_path) == False:
            raise FileNotFoundError(f'path {setter_output_path} does not exists')

        if os.path.isdir(setter_output_path) == False:
            raise NotADirectoryError(f'path {setter_output_path} is not a directory')

        self._output_path = setter_output_path

    @property
    def previous(self):
        return self._previous

    @previous.setter
    def previous(self, value:Enricher):
        self._previous = value

    @property
    def next(self):
        return self._next

    @next.setter
    def next(self, value:Enricher):
        self._next = value

    def __len__(self):
        return self._jobs_queue.qsize()


if __name__ == '__main__':
    e = Enricher()
    e._input_path = '3'

    print(e.Enrich(1))
