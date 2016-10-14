#!/usr/bin/env python

import app_config
import app_utils
import calendar
import time
import unittest

from fabfile import data, render
from models import models
from peewee import *

# class ResultsLoadingTestCase(unittest.TestCase):
#     """
#     Test bootstrapping postgres database
#     """
#     def setUp(self):
#         data.load_results(app_config.ELEX_INIT_FLAGS)
#         data.create_calls()
#         data.create_race_meta()

#     # def test_results_loading(self):
#     #     results_length = models.Result.select().count()
#     #     self.assertEqual(results_length, 63227)

#     # def test_calls_creation(self):
#     #     calls_length = models.Call.select().count()
#     #     self.assertEqual(calls_length, 1762)

#     # def test_race_meta_creation(self):
#     #     race_meta_length = models.RaceMeta.select().count()
#     #     self.assertEqual(race_meta_length, 1733)

#     # def test_multiple_calls_creation(self):
#     #     data.create_calls()
#     #     calls_length = models.Call.select().count()
#     #     self.assertEqual(calls_length, 1762)

#     # def test_results_deletion(self):
#     #     data.delete_results()
#     #     results_length = models.Result.select().count()
#     #     self.assertEqual(results_length, 0)

class ResultsRenderingTestCase(unittest.TestCase):
    """
    Test selecting and rendering results
    """

    def test_presidential_state_selection(self):
        results = render._select_presidential_state_results()
        results_length = len(results)
        self.assertEqual(results_length, 216)

    def test_presidential_county_selection(self):
        results = render._select_presidential_county_results('PA')
        results_length = len(results)
        self.assertEqual(results_length, 268)

    def test_senate_selection(self):
        results = render._select_senate_results()
        results_length = len(results)
        self.assertEqual(results_length, 127)

    def test_house_selection(self):
        results = render._select_house_results()
        results_length = len(results)
        self.assertEqual(results_length, 102)

    def test_governor_selection(self):
        results = render._select_governor_results()
        results_length = len(results)
        self.assertEqual(results_length, 38)

    def test_ballot_measure_selection(self):
        results = render._select_ballot_measure_results()
        results_length = len(results)
        self.assertEqual(results_length, 78)



if __name__ == '__main__':
    unittest.main()