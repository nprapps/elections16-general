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
        self.assertEqual(results_length, 212)

    def test_presidential_national_selection(self):
        results = render._select_presidential_national_results()
        results_length = len(results)
        self.assertEqual(results_length, 4)

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

    def test_calculate_electoral_college(self):
        state_results = render._select_presidential_state_results()
        electoral_totals = {}
        serialized_results = render._serialize_results(state_results, electoral_totals=electoral_totals)

        national_results = render._select_presidential_national_results()
        national_serialized_results = render._serialize_results(national_results, determine_winner=False, electoral_totals=electoral_totals, is_national_results=True)

        for result in national_serialized_results['US']:
            if result['party'] == 'Dem':
                self.assertEqual(result['npr_electwon'], 332)

    def test_npr_winner_determination(self):
        senate_results = render._select_senate_results()
        serialized_results = render._serialize_results(senate_results)

        for result in serialized_results['FL']:
            if result['party'] == 'Dem':
                self.assertTrue(result['npr_winner'])

    def test_attach_call_to_results(self):
        governor_results = render._select_governor_results()
        serialized_results = render._serialize_results(governor_results)

        self.assertTrue(serialized_results['ND'][0]['call']['accept_ap'])

    def test_attach_meta_to_results(self):
        presidential_results = render._select_presidential_state_results()
        serialized_results = render._serialize_results(presidential_results)

        self.assertEqual(serialized_results['KS'][0]['meta']['first_results'], '8:00 PM')

    def test_serialization(self):
        presidential_results = render._select_presidential_state_results()
        serialized_results = render._serialize_results(presidential_results)

        self.assertEqual(len(serialized_results.keys()), 51)

    def test_custom_key_serialization(self):
        county_results = render._select_presidential_county_results('FL')
        serialized_results = render._serialize_results(county_results, key='fipscode', apply_backrefs=False, determine_winner=False)

        self.assertEqual(len(serialized_results.keys()), 67)

if __name__ == '__main__':
    unittest.main()