import os
import shutil
import simplejson as json

from datetime import date, datetime
from fabric.api import execute, hide, local, task, settings, shell_env
from fabric.state import env
from models import models
from playhouse.shortcuts import model_to_dict
from pytz import timezone
from time import time

from . import utils


@task
def render_presidential_state_results():
    results = models.Result.select().where(
        (models.Result.level == 'state') | (models.Result.level == 'national') | (models.Result.level == 'district'),
        models.Result.officename == 'President',
        (models.Result.last == 'Obama') | (models.Result.last == 'Romney')
    )
    serialized_results = {}

    for result in results:
        if not serialized_results.get(result.statepostal):
            serialized_results[result.statepostal] = []

        obj = model_to_dict(result, backrefs=True)
        serialized_results[result.statepostal].append(obj)

    _write_json_file(serialized_results, 'presidential-national.json')

@task
def render_presidential_county_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = models.Result.select().where(
            (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district'),
            models.Result.officename == 'President',
            (models.Result.last == 'Obama') | (models.Result.last == 'Romney'),
            models.Result.statepostal == state.statepostal
        )

        serialized_results = {}

        for result in results:
            if not serialized_results.get(result.fipscode):
                serialized_results[result.fipscode] = []

            obj = model_to_dict(result, backrefs=True)
            serialized_results[result.fipscode].append(obj)

        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(serialized_results, filename)

@task
def render_governor_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'Governor'
    )

    serialized_results = {}

    for result in results:
        if not serialized_results.get(result.statepostal):
            serialized_results[result.statepostal] = []

        obj = model_to_dict(result, backrefs=True)
        serialized_results[result.statepostal].append(obj)

    _write_json_file(serialized_results, 'governor-national.json')

@task
def render_house_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. House',
        models.Result.raceid << app_config.SELECTED_HOUSE_RACES
    )

    serialized_results = {}

    for result in results:
        slug = '{0}-{1}'.format(result.statepostal, result.seatnum)

        if not serialized_results.get(result.statepostal):
            serialized_results[slug] = []

        obj = model_to_dict(result, backrefs=True)
        serialized_results[slug].append(obj)

    _write_json_file(serialized_results, 'house-national.json')

@task
def render_senate_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. Senate'
    )
    serialized_results = {}

    for result in results:
        if not serialized_results.get(result.statepostal):
            serialized_results[result.statepostal] = []

        obj = model_to_dict(result, backrefs=True)
        serialized_results[result.statepostal].append(obj)

    _write_json_file(serialized_results, 'senate-national.json')

@task
def render_ballot_measure_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.is_ballot_measure == True
    )

    serialized_results = {}

    for result in results:
        if not serialized_results.get(result.statepostal):
            serialized_results[result.statepostal] = []

        obj = model_to_dict(result, backrefs=True)
        serialized_results[result.statepostal].append(obj)

    _write_json_file(serialized_results, 'ballot-measures-national.json')

@task
def render_state_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        presidential = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.officename == 'President',
            (models.Result.last == 'Obama') | (models.Result.last == 'Romney'),
            models.Result.statepostal == state.statepostal
        )
        senate = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.officename == 'U.S. Senate',
            models.Result.statepostal == state.statepostal
        )
        house = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.officename == 'U.S. House',
            models.Result.statepostal == state.statepostal
        )
        governor = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.officename == 'Governor',
            models.Result.statepostal == state.statepostal
        )
        ballot_measures = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.is_ballot_measure == True,
            models.Result.statepostal == state.statepostal
        )

        # TODO: ballot initiatives

        state_results = {}
        state_results['presidential'] = [model_to_dict(result, backrefs=True) for result in presidential]
        state_results['senate'] = [model_to_dict(result, backrefs=True) for result in senate]
        state_results['house'] = [model_to_dict(result, backrefs=True) for result in house]
        state_results['governor'] = [model_to_dict(result, backrefs=True) for result in governor]
        state_results['ballot_measures'] = [model_to_dict(result, backrefs=True) for result in ballot_measures]

        filename = '{0}.json'.format(state.statepostal.lower())
        _write_json_file(state_results, filename)

def _write_json_file(serialized_results, filename):
    with open('.rendered/{0}'.format(filename), 'w') as f:
        json.dump(serialized_results, f, use_decimal=True, cls=utils.APDatetimeEncoder)

@task
def render_all():
    shutil.rmtree('.rendered')
    os.makedirs('.rendered')

    render_presidential_state_results()
    render_presidential_county_results()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    render_state_results()

@task
def render_all_national():
    render_presidential_state_results()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    # render_house_results()
    render_state_results()

@task
def render_presidential_files():
    render_presidential_state_results()
    render_presidential_county_results()