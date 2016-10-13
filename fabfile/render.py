import app_config
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

COMMON_SELECTIONS = [
    models.Result.electtotal,
    models.Result.electwon,
    models.Result.first,
    models.Result.id,
    models.Result.last,
    models.Result.lastupdated,
    models.Result.level,
    models.Result.officename,
    models.Result.party,
    models.Result.precinctsreporting,
    models.Result.precinctsreportingpct,
    models.Result.precinctstotal,
    models.Result.statename,
    models.Result.statepostal,
    models.Result.votepct,
    models.Result.votecount,
    models.Result.winner
]

PRESIDENTIAL_COUNTY_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.fipscode
]

HOUSE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.runoff,
    models.Result.seatname,
    models.Result.seatnum
]

SENATE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.runoff
]

GOVERNOR_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent
]

BALLOT_MEASURE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.officename,
    models.Result.seatname
]

CALLS_SELECTIONS = [
    models.Call.accept_ap,
    models.Call.override_winner
]

RACE_META_SELECTIONS = [
    models.RaceMeta.poll_closing,
    models.RaceMeta.first_results
]

ACCEPTED_PRESIDENTIAL_CANDIDATES = ['Johnson', 'Obama', 'Romney', 'Stein']


@task
def render_presidential_state_results():
    results = models.Result.select(*COMMON_SELECTIONS).where(
        (models.Result.level == 'state') | (models.Result.level == 'national') | (models.Result.level == 'district'),
        models.Result.officename == 'President',
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
    ).dicts()
    
    serialized_results = {}

    for result in results:
        if not serialized_results.get(result['statepostal']):
            serialized_results[result['statepostal']] = []

        call = models.Call.get(models.Call.call_id == result['id'])
        result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

        # no race meta for national race
        if result['level'] != 'national':
            meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
            result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        serialized_results[result['statepostal']].append(result)

    _write_json_file(serialized_results, 'presidential-national.json')

@task
def render_presidential_county_results():
    states = models.Result.select(*PRESIDENTIAL_COUNTY_SELECTIONS).distinct()

    for state in states:
        results = models.Result.select(*selections).where(
            (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district'),
            models.Result.officename == 'President',
            models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES,
            models.Result.statepostal == state.statepostal
        ).dicts()

        serialized_results = {}

        for result in results:
            if not serialized_results.get(result['fipscode']):
                serialized_results[result['fipscode']] = []

            serialized_results[result['fipscode']].append(result)

        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(serialized_results, filename)

@task
def render_governor_results():
    results = models.Result.select(*GOVERNOR_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'Governor'
    ).dicts()

    serialized_results = {}

    for result in results:
        if not serialized_results.get(result['statepostal']):
            serialized_results[result['statepostal']] = []

        call = models.Call.get(models.Call.call_id == result['id'])
        result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

        meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
        result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        serialized_results[result['statepostal']].append(result)

    _write_json_file(serialized_results, 'governor-national.json')

@task
def render_house_results():
    results = models.Result.select(*HOUSE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. House',
        models.Result.raceid << app_config.SELECTED_HOUSE_RACES
    ).dicts()

    serialized_results = {}

    for result in results:
        slug = '{0}-{1}'.format(result['statepostal'], ['result.seatnum'])

        if not serialized_results.get(slug):
            serialized_results[slug] = []

        call = models.Call.get(models.Call.call_id == result['id'])
        result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

        meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
        result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        serialized_results[slug].append(result)

    _write_json_file(serialized_results, 'house-national.json')

@task
def render_senate_results():
    results = models.Result.select(*SENATE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. Senate'
    ).dicts()

    serialized_results = {}

    for result in results:
        if not serialized_results.get(result['statepostal']):
            serialized_results[result['statepostal']] = []

        call = models.Call.get(models.Call.call_id == result['id'])
        result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

        meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
        result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        serialized_results[result['statepostal']].append(result)

    _write_json_file(serialized_results, 'senate-national.json')

@task
def render_ballot_measure_results():
    results = models.Result.select(*BALLOT_MEASURE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.is_ballot_measure == True
    ).dicts()

    serialized_results = {}

    for result in results:
        if not serialized_results.get(result['statepostal']):
            serialized_results[result['statepostal']] = []

        call = models.Call.get(models.Call.call_id == result['id'])
        result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

        meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
        result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        serialized_results[result['statepostal']].append(result)

    _write_json_file(serialized_results, 'ballot-measures-national.json')

@task
def render_state_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        presidential = models.Result.select(*COMMON_SELECTIONS).where(
            models.Result.level == 'state',
            models.Result.officename == 'President',
            models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES,
            models.Result.statepostal == state.statepostal
        ).dicts()
        senate = models.Result.select(*SENATE_SELECTIONS).where(
            models.Result.level == 'state',
            models.Result.officename == 'U.S. Senate',
            models.Result.statepostal == state.statepostal
        ).dicts()
        house = models.Result.select(*HOUSE_SELECTIONS).where(
            models.Result.level == 'state',
            models.Result.officename == 'U.S. House',
            models.Result.statepostal == state.statepostal
        ).dicts()
        governor = models.Result.select(*GOVERNOR_SELECTIONS).where(
            models.Result.level == 'state',
            models.Result.officename == 'Governor',
            models.Result.statepostal == state.statepostal
        ).dicts()
        ballot_measures = models.Result.select(*BALLOT_MEASURE_SELECTIONS).where(
            models.Result.level == 'state',
            models.Result.is_ballot_measure == True,
            models.Result.statepostal == state.statepostal
        ).dicts()

        state_results = {}
        queries = [presidential, senate, house, governor, ballot_measures]
        for query in queries:
            results_key = [ k for k,v in locals().iteritems() if v is query][0]
            state_results[results_key] = []

            for result in query:
                call = models.Call.get(models.Call.call_id == result['id'])
                result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

                meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
                result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

                state_results[results_key].append(result)


        filename = '{0}.json'.format(state.statepostal.lower())
        _write_json_file(state_results, filename)

def _write_json_file(serialized_results, filename):
    with open('{0}/{1}'.format(app_config.DATA_OUTPUT_FOLDER, filename), 'w') as f:
        json.dump(serialized_results, f, use_decimal=True, cls=utils.APDatetimeEncoder)

@task
def render_all():
    shutil.rmtree('{0}'.format(app_config.DATA_OUTPUT_FOLDER))
    os.makedirs('{0}'.format(app_config.DATA_OUTPUT_FOLDER))

    render_presidential_state_results()
    render_presidential_county_results()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    render_house_results()
    render_state_results()

@task
def render_all_national():
    render_presidential_state_results()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    render_house_results()
    render_state_results()

@task
def render_presidential_files():
    render_presidential_state_results()
    render_presidential_county_results()