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

PRESIDENTIAL_SELECTIONS = [
    models.Result.call,
    models.Result.electtotal,
    models.Result.electwon,
    models.Result.fipscode,
    models.Result.first,
    models.Result.id,
    models.Result.last,
    models.Result.lastupdated,
    models.Result.level,
    models.Result.meta,
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

CALLS_SELECTIONS = [
    models.Call.accept_ap,
    models.Call.override_winner
]

RACE_META_SELECTIONS = [
    models.RaceMeta.poll_closing,
    models.RaceMeta.first_results
]

ACCEPTED_PRESIDENTIAL_CANDIDATES = ['Clinton', 'Johnson', 'Stein', 'Trump']


@task
def render_presidential_state_results():
    results = models.Result.select(*PRESIDENTIAL_SELECTIONS).where(
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
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = models.Result.select(*PRESIDENTIAL_SELECTIONS).where(
            (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district'),
            models.Result.officename == 'President',
            models.Result.statepostal == state.statepostal,
            models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
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
            models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES,
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