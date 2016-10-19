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

PRESIDENTIAL_STATE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.reportingunitname
]

PRESIDENTIAL_COUNTY_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.reportingunitname,
    models.Result.fipscode
]

HOUSE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.runoff,
    models.Result.raceid,
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
    models.RaceMeta.first_results,
    models.RaceMeta.current_party
]

ACCEPTED_PRESIDENTIAL_CANDIDATES = ['Obama', 'Johnson', 'Stein', 'Romney', 'McMullin']


@task
def render_presidential_state_results():
    results = models.Result.select(*PRESIDENTIAL_STATE_SELECTIONS).where(
        (models.Result.level == 'state') | (models.Result.level == 'district'),
        models.Result.officename == 'President',
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
    ).dicts()
    
    electoral_totals = {}
    serialized_results = _serialize_results(results, electoral_totals=electoral_totals)

    # now that we have correct electoral counts, get the national results
    # this sucks
    national_results = models.Result.select(*PRESIDENTIAL_STATE_SELECTIONS).where(
        models.Result.level == 'national',
        models.Result.officename == 'President',
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
    ).dicts()

    national_serialized_results = _serialize_results(national_results, determine_winner=False, electoral_totals=electoral_totals, is_national_results=True)

    all_results = {**serialized_results, **national_serialized_results}

    _write_json_file(all_results, 'presidential-national.json')

@task
def render_presidential_county_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = models.Result.select(*PRESIDENTIAL_COUNTY_SELECTIONS).where(
            (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district'),
            models.Result.officename == 'President',
            models.Result.statepostal == state.statepostal,
            models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
        ).dicts()

        serialized_results = _serialize_results(results, key='fipscode', apply_backrefs=False, determine_winner=False)

        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(serialized_results, filename)

@task
def render_governor_results():
    results = models.Result.select(*GOVERNOR_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'Governor'
    ).dicts()

    serialized_results = _serialize_results(results)
    _write_json_file(serialized_results, 'governor-national.json')

@task
def render_house_results():
    results = models.Result.select(*HOUSE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. House',
        models.Result.raceid << app_config.SELECTED_HOUSE_RACES
    ).dicts()

    serialized_results = _serialize_results(results, key='raceid')
    _write_json_file(serialized_results, 'house-national.json')

@task
def render_senate_results():
    results = models.Result.select(*SENATE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. Senate'
    ).dicts()

    serialized_results = _serialize_results(results)
    _write_json_file(serialized_results, 'senate-national.json')

@task
def render_ballot_measure_results():
    results = models.Result.select(*BALLOT_MEASURE_SELECTIONS).where(
        models.Result.level == 'state',
        models.Result.is_ballot_measure == True
    ).dicts()

    serialized_results = _serialize_results(results)
    _write_json_file(serialized_results, 'ballot-measures-national.json')


@task
def render_state_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        presidential = models.Result.select(*PRESIDENTIAL_STATE_SELECTIONS).where(
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
            results_key = [ k for k,v in locals().items() if v is query][0]
            state_results[results_key] = _serialize_results(query)

        filename = '{0}.json'.format(state.statepostal.lower())
        _write_json_file(state_results, filename)


def _serialize_results(results, key='statepostal', apply_backrefs=True, determine_winner=True, electoral_totals={}, is_national_results=False):
    serialized_results = {}

    for result in results:
        if not serialized_results.get(result[key]):
            serialized_results[result[key]] = []

        if apply_backrefs:
            _set_call(result)
            _set_meta(result)

        if determine_winner:
            _determine_winner(result, electoral_totals)

        if is_national_results:
            _set_electoral_counts(result, electoral_totals)

        serialized_results[result[key]].append(result)

    return serialized_results

def _write_json_file(serialized_results, filename):
    with open('{0}/{1}'.format(app_config.DATA_OUTPUT_FOLDER, filename), 'w') as f:
        json.dump(serialized_results, f, use_decimal=True, cls=utils.APDatetimeEncoder)

def _set_call(result):
    call = models.Call.get(models.Call.call_id == result['id'])
    result['call'] = model_to_dict(call, only=CALLS_SELECTIONS)

def _set_meta(result):
    if result['level'] != 'national':
        meta = models.RaceMeta.get(models.RaceMeta.result_id == result['id'])
        result['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

def _determine_winner(result, electoral_totals={}):
    if not electoral_totals.get(result['party']):
        electoral_totals[result['party']] = 0

    if (result['winner'] and result['call']['accept_ap']) or result['call']['override_winner']:
        result['npr_winner'] = True
        
        if result['officename'] == 'President':
            if not (result['level'] == 'state' and (result['statename'] == 'Maine' or result['statename'] == 'Nebraska')):
                electoral_totals[result['party']] += int(result['electwon'])

        if result['officename'] == 'U.S. Senate' or result['officename'] == 'U.S. House':
            if result['party'] != result['meta']['current_party']:
                result['pickup'] = True

def _set_electoral_counts(result, electoral_totals):
    party = result['party']
    result['npr_electwon'] = electoral_totals[party]


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