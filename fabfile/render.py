import app_config
import os
import re
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
    models.Result.raceid,
    models.Result.statename,
    models.Result.statepostal,
    models.Result.votepct,
    models.Result.votecount,
    models.Result.winner
]

PRESIDENTIAL_STATE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.reportingunitname,
    models.Result.meta
]

PRESIDENTIAL_COUNTY_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.reportingunitname,
    models.Result.fipscode
]

HOUSE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.runoff,
    models.Result.seatname,
    models.Result.seatnum,
    models.Result.meta
]

SENATE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.runoff,
    models.Result.meta
]

GOVERNOR_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.incumbent,
    models.Result.meta
]

BALLOT_MEASURE_SELECTIONS = COMMON_SELECTIONS + [
    models.Result.officename,
    models.Result.seatname,
    models.Result.is_ballot_measure,
    models.Result.meta
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

ACCEPTED_PRESIDENTIAL_CANDIDATES = ['Clinton', 'Johnson', 'Stein', 'Trump', 'McMullin']

SELECTIONS_LOOKUP = {
    'president': PRESIDENTIAL_STATE_SELECTIONS,
    'governor': GOVERNOR_SELECTIONS,
    'senate': SENATE_SELECTIONS,
    'house': HOUSE_SELECTIONS,
    'ballot_measures': BALLOT_MEASURE_SELECTIONS
}


def _select_presidential_state_results():
    results = models.Result.select().where(
        (models.Result.level == 'state') | (models.Result.level == 'district'),
        models.Result.officename == 'President',
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
    )

    return results

def _select_presidential_national_results():
    results = models.Result.select().where(
        models.Result.level == 'national',
        models.Result.officename == 'President',
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES
    )

    return results

def _select_presidential_county_results(statepostal):
    results = models.Result.select().where(
        (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district') | (models.Result.level == 'state'),
        models.Result.officename == 'President',
        models.Result.statepostal == statepostal,
        models.Result.last << ACCEPTED_PRESIDENTIAL_CANDIDATES,
    )

    return results

def _select_governor_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'Governor'
    )

    return results

def _select_selected_house_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. House',
        models.Result.raceid << app_config.SELECTED_HOUSE_RACES
    )

    return results

def _select_all_house_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. House',
    )

    return results

def _select_senate_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. Senate'
    )

    return results

def _select_ballot_measure_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.is_ballot_measure == True
    )

    return results

@task
def render_top_level_numbers():
    # init with parties that already have seats
    senate_bop = {
        'total_seats': 100,
        'majority': 51,
        'uncalled_races': 34,
        'Dem': {
            'seats': 34,
            'pickups': 0,
            'needed': 17
        },
        'GOP': {
            'seats': 30,
            'pickups': 0,
            'needed': 21
        },
        'Other': {
            'seats': 2,
            'pickups': 0,
            'needed': 49
        }
    }

    house_bop = {
        'total_seats': 435,
        'majority': 218,
        'uncalled_races': 435,
        'Dem': {
            'seats': 0,
            'pickups': 0,
            'needed': 218
        },
        'GOP': {
            'seats': 0,
            'pickups': 0,
            'needed': 218
        },
        'Other': {
            'seats': 0,
            'pickups': 0,
            'needed': 218
        }
    }

    presidential_results = _select_presidential_state_results()
    senate_results = _select_senate_results()
    house_results = _select_all_house_results()

    electoral_totals = _calculate_electoral_votes(presidential_results)

    for result in senate_results:
        _calculate_bop(result, senate_bop)

    for result in house_results:
        _calculate_bop(result, house_bop)

    data = {
        'electoral_college': electoral_totals,
        'senate_bop': senate_bop,
        'house_bop': house_bop
    }

    _write_json_file(data, 'top-level-results.json')

@task
def render_presidential_state_results():
    state_results = _select_presidential_state_results()
    national_results = _select_presidential_national_results()
    electoral_totals = _calculate_electoral_votes(state_results)
    state_serialized_results = _serialize_by_key(state_results, PRESIDENTIAL_STATE_SELECTIONS, 'statepostal')
    national_serialized_results = _serialize_by_key(national_results, PRESIDENTIAL_STATE_SELECTIONS, 'statepostal')

    for result in national_serialized_results['US']:
        result['npr_electwon'] = electoral_totals[result['party']]

    all_results = {**state_serialized_results, **national_serialized_results}

    _write_json_file(all_results, 'presidential-national.json')

@task
def render_presidential_county_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = _select_presidential_county_results(state.statepostal)
        serialized_results = _serialize_by_key(results, PRESIDENTIAL_COUNTY_SELECTIONS, 'fipscode')

        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(serialized_results, filename)

@task
def render_presidential_big_board():
    results = _select_presidential_state_results()
    serialized_results = _serialize_for_big_board(results, PRESIDENTIAL_STATE_SELECTIONS, key='statepostal')
    _write_json_file(serialized_results, 'presidential-big-board.json')

@task
def render_governor_results():
    results = _select_governor_results()

    serialized_results = _serialize_for_big_board(results, GOVERNOR_SELECTIONS)
    _write_json_file(serialized_results, 'governor-national.json')

@task
def render_house_results():
    results = _select_selected_house_results()

    serialized_results = _serialize_for_big_board(results, HOUSE_SELECTIONS)
    _write_json_file(serialized_results, 'house-national.json')

@task
def render_senate_results():
    results = _select_senate_results()

    serialized_results = _serialize_for_big_board(results, SENATE_SELECTIONS)
    _write_json_file(serialized_results, 'senate-national.json')

@task
def render_ballot_measure_results():
    results = _select_ballot_measure_results()

    serialized_results = _serialize_for_big_board(results, BALLOT_MEASURE_SELECTIONS)
    _write_json_file(serialized_results, 'ballot-measures-national.json')


@task
def render_state_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
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

        state_results = {}
        queries = [senate, house, governor, ballot_measures]
        for query in queries:
            results_key = [ k for k,v in locals().items() if v is query][0]
            if results_key != 'query':
                selectors = SELECTIONS_LOOKUP[results_key]
                state_results[results_key] = _serialize_by_key(query, selectors, 'raceid')

        filename = '{0}.json'.format(state.statepostal.lower())
        _write_json_file(state_results, filename)

uncallable_levels = ['county', 'township']
pickup_offices = ['U.S. House', 'U.S. Senate']

def _serialize_for_big_board(results, selections, key='raceid'):
    serialized_results = {}

    for result in results:
        result_dict = model_to_dict(result, backrefs=True, only=selections)
        if result.level not in uncallable_levels:
            _set_meta(result, result_dict)

        if result.officename in pickup_offices:
            _set_pickup(result, result_dict)

        if not serialized_results.get(result.meta[0].first_results):
            serialized_results[result.meta[0].first_results] = {}
        
        # handle district-level presidential results
        if key == 'statepostal' and result.reportingunitname:
            if result.reportingunitname == 'At Large':
                continue

            m = re.search(r'\d$', result.reportingunitname)
            if m is not None:
                dict_key = '{0}-{1}'.format(result.statepostal, m.group())
            else:
                dict_key = result.statepostal
        else:
            dict_key = result_dict[key]

        time_bucket = serialized_results[result.meta[0].first_results]
        if not time_bucket.get(dict_key):
            time_bucket[dict_key] = []

        time_bucket[dict_key].append(result_dict)

    return serialized_results


def _serialize_by_key(results, selections, key):
    serialized_results = {}

    for result in results:
        result_dict = model_to_dict(result, backrefs=True, only=selections)

        if result.level not in uncallable_levels:
            _set_meta(result, result_dict)

        if result.officename in pickup_offices:
            _set_pickup(result, result_dict)

        # handle state results in the county files
        if key == 'fipscode' and result.level == 'state':
            dict_key = 'state'
        else:
            dict_key = result_dict[key]

        if not serialized_results.get(result_dict[key]):
            serialized_results[dict_key] = []

        serialized_results[dict_key].append(result_dict)

    return serialized_results

def _set_meta(result, result_dict):
    meta = models.RaceMeta.get(models.RaceMeta.result_id == result.id)
    result_dict['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)
    result_dict['npr_winner'] = result.is_npr_winner()

def _set_pickup(result, result_dict):
    result_dict['pickup'] = result.is_pickup()

def _calculate_electoral_votes(results):
    electoral_totals = {
        'Dem': 0,
        'GOP': 0,
        'Ind': 0,
        'Lib': 0,
        'Grn': 0,
        'BFA': 0
    }

    for result in results:
        if result.is_npr_winner():
            if not (result.level == 'state' and (result.statename == 'Maine' or result.statename == 'Nebraska')):
                electoral_totals[result.party] += result.electtotal

    return electoral_totals

def _calculate_bop(result, bop):
    ACCEPTED_PARTIES = ['Dem', 'GOP']

    party = result.party if result.party in ACCEPTED_PARTIES else 'Other'
    if result.is_npr_winner():
        bop[party]['seats'] += 1
        bop[party]['needed'] -= 1
        bop['uncalled_races'] -= 1

    if result.is_pickup():
        bop[party]['pickups'] += 1
        bop[result.meta[0].current_party]['pickups'] -= 1



def _write_json_file(serialized_results, filename):
    with open('{0}/{1}'.format(app_config.DATA_OUTPUT_FOLDER, filename), 'w') as f:
        json.dump(serialized_results, f, use_decimal=True, cls=utils.APDatetimeEncoder)

@task
def render_all():
    shutil.rmtree('{0}'.format(app_config.DATA_OUTPUT_FOLDER))
    os.makedirs('{0}'.format(app_config.DATA_OUTPUT_FOLDER))

    render_top_level_numbers()
    render_presidential_state_results()
    render_presidential_county_results()
    render_presidential_big_board()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    render_house_results()
    render_state_results()

@task
def render_all_national():
    render_top_level_numbers()
    render_presidential_state_results()
    render_presidential_big_board()
    render_senate_results()
    render_governor_results()
    render_ballot_measure_results()
    render_house_results()
    render_state_results()

@task
def render_presidential_files():
    render_top_level_numbers()
    render_presidential_state_results()
    render_presidential_county_results()
    render_presidential_big_board()
