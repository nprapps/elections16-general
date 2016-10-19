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
    models.Result.raceid,
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

ACCEPTED_PRESIDENTIAL_CANDIDATES = ['Trump', 'Johnson', 'Stein', 'Clinton', 'McMullin']

SELECTIONS_LOOKUP = {
    'president': PRESIDENTIAL_STATE_SELECTIONS,
    'governor': GOVERNOR_SELECTIONS,
    'senate': SENATE_SELECTIONS,
    'house': HOUSE_SELECTIONS,
    'ballot_measures': BALLOT_MEASURE_SELECTIONS
}

@task
def render_top_level_numbers():
    presidential_results = _select_presidential_state_results()
    senate_results = _select_senate_results()
    house_results = _select_all_house_results()

    electoral_totals = {
        'Dem': 0,
        'GOP': 0,
        'Ind': 0,
        'Lib': 0,
        'Grn': 0
    }

    senate_bop = {
        'Dem': {
            'seats': 34,
            'pickups': 0,
        },
        'GOP': {
            'seats': 30,
            'pickups': 0,
        },
        'Ind': {
            'seats': 2,
            'pickups': 0,
        }
    }

    house_bop = {
        'Dem': {
            'seats': 0,
            'pickups': 0,
        },
        'GOP': {
            'seats': 0,
            'pickups': 0,
        },
        'Ind': {
            'seats': 0,
            'pickups': 0,
        },
        'Lib': {
            'seats': 0,
            'pickups': 0
        },
        'Grn': {
            'seats': 0,
            'pickups': 0
        },
        'NPP': {
            'seats': 0,
            'pickups': 0
        },
        'Oth': {
            'seats': 0,
            'pickups': 0
        }
    }

    for result in presidential_results:
        if result.is_npr_winner():
            electoral_totals[result.party] += result.electwon

    for result in senate_results:
        if result.is_npr_winner():
            senate_bop[result.party]['seats'] += 1
        if result.is_pickup():
            senate_bop[result.party]['pickups'] += 1
            senate_bop[result.meta[0].current_party]['pickups'] -= 1

    for result in house_results:
        if result.is_npr_winner():
            house_bop[result.party]['seats'] += 1
        if result.is_pickup():
            house_bop[result.party]['pickups'] += 1
            house_bop[result.meta[0].current_party]['pickups'] -= 1

    data = {
        'electoral_college': electoral_totals,
        'senate_bop': senate_bop,
        'house_bop': house_bop
    }

    print(data)

@task
def render_presidential_state_results():
    results = _select_presidential_state_results()

    electoral_totals = {}
    serialized_results = _serialize_results(results, PRESIDENTIAL_STATE_SELECTIONS)

    # now that we have correct electoral counts, get the national results
    # this sucks
    national_results = _select_presidential_national_results()

    national_serialized_results = _serialize_results(national_results, PRESIDENTIAL_STATE_SELECTIONS)

    all_results = {**serialized_results, **national_serialized_results}

    _write_json_file(all_results, 'presidential-national.json')

@task
def render_presidential_county_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = _select_presidential_county_results(state.statepostal)
        serialized_results = _serialize_results(results, PRESIDENTIAL_COUNTY_SELECTIONS, key='fipscode', determine_winner=False)
        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(serialized_results, filename)

@task
def render_governor_results():
    results = _select_governor_results()

    serialized_results = _serialize_results(results, GOVERNOR_SELECTIONS)
    _write_json_file(serialized_results, 'governor-national.json')

@task
def render_house_results():
    results = _select_selected_house_results()

    serialized_results = _serialize_results(results, HOUSE_SELECTIONS, key='raceid', determine_pickup=True)
    _write_json_file(serialized_results, 'house-national.json')

@task
def render_senate_results():
    results = _select_senate_results()

    serialized_results = _serialize_results(results, SENATE_SELECTIONS, determine_pickup=True)
    _write_json_file(serialized_results, 'senate-national.json')

@task
def render_ballot_measure_results():
    results = _select_ballot_measure_results()

    serialized_results = _serialize_results(results, BALLOT_MEASURE_SELECTIONS)
    _write_json_file(serialized_results, 'ballot-measures-national.json')


@task
def render_state_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        president = models.Result.select().where(
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

        state_results = {}
        queries = [president, senate, house, governor, ballot_measures]
        for query in queries:
            results_key = [ k for k,v in locals().items() if v is query][0]
            if results_key != 'query':
                selectors = SELECTIONS_LOOKUP[results_key]
                state_results[results_key] = _serialize_results(query, selectors)

        filename = '{0}.json'.format(state.statepostal.lower())
        _write_json_file(state_results, filename)


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
        (models.Result.level == 'county') | (models.Result.level == 'township') | (models.Result.level == 'district'),
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


def _serialize_results(results, selections, key='statepostal', determine_winner=True, determine_pickup=False):
    serialized_results = {}

    for result in results:
        result_dict = model_to_dict(result, backrefs=True, only=selections)

        if not serialized_results.get(result_dict[key]):
            serialized_results[result_dict[key]] = []

        if result.level != 'county' and result.level != 'township':
            meta = models.RaceMeta.get(models.RaceMeta.result_id == result.id)
            result_dict['meta'] = model_to_dict(meta, only=RACE_META_SELECTIONS)

        if determine_winner:
            result_dict['npr_winner'] = result.is_npr_winner()

        if determine_pickup:
            result_dict['pickup'] = result.is_pickup()

        if result.level == 'national':
            electoral_votes = _calculate_electoral_votes(result)
            result_dict['npr_electwon'] = electoral_votes

        serialized_results[result_dict[key]].append(result_dict)

    return serialized_results


def _calculate_electoral_votes(result):
    party_results = models.Result.select().where(
        (models.Result.level == 'state') | (models.Result.level == 'district'),
        models.Result.officename == 'President',
        models.Result.party == result.party
    )

    total_votes = 0
    for result in party_results:
        if not (result.level == 'state' and (result.statename == 'Maine' or result.statename == 'Nebraska')):
            if result.is_npr_winner():
                total_votes += result.electwon

    return total_votes

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