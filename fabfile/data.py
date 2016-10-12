#!/usr/bin/env python

"""
Commands that update or process the application data.
"""
import app_config
import os
import shutil
import simplejson as json

from datetime import date, datetime
from oauth import get_document
from fabric.api import execute, hide, local, task, settings, shell_env
from fabric.state import env
from models import models
from playhouse.shortcuts import model_to_dict
from pytz import timezone
from time import time

import copytext
from . import utils

@task
def bootstrap_db():
    """
    Build the database.
    """
    create_db()
    create_tables()
    load_results(app_config.SLOW_ELEX_FLAGS)
    create_calls()
    # create_race_meta()

@task
def create_db():
    with settings(warn_only=True), hide('output', 'running'):
        if env.get('settings'):
            execute('servers.stop_service', 'uwsgi')
            execute('servers.stop_service', 'deploy')

        with shell_env(**app_config.database):
            local('dropdb --if-exists %s' % app_config.database['PGDATABASE'])

        if not env.get('settings'):
            local('psql -c "DROP USER IF EXISTS %s;"' % app_config.database['PGUSER'])
            local('psql -c "CREATE USER %s WITH SUPERUSER PASSWORD \'%s\';"' % (app_config.database['PGUSER'], app_config.database['PGPASSWORD']))

        with shell_env(**app_config.database):
            local('createdb %s' % app_config.database['PGDATABASE'])

        if env.get('settings'):
            execute('servers.start_service', 'uwsgi')
            execute('servers.start_service', 'deploy')

@task
def create_tables():
    models.Result.create_table()
    models.Call.create_table()
    models.RaceMeta.create_table()

@task
def delete_results():
    """
    Delete results without droppping database.
    """
    with shell_env(**app_config.database), hide('output', 'running'):
        local('psql {0} -c "set session_replication_role = replica; DELETE FROM result; set session_replication_role = default;"'.format(app_config.database['PGDATABASE']))

@task
def load_results(flags):
    """
    Load AP results. Defaults to next election, or specify a date as a parameter.
    """
    election_date = app_config.NEXT_ELECTION_DATE
    with hide('output', 'running'):
        local('mkdir -p .data')

    cmd = 'elex results {0} {1} > .data/first_query.csv'.format(election_date, flags)
    districts_cmd = 'elex results {0} {1} | csvgrep -c level -m district > .data/districts.csv'.format(election_date, app_config.ELEX_DISTRICTS_FLAGS)


    with shell_env(**app_config.database):
        first_cmd_output = local(cmd, capture=True)

        if first_cmd_output.succeeded:
            district_cmd_output = local(districts_cmd, capture=True)

            if district_cmd_output.succeeded:
                delete_results()
                with hide('output', 'running'):
                    local('csvstack .data/first_query.csv .data/districts.csv | psql {0} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.database['PGDATABASE']))

            else:
                print("ERROR GETTING DISTRICT RESULTS")
                print(cmd_output.stderr)

        else:
            print("ERROR GETTING MAIN RESULTS")
            print(cmd_output.stderr)
        

@task
def create_calls():
    """
    Create database of race calls for all races in results data.
    """
    models.Call.delete().execute()

    results = models.Result.select().where(
        models.Result.level == 'state'
    )

    for result in results:
        models.Call.create(call_id=result.id)

@task
def create_race_meta():
    models.RaceMeta.delete().execute()

    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'President'
    )

    calendar = copytext.Copy(app_config.CALENDAR_PATH)
    calendar_sheet = calendar['data']

    for row in calendar_sheet._serialize():
        if not row.get('full_poll_closing_time'):
            continue
        if row.get('status') == 'past':
            continue

        results = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.statename == row['state_name'],
            models.Result.officename == 'President'
        )

        for result in results:
            race_type = row['type'].lower()
            models.RaceMeta.create(
                    result_id=result.id,
                    race_type=race_type,
                    poll_closing=row['full_poll_closing_time'],
                    order=row['ordinal']
            )

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

@task
def copy_data_for_graphics():
    render_all()
    local('cp -r .rendered/* ../elections16graphics/www/data/')