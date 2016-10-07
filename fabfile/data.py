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
from fabric.api import hide, local, task, settings, shell_env
from fabric.state import env
from models import models
from playhouse.shortcuts import model_to_dict
from pytz import timezone
from time import time

import copytext
from . import servers
from . import utils

@task
def bootstrap_db():
    """
    Build the database.
    """
    create_db()
    create_tables()
    load_results(app_config.FAST_ELEX_FLAGS)
    create_calls()
    # create_race_meta()

@task
def create_db():
    with settings(warn_only=True), hide('output', 'running'):
        if env.get('settings'):
            servers.stop_service('uwsgi')
            servers.stop_service('deploy')

        with shell_env(**app_config.database):
            print('dropping db')
            local('dropdb --if-exists %s' % app_config.database['PGDATABASE'])

        if not env.get('settings'):
            print('dropping user')
            local('psql -c "DROP USER IF EXISTS %s;"' % app_config.database['PGUSER'])
            print('creating user')
            local('psql -c "CREATE USER %s WITH SUPERUSER PASSWORD \'%s\';"' % (app_config.database['PGUSER'], app_config.database['PGPASSWORD']))

        with shell_env(**app_config.database):
            print('creating db')
            local('createdb %s' % app_config.database['PGDATABASE'])

        if env.get('settings'):
            # servers.start_service('uwsgi')
            # servers.start_service('deploy')

@task
def create_tables():
    print('creating tables')
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

    cmd = 'elex results {0} {1} > .data/results.csv'.format(election_date, flags)
    with shell_env(**app_config.database):
        with settings(warn_only=True), hide('output', 'running'):
            cmd_output = local(cmd, capture=True)

        if cmd_output.succeeded:
            delete_results()
            with hide('output', 'running'):
                local('cat .data/results.csv | psql {0} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.database['PGDATABASE']))
            print('results loaded')
        else:
            print("ERROR GETTING RESULTS")
            print(cmd_output.stderr)

@task
def create_calls():
    """
    Create database of race calls for all races in results data.
    """
    print('creating calls')
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
        models.Result.level == 'state',
        models.Result.officename == 'President',
        (models.Result.last == 'Obama') | (models.Result.last == 'Romney')
    )
    json_string = _write_json(results)

    filename = 'presidential-national.json'
    _write_json_file(json_string, filename)

@task
def render_presidential_county_results():
    states = models.Result.select(models.Result.statepostal).distinct()

    for state in states:
        results = models.Result.select().where(
            models.Result.level == 'county',
            models.Result.officename == 'President',
            (models.Result.last == 'Obama') | (models.Result.last == 'Romney'),
            models.Result.statepostal == state
        )
        json_string = _write_json(results)

        filename = 'presidential-{0}-counties.json'.format(state.statepostal.lower())
        _write_json_file(json_string, filename)

@task
def render_governor_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'Governor'
    )
    json_string = _write_json(results)

    filename = 'governor-national.json'
    _write_json_file(json_string, filename)

@task
def render_senate_results():
    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'U.S. Senate'
    )
    json_string = _write_json(results)

    filename = 'senate-national.json'
    _write_json_file(json_string, filename)

def _write_json(results):
    serialized_results = []

    for result in results:
        obj = model_to_dict(result, backrefs=True)
        serialized_results.append(obj)

    return json.dumps(serialized_results, use_decimal=True, cls=utils.APDatetimeEncoder)

def _write_json_file(json_string, filename):
    with open('.rendered/{0}'.format(filename), 'w') as f:
        f.write(json_string)

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

        # TODO: ballot initiatives

        state_results = {}
        state_results['presidential'] = [model_to_dict(result, backrefs=True) for result in presidential]
        state_results['senate'] = [model_to_dict(result, backrefs=True) for result in senate]
        state_results['house'] = [model_to_dict(result, backrefs=True) for result in house]
        state_results['governor'] = [model_to_dict(result, backrefs=True) for result in governor]

        filename = '{0}.json'.format(state.statepostal.lower())
        with open('.rendered/{0}'.format(filename), 'w') as f:
            json.dump(state_results, f, use_decimal=True, cls=utils.APDatetimeEncoder)

@task
def render_all():
    shutil.rmtree('.rendered')
    os.makedirs('.rendered')

    render_presidential_state_results()
    render_presidential_county_results()
    render_senate_results()
    render_governor_results()
    render_state_results()

@task
def render_all_national():
    render_presidential_state_results()
    render_senate_results()
    render_governor_results()
    # render_house_results()
    render_state_results()

@task
def render_presidential_files():
    render_presidential_state_results()
    render_presidential_county_results()
