#!/usr/bin/env python

"""
Commands that update or process the application data.
"""
import app_config

from oauth import get_document
from fabric.api import hide, local, task, settings, shell_env
from fabric.state import env
from models import models

import copytext
import servers

@task
def bootstrap_db():
    """
    Build the database.
    """
    create_db()
    create_tables()
    load_results()
    create_calls()
    # create_race_meta()

@task
def create_db():
    with settings(warn_only=True), hide('output', 'running'):
        if env.get('settings'):
            servers.stop_service('uwsgi')
            servers.stop_service('deploy')

        with shell_env(**app_config.database):
            local('dropdb --if-exists %s' % app_config.database['PGDATABASE'])

        if not env.get('settings'):
            local('psql -c "DROP USER IF EXISTS %s;"' % app_config.database['PGUSER'])
            local('psql -c "CREATE USER %s WITH SUPERUSER PASSWORD \'%s\';"' % (app_config.database['PGUSER'], app_config.database['PGPASSWORD']))

        with shell_env(**app_config.database):
            local('createdb %s' % app_config.database['PGDATABASE'])

        if env.get('settings'):
            servers.start_service('uwsgi')
            servers.start_service('deploy')

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
def load_results():
    """
    Load AP results. Defaults to next election, or specify a date as a parameter.
    """
    election_date = app_config.NEXT_ELECTION_DATE
    with hide('output', 'running'):
        local('mkdir -p .data')
    cmd = 'elex results {0} {1} > .data/results.csv'.format(election_date, app_config.ELEX_FLAGS)
    with shell_env(**app_config.database):
        with settings(warn_only=True), hide('output', 'running'):
            cmd_output = local(cmd, capture=True)

        if cmd_output.succeeded:
            delete_results()
            with hide('output', 'running'):
                local('cat .data/results.csv | psql {0} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.database['PGDATABASE']))
        else:
            print("ERROR GETTING RESULTS")
            print(cmd_output.stderr)

@task
def create_calls():
    """
    Create database of race calls for all races in results data.
    """
    models.Call.delete().execute()

    results = models.Result.select().where(
        models.Result.level == 'state',
        models.Result.officename == 'President'
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