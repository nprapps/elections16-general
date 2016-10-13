#!/usr/bin/env python

"""
Commands that update or process the application data.
"""
import app_config

from oauth import get_document
from fabric.api import execute, hide, local, task, settings, shell_env
from fabric.state import env
from models import models

import copytext

@task
def bootstrap_db():
    """
    Build the database.
    """
    create_db()
    create_tables()
    load_results(app_config.SLOW_ELEX_FLAGS)
    create_calls()
    create_race_meta()

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

    calendar = copytext.Copy(app_config.CALENDAR_PATH)
    calendar_sheet = calendar['polls']

    for row in calendar_sheet:

        results = models.Result.select().where(
            models.Result.level == 'state',
            models.Result.statepostal == row['key'],
        )

        for result in results:
            models.RaceMeta.create(
                result_id=result.id,
                poll_closing=row['time_est'],
                first_results=row['first_results_est']
            )

@task
def copy_data_for_graphics():
    execute('render.render_all')
    local('cp -r .rendered/* ../elections16graphics/www/data/')