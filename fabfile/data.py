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
    load_results(app_config.ELEX_INIT_FLAGS)
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
        local('mkdir -p {0}'.format(app_config.ELEX_OUTPUT_FOLDER))

    cmd = 'elex results {0} {1} > {2}/first_query.csv'.format(election_date, flags, app_config.ELEX_OUTPUT_FOLDER)
    districts_cmd = 'elex results {0} {1} | csvgrep -c level -m district > {2}/districts.csv'.format(election_date, app_config.ELEX_DISTRICTS_FLAGS, app_config.ELEX_OUTPUT_FOLDER)


    with shell_env(**app_config.database):
        with settings(warn_only=True), hide('output', 'running'):
            first_cmd_output = local(cmd, capture=True)

        if first_cmd_output.succeeded:
            with hide('output', 'running'):
                district_cmd_output = local(districts_cmd, capture=True)

            if district_cmd_output.succeeded:
                delete_results()
                with hide('output', 'running'):
                    local('csvstack {0}/first_query.csv {1}/districts.csv | psql {2} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.ELEX_OUTPUT_FOLDER, app_config.ELEX_OUTPUT_FOLDER, app_config.database['PGDATABASE']))

            else:
                print("ERROR GETTING DISTRICT RESULTS")
                print(district_cmd_output.stderr)

        else:
            print("ERROR GETTING MAIN RESULTS")
            print(first_cmd_output.stderr)
        
    print('results loaded', )

@task
def create_calls():
    """
    Create database of race calls for all races in results data.
    """
    models.Call.delete().execute()

    results = models.Result.select().where(
        (models.Result.level == 'state') | (models.Result.level == 'national') | (models.Result.level == 'district')
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
            (models.Result.level == 'state') | (models.Result.level == 'district'),
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

    if app_config.NEXT_ELECTION_DATE[:4] == '2012':
        graphics_folder = '../elections16graphics/www/2012/data/'
    else:
        graphics_folder = '../elections16graphics/www/data/'

    local('cp -r {0}/* {1}'.format(app_config.DATA_OUTPUT_FOLDER, graphics_folder))