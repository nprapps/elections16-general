#!/usr/bin/env python

"""
Commands that update or process the application data.
"""
import app_config
import copytext
import csv
import yaml
import requests

from oauth import get_document
from fabric.api import execute, hide, local, task, settings, shell_env
from fabric.state import env
from models import models

CENSUS_REPORTER_URL = 'http://api.censusreporter.org/1.0/data/show/acs2014_5yr'
FIPS_TEMPLATE = '05000US{0}'
CENSUS_TABLES = ['B01001', 'B02001', 'B23006', 'B01003', 'B99211', 'B05001', 'B99181', 'B23025']

@task
def bootstrap_db():
    """
    Build the database.
    """
    create_db()
    create_tables()
    load_results('init')
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
    models.CensusData.create_table()

@task
def delete_results(mode):
    """
    Delete results without droppping database.
    """
    if mode == 'fast':
        where_clause = "WHERE level = 'state' OR level = 'national' OR level = 'district'"
    elif mode == 'slow':
        where_clause = "WHERE officename = 'President'"
    else:
        where_clause = ''

    with shell_env(**app_config.database), hide('output', 'running'):
        local('psql {0} -c "set session_replication_role = replica; DELETE FROM result {1}; set session_replication_role = default;"'.format(app_config.database['PGDATABASE'], where_clause))

@task
def load_results(mode):
    """
    Load AP results. Defaults to next election, or specify a date as a parameter.
    """

    if mode == 'fast':
        flags = app_config.FAST_ELEX_FLAGS
    elif mode == 'slow':
        flags = app_config.SLOW_ELEX_FLAGS
    else:
        flags = app_config.ELEX_INIT_FLAGS

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
                delete_results(mode)
                with hide('output', 'running'):
                    local('csvstack .data/first_query.csv .data/districts.csv | psql {0} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.database['PGDATABASE']))

            else:
                print("ERROR GETTING DISTRICT RESULTS")
                print(district_cmd_output.stderr)

        else:
            print("ERROR GETTING MAIN RESULTS")
            print(first_cmd_output.stderr)


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

@task
def build_current_congress():
    party_dict = {
        'Democrat': 'Dem',
        'Republican': 'GOP',
        'Independent': 'Ind'
    }

    house_fieldnames = ['first', 'last', 'party', 'state', 'district']
    senate_fieldnames = ['first', 'last', 'party', 'state']

    with open('data/house-seats.csv', 'w') as f:
        house_writer = csv.DictWriter(f, fieldnames=house_fieldnames)
        house_writer.writeheader()

        with open('data/senate-seats.csv', 'w') as f:
            senate_writer = csv.DictWriter(f, fieldnames=senate_fieldnames)
            senate_writer.writeheader()

            with open('etc/legislators-current.yaml') as f:
                data = yaml.load(f)

            for legislator in data:
                current_term = legislator['terms'][-1]

                if current_term['end'][:4] == '2017':
                    obj = {
                        'first': legislator['name']['first'],
                        'last': legislator['name']['last'],
                        'state': current_term['state'],
                        'party': party_dict[current_term['party']]
                    }

                    if current_term.get('district'):
                        obj['district'] = current_term['district']

                    if current_term['type'] == 'sen':
                        senate_writer.writerow(obj)
                    elif current_term['type'] == 'rep':
                        house_writer.writerow(obj)


@task
def get_census_data():
    with open('data/fipscodes.csv') as f:
        fips_reader = csv.DictReader(f)
        for row in fips_reader:
            save_census_row(row['fipscode'])
            break


@task
def save_census_row():
    fipscodes = models.Result.select(models.Result.fipscode, models.Result.id).where(
        models.Result.officename == 'President'
    )
    for fipscode in fipscodes:
        if fipscode.fipscode:
            print(fipscode.fipscode)
            geo_id = FIPS_TEMPLATE.format(fipscode.fipscode)
            params = {
                'geo_ids': geo_id,
                'table_ids': ','.join(CENSUS_TABLES)
            }
            response = requests.get(CENSUS_REPORTER_URL, params=params)
            models.CensusData.create(fipscode=fipscode.fipscode, data=response.json(), census_id=fipscode.id)
