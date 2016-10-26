#!/usr/bin/env python

"""
Commands that update or process the application data.
"""
import app_config
import copytext
import csv
import yaml
import json

from oauth import get_document
from fabric.api import execute, hide, local, task, settings, shell_env
from fabric.state import env
from models import models

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
                delete_results(mode)
                with hide('output', 'running'):
                    local('csvstack {0}/first_query.csv {1}/districts.csv | psql {2} -c "COPY result FROM stdin DELIMITER \',\' CSV HEADER;"'.format(app_config.ELEX_OUTPUT_FOLDER, app_config.ELEX_OUTPUT_FOLDER, app_config.database['PGDATABASE']))

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
    calendar_sheet = calendar['poll_times']
    senate_sheet = calendar['senate_seats']
    house_sheet = calendar['house_seats']

    results = models.Result.select()
    for result in results:
        meta_obj = {
            'result_id': result.id
        }

        if result.level == 'county' or result.level == 'township':
            continue

        if result.level == 'state' or result.level == 'district':
            calendar_row = list(filter(lambda x: x['key'] == result.statepostal, calendar_sheet))[0]

            meta_obj['poll_closing'] = calendar_row['time_est']
            meta_obj['first_results'] = calendar_row['first_results_est']

        if result.level == 'state' and result.officename == 'U.S. House':
            seat = '{0}-{1}'.format(result.statepostal, result.seatnum)
            house_row = list(filter(lambda x: x['seat'] == seat, house_sheet))[0]
            meta_obj['current_party'] = house_row['party']

        if result.level == 'state' and result.officename == 'U.S. Senate':
            senate_row = list(filter(lambda x: x['state'] == result.statepostal, senate_sheet))[0]
            meta_obj['current_party'] = senate_row['party']

        models.RaceMeta.create(**meta_obj)

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

    house_fieldnames = ['first', 'last', 'party', 'state', 'seat']
    senate_fieldnames = ['first', 'last', 'party', 'state']

    with open('data/house-seats.csv', 'w') as h, open('data/senate-seats.csv', 'w') as s:
        house_writer = csv.DictWriter(h, fieldnames=house_fieldnames)
        house_writer.writeheader()

        senate_writer = csv.DictWriter(s, fieldnames=senate_fieldnames)
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
                    obj['seat'] = '{0}-{1}'.format(current_term['state'], current_term['district'])

                if current_term['type'] == 'sen':
                    senate_writer.writerow(obj)
                elif current_term['type'] == 'rep':
                    house_writer.writerow(obj)


@task
def save_old_data():
    with open('data/unemployment.csv') as f:
        unemployment_reader = csv.DictReader(f)

        data = {}

        for row in unemployment_reader:
            full_fips = row['State FIPS Code'] + row['County FIPS Code']
            rate = row['Unemployment Rate (%)']
            obama_result = 0
            romney_result = 0
            winner = ''
            advantage = 0
            with open('data/twentyTwelve.csv') as g:
                twentyTwelve_reader = csv.DictReader(g)
                for county in twentyTwelve_reader:
                    if county['fipscode'] == full_fips and county['last'] == 'Obama':
                        obama_result = county['votepct']
                    elif county['fipscode'] == full_fips and county['last'] == 'Romney':
                        romney_result = county['votepct']
            difference = float(obama_result) - float(romney_result)
            if difference > 0:
                winner = 'Obama'
                advantage = abs(difference)
            else:
                winner = 'Romney'
                advantage = abs(difference)

            this_row = {
                'unemployment': rate,
                'winner': winner,
                'advantage': advantage
            }
            data[full_fips] = this_row
            print(full_fips)

        with open('data/fixed-data.json', 'w') as datafile:
            json.dump(data, datafile)
