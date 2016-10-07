from collections import OrderedDict
from decimal import Decimal, ROUND_DOWN
from models import models
from peewee import fn

def filter_results():
    results = models.Result.select().where(
        (models.Result.level == 'state') | (models.Result.level == None),
        models.Result.officename == 'President'
    ).order_by(models.Result.statepostal, models.Result.party, -models.Result.votecount, models.Result.last)

    return results

def group_results_by_race(results):
    grouped = OrderedDict()
    for result in results:
        if result.raceid not in grouped:
            grouped[result.raceid] = []

        grouped[result.raceid].append(result)

    return grouped

def comma_filter(value):
    """
    Format a number with commas.
    """
    return '{:,}'.format(value)


def percent_filter(value):
    """
    Format percentage
    """
    value = Decimal(value) * Decimal(100)
    if value == 0:
        return '0%'
    elif value == 100:
        return '100%'
    elif value > 0 and value < 1:
        return '<1%'
    else:
        cleaned_pct = value.quantize(Decimal('.1'), rounding=ROUND_DOWN)
        return '{:.1f}%'.format(cleaned_pct)

def never_cache_preview(response):
    """
    Ensure preview is never cached
    """
    response.cache_control.max_age = 0
    response.cache_control.no_cache = True
    response.cache_control.must_revalidate = True
    response.cache_control.no_store = True
    return response


def open_db():
    """
    Open db connection
    """
    models.db.connect()


def close_db(response):
    """
    Close db connection
    """
    models.db.close()
    return response
