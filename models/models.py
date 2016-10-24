import app_config

from peewee import Model, PostgresqlDatabase
from peewee import BooleanField, CharField, DateField, DateTimeField, DecimalField, ForeignKeyField, IntegerField
from slugify import slugify
from playhouse.postgres_ext import JSONField

db = PostgresqlDatabase(
    app_config.database['PGDATABASE'],
    user=app_config.database['PGUSER'],
    password=app_config.database['PGPASSWORD'],
    host=app_config.database['PGHOST'],
    port=app_config.database['PGPORT']
)


class BaseModel(Model):
    """
    Base class for Peewee models. Ensures they all live in the same database.
    """
    class Meta:
        database = db


class Result(BaseModel):
    id = CharField(primary_key=True)
    raceid = CharField(null=True)
    racetype = CharField(null=True)
    racetypeid = CharField(null=True)
    ballotorder = IntegerField(null=True)
    candidateid = CharField(null=True)
    description = CharField(null=True)
    delegatecount = IntegerField(null=True)
    electiondate = DateField(null=True)
    electtotal = IntegerField(null=True)
    electwon = IntegerField(null=True)
    fipscode = CharField(max_length=5, null=True)
    first = CharField(null=True)
    incumbent = BooleanField(null=True)
    initialization_data = BooleanField(null=True)
    is_ballot_measure = BooleanField(null=True)
    last = CharField(null=True)
    lastupdated = DateTimeField(null=True)
    level = CharField(null=True)
    national = BooleanField(null=True)
    officeid = CharField(null=True)
    officename = CharField(null=True)
    party = CharField(null=True)
    polid = CharField(null=True)
    polnum = CharField(null=True)
    precinctsreporting = IntegerField(null=True)
    precinctsreportingpct = DecimalField(null=True)
    precinctstotal = IntegerField(null=True)
    reportingunitid = CharField(null=True)
    reportingunitname = CharField(null=True)
    runoff = BooleanField(null=True)
    seatname = CharField(null=True)
    seatnum = CharField(null=True)
    statename = CharField(null=True)
    statepostal = CharField(max_length=2)
    test = BooleanField(null=True)
    uncontested = BooleanField(null=True)
    votecount = IntegerField(null=True)
    votepct = DecimalField(null=True)
    winner = BooleanField(null=True)


class Call(BaseModel):
    call_id = ForeignKeyField(Result, related_name='call')
    accept_ap = BooleanField(default=True)
    override_winner = BooleanField(default=False)


class RaceMeta(BaseModel):
    result_id = ForeignKeyField(Result, related_name='meta')
    poll_closing = CharField(null=True)
    first_results = CharField(null=True)


class CensusData(BaseModel):
    fipscode = CharField()
    census_id = ForeignKeyField(Result, related_name='census')
    data = JSONField()
