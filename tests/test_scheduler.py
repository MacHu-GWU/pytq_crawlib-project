#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from datetime import datetime
# from helper import HashAndProcessImplement, validate_schduler_implement

import os
import shutil
import responses
import mongoengine
import mongoengine_mate
from crawlib import create_cache, Status, ParseResult
from pytq_crawlib.scheduler import OneToMany, OneToOne

client = mongoengine.connect('mongoenginetest', host='mongomock://localhost')


class State(mongoengine_mate.ExtendedDocument):
    _id = mongoengine.StringField(primary_key=True)
    _status = mongoengine.IntField(default=Status.S0_ToDo.id)
    _edit_at = mongoengine.DateTimeField(default=datetime(1970, 1, 1))
    n_zipcode = mongoengine.IntField()


class Zipcode(mongoengine_mate.ExtendedDocument):
    _id = mongoengine.StringField(primary_key=True)
    lat = mongoengine.FloatField()
    lng = mongoengine.FloatField()
    _status = mongoengine.IntField(default=Status.S0_ToDo.id)
    _edit_at = mongoengine.DateTimeField(default=datetime(1970, 1, 1))


cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
try:
    shutil.rmtree(cache_dir)
except:
    pass
cache = create_cache(cache_dir)


class SchedulerState(OneToMany):
    model_klass = State
    status_key = State._status.name
    edit_at_key = State._edit_at.name
    n_child_key = State.n_zipcode.name

    cache = cache

    child_klass = Zipcode

    def build_url(self, doc):
        return "https://www.example.com/state/%s" % doc._id

    def parse_html(self, html, **kwargs):
        return ParseResult(
            kwargs=dict(html=html),
            data=[
                Zipcode(_id="10001"), Zipcode(_id="10002")
            ]
        )


def test_SchedulerOneToMany():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(
            responses.GET, url="https://www.example.com/state/va",
            status=200, body="Virginia",
        )
        rsps.add(
            responses.GET, url="https://www.example.com/state/md",
            status=200, body="Maryland",
        )
        s = SchedulerState()
        s.log_off()
        State.smart_insert([State(_id="va"), State(_id="md")])
        input_data_queue = s.get_input_data_queue()
        s.do(input_data_queue, ignore_error=False)

    doc1, doc2 = list(s.col.find())
    assert doc1["_id"] == "va"
    assert doc1[SchedulerState.n_child_key] == 2
    assert doc2["_id"] == "md"
    assert doc2[SchedulerState.n_child_key] == 2


class SchedulerZipcode(OneToOne):
    model_klass = Zipcode
    status_key = Zipcode._status.name
    edit_at_key = Zipcode._edit_at.name

    cache = cache

    def build_url(self, doc):
        return "https://www.example.com/zipcode/%s" % doc._id

    def parse_html(self, html, **kwargs):
        return ParseResult(
            kwargs=dict(html=html),
            data=Zipcode(lat=32.0, lng=-77.0)
        )


def test_SchedulerOneToOne():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(
            responses.GET, url="https://www.example.com/zipcode/10001",
            status=200, body="zipcode 10001",
        )
        rsps.add(
            responses.GET, url="https://www.example.com/zipcode/10002",
            status=200, body="zipcode 10002",
        )

        s = SchedulerZipcode()
        s.log_off()
        input_data_queue = s.get_input_data_queue()
        s.do(input_data_queue, ignore_error=False)

    docs = list(s.col.find())
    doc1, doc2 = docs

    assert doc1["_id"] == "10001"
    assert doc1["lat"] == 32.0
    assert doc1["lng"] == -77
    assert doc1[SchedulerZipcode.status_key] == SchedulerZipcode.duplicate_flag

    assert doc2["_id"] == "10002"
    assert doc2["lat"] == 32.0
    assert doc2["lng"] == -77
    assert doc2[SchedulerZipcode.status_key] == SchedulerZipcode.duplicate_flag


class House(mongoengine_mate.ExtendedDocument):
    _id = mongoengine.StringField(primary_key=True)
    address = mongoengine.StringField()
    _status = mongoengine.IntField(default=Status.S0_ToDo.id)
    _edit_at = mongoengine.DateTimeField(default=datetime(1970, 1, 1))


class SchedulerHouse(OneToOne):
    model_klass = House
    cache = cache
    use_requests = False
    chrome_drive_path = "/Users/sanhehu/Documents/chromedriver"

    def build_url(self, doc):
        return "https://www.python.org/"

    def parse_html(self, html, **kwargs):
        return ParseResult(
            data=House(address="123 Main St")
        )


def test_SchedulerUseBrowser():
    s = SchedulerHouse()
    s.log_off()
    House.smart_insert([House(_id="h1"), House(_id="h2")])
    input_data_queue = s.get_input_data_queue()
    s.do(input_data_queue, ignore_error=False)

    doc1, doc2 = list(s.col.find())
    assert doc1["_id"] == "h1"
    assert doc1["address"] == "123 Main St"
    assert doc2["_id"] == "h2"
    assert doc2["address"] == "123 Main St"


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
