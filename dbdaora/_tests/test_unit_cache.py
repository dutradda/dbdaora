import time

import pytest

from dbdaora import TTLDaoraCache


@pytest.fixture
def cache():
    return TTLDaoraCache(maxsize=2, ttl=2, ttl_failure_threshold=1)


def test_should_set_and_get_data(cache):
    cache['fake'] = 'faked'
    assert cache.get('fake') == 'faked'


def test_should_not_get_data(cache):
    assert cache.get('fake') is None


def test_should_not_get_data_when_expired(cache):
    cache['fake'] = 'faked'

    assert cache.get('fake') == 'faked'

    time.sleep(2)

    assert cache.get('fake') is None


def test_should_not_set_data_when_reach_maxsize(cache):
    cache['fake'] = 'faked'
    cache['fake2'] = 'faked2'
    cache['fake3'] = 'faked3'

    assert cache.get('fake') == 'faked'
    assert cache.get('fake2') == 'faked2'
    assert cache.get('fake3') is None
