"""Shared pytest policy; tier-specific bootstrap stays in child conftests."""

from tests.tier_policy import mark_collected_tests


def pytest_collection_modifyitems(items):
    mark_collected_tests(items)
