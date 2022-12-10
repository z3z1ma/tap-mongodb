"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests
from tap_mongodb_z.tap import TapMongoDB

SAMPLE_CONFIG = {
    "mongo": {
        "host": "mongodb://frank:1234@github.com/test?retryWrites=true&w=majority"
    },
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapMongoDB, config=SAMPLE_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
test_standard_tap_tests()
