"""Tests standard tap features using the built-in SDK tests library."""
import os

from singer_sdk.testing import get_standard_tap_tests

from tap_mongodb.tap import TapMongoDB

BASE_CONFIG = {
    "mongo": {"host": "mongodb://frank:1234@github.com/test?retryWrites=true&w=majority"},
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    os.environ["TAP_MONGO_TEST_NO_DB"] = "1"
    tests = get_standard_tap_tests(TapMongoDB, config=BASE_CONFIG)
    for test in tests:
        test()
