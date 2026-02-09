"""
Test file for validating all search module configs.
Tests CONFIG GET and CONFIG SET for each config parameter.
"""
import random
from valkey_search_test_case import (
    ValkeySearchTestCaseBase, ValkeySearchClusterTestCase,
    ValkeySearchTestCaseDebugMode, ValkeySearchClusterTestCaseDebugMode
)
from valkey.exceptions import ResponseError
from valkeytestframework.conftest import resource_port_tracker


# All configs: (name, type, visibility)
ALL_CONFIGS = [
    # APP configs
    ("backfill-batch-size", "Number", "APP"),
    ("cluster-map-expiration-ms", "Number", "APP"),
    ("coordinator-query-timeout-secs", "Number", "APP"),
    ("default-timeout-ms", "Number", "APP"),
    ("drain-mutation-queue-on-save", "Boolean", "APP"),
    ("ft-info-rpc-timeout-ms", "Number", "APP"),
    ("ft-info-timeout-ms", "Number", "APP"),
    ("fuzzy-max-distance", "Number", "APP"),
    ("high-priority-weight", "Number", "APP"),
    ("hnsw-block-size", "Number", "APP"),
    ("info-developer-visible", "Boolean", "APP"),
    ("local-fanout-queue-wait-threshold", "Number", "APP"),
    ("log-level", "Enum", "APP"),
    ("max-indexes", "Number", "APP"),
    ("max-numeric-field-length", "Number", "APP"),
    ("max-prefixes", "Number", "APP"),
    ("max-search-result-fields-count", "Number", "APP"),
    ("max-search-result-record-size", "Number", "APP"),
    ("max-tag-field-length", "Number", "APP"),
    ("max-term-expansions", "Number", "APP"),
    ("max-vector-attributes", "Number", "APP"),
    ("max-vector-dimensions", "Number", "APP"),
    ("max-vector-ef-construction", "Number", "APP"),
    ("max-vector-ef-runtime", "Number", "APP"),
    ("max-vector-knn", "Number", "APP"),
    ("max-vector-m", "Number", "APP"),
    ("max-worker-suspension-secs", "Number", "APP"),
    ("enable-consistent-results", "Boolean", "APP"),
    ("enable-partial-results", "Boolean", "APP"),
    ("proximity-inorder-compat-mode", "Boolean", "APP"),
    ("query-string-bytes", "Number", "APP"),
    ("query-string-depth", "Number", "APP"),
    ("query-string-terms-count", "Number", "APP"),
    ("reader-threads", "Number", "APP"),
    ("search-result-background-cleanup", "Boolean", "APP"),
    ("search-result-buffer-multiplier", "String", "APP"),
    ("skip-corrupted-internal-update-entries", "Boolean", "APP"),
    ("skip-rdb-load", "Boolean", "APP"),
    ("thread-pool-wait-time-samples", "Number", "APP"),
    ("utility-threads", "Number", "APP"),
    ("writer-threads", "Number", "APP"),
    # DEV configs
    ("drain-mutation-queue-on-load", "Boolean", "DEV"),
    ("rdb-read-v2", "Boolean", "DEV"),
    ("rdb-validate-on-write", "Boolean", "DEV"),
    ("rdb-write-v2", "Boolean", "DEV"),
    # HIDDEN configs
    ("use-coordinator", "Boolean", "HIDDEN"),
    ("debug-mode", "Boolean", "HIDDEN"),
]

APP_CONFIGS = [(n, t) for n, t, v in ALL_CONFIGS if v == "APP"]
DEV_CONFIGS = [(n, t) for n, t, v in ALL_CONFIGS if v == "DEV"]
HIDDEN_CONFIGS = [(n, t) for n, t, v in ALL_CONFIGS if v == "HIDDEN"]

CONFIG_RANGES = {
    "high-priority-weight": (0, 100),
    "fuzzy-max-distance": (1, 50),
    "max-prefixes": (1, 16),
    "max-numeric-field-length": (1, 256),
    "max-vector-attributes": (1, 100),
    "max-indexes": (1, 10),
    "reader-threads": (1, 1024),
    "writer-threads": (1, 1024),
    "utility-threads": (1, 1024),
    "max-worker-suspension-secs": (0, 3600),
    "max-vector-ef-construction": (1, 4096),
    "coordinator-query-timeout-secs": (1, 3600),
    "max-vector-ef-runtime": (1, 4096),
    "max-search-result-fields-count": (1, 1000),
    "local-fanout-queue-wait-threshold": (1, 10000),
    "thread-pool-wait-time-samples": (10, 10000),
    "max-search-result-record-size": (100, 10485760),
    "search-result-buffer-multiplier": (1.0, 1000.0),
}


def get_new_value(current_value, config_type, config_name=None):
    """Generate a new value based on config type."""
    if config_type == "Boolean":
        return "no" if current_value in (b"yes", "yes", b"true", "true", b"1", "1") else "yes"
    if config_type == "Number":
        if config_name in CONFIG_RANGES:
            return str(random.randint(*CONFIG_RANGES[config_name]))
        return str(random.randint(1, 10240))
    if config_type == "Enum":
        return random.choice(["WARNING", "NOTICE", "DEBUG"])
    if config_type == "String":
        match config_name:
            case "search-result-buffer-multiplier":
                low, high = CONFIG_RANGES[config_name]
                return str(round(random.uniform(low, high), 2))
            case _:
                assert False, f"Unknown config name {config_name}"
    return str(random.randint(1, 100))


class ConfigTestMixin:
    """Mixin providing common config test methods."""

    def get_client(self):
        """Override in cluster tests to return appropriate client."""
        return self.client

    def assert_config_get_set(self, configs):
        """Test that configs can be read and written."""
        client = self.get_client()
        get_failures, set_failures = [], []

        for name, config_type in configs:
            full_name = f"search.{name}"
            # Test GET
            try:
                result = client.execute_command("CONFIG", "GET", full_name)
                if not result or len(result) < 2:
                    get_failures.append((name, "empty result"))
                    continue
                current_value = result[1]
            except Exception as e:
                get_failures.append((name, str(e)))
                continue

            # Test SET
            new_value = get_new_value(current_value, config_type, name)
            try:
                client.execute_command("CONFIG", "SET", full_name, new_value)
            except Exception as e:
                set_failures.append((name, str(e)))

        assert not get_failures, f"CONFIG GET failures: {get_failures}"
        assert not set_failures, f"CONFIG SET failures: {set_failures}"

    def assert_config_readable_not_writable(self, configs):
        """Test that configs can be read but not written."""
        client = self.get_client()
        for name, config_type in configs:
            full_name = f"search.{name}"
            # Should be readable
            result = client.execute_command("CONFIG", "GET", full_name)
            assert result and len(result) >= 2, f"{name} should be readable"
            current_config_value = result[1]
            # Should not be writable - use toggled value
            if config_type == "Boolean":
                test_value = "no" if current_config_value in (b"yes", "yes", b"true", "true", b"1", "1") else "yes"
            else:
                test_value = "1"
            set_failed = False
            try:
                client.execute_command("CONFIG", "SET", full_name, test_value)
            except ResponseError:
                set_failed = True
            assert set_failed, f"{name} should not be writable"

    def assert_config_not_accessible(self, configs):
        """Test that configs cannot be read or written."""
        client = self.get_client()
        for name, config_type in configs:
            full_name = f"search.{name}"
            # Should not be readable
            result = client.execute_command("CONFIG", "GET", full_name)
            assert result == [] or len(result) == 0, f"{name} should not be readable"
            # Should not be writable
            set_failed = False
            try:
                client.execute_command("CONFIG", "SET", full_name, "yes")
            except ResponseError:
                set_failed = True
            assert set_failed, f"{name} should not be writable"


class TestAPPConfigsStandalone(ConfigTestMixin, ValkeySearchTestCaseBase):
    def test_app_configs_get_set(self):
        self.assert_config_get_set(APP_CONFIGS)
        self.assert_config_readable_not_writable(DEV_CONFIGS)
        self.assert_config_not_accessible(HIDDEN_CONFIGS)


class TestAPPConfigsCluster(ConfigTestMixin, ValkeySearchClusterTestCase):
    def get_client(self):
        return self.client_for_primary(0)

    def test_app_configs_get_set(self):
        self.assert_config_get_set(APP_CONFIGS)
        self.assert_config_readable_not_writable(DEV_CONFIGS)
        self.assert_config_not_accessible(HIDDEN_CONFIGS)


class TestDevConfigsStandalone(ConfigTestMixin, ValkeySearchTestCaseDebugMode):
    def test_dev_configs_get_set(self):
        self.assert_config_get_set(DEV_CONFIGS)


class TestDevConfigsCluster(ConfigTestMixin, ValkeySearchClusterTestCaseDebugMode):
    def get_client(self):
        return self.client_for_primary(0)

    def test_dev_configs_get_set(self):
        self.assert_config_get_set(DEV_CONFIGS)


class TestHiddenConfigsStandalone(ConfigTestMixin, ValkeySearchTestCaseDebugMode):
    def test_hidden_configs_not_accessible(self):
        self.assert_config_not_accessible(HIDDEN_CONFIGS)


class TestHiddenConfigsCluster(ConfigTestMixin, ValkeySearchClusterTestCaseDebugMode):
    def get_client(self):
        return self.client_for_primary(0)

    def test_hidden_configs_not_accessible(self):
        self.assert_config_not_accessible(HIDDEN_CONFIGS)