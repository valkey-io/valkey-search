import os
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import random
import string
import logging


class ValkeySearchTestCaseBase(ValkeyTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        loadmodule = f"{os.getenv('MODULE_PATH')} --loadmodule {os.getenv('JSON_MODULE_PATH')}"
        args = {
            "enable-debug-command": "yes",
            "loadmodule": loadmodule,
        }
        server_path = os.getenv("VALKEY_SERVER_PATH")

        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=server_path, args=args
        )
        logging.info("startup args are: %s", args)

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg
            return str(e)

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = self.server.num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def generate_random_string(self, length=7):
        """Creates a random string with specified length."""
        characters = string.ascii_letters + string.digits
        random_string = "".join(
            random.choice(characters) for _ in range(length)
        )
        return random_string

    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command("INFO " + section)
        lines = mem_info.decode("utf-8").split("\r\n")
        stats_dict = {}
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict
