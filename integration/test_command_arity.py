"""Command arity is enforced by the engine (via SetCommandInfo at
registration), so a wrong-arity call is rejected before the handler runs."""

import pytest
import valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestCommandArity(ValkeySearchTestCaseBase):
    # (command, args) pairs the engine must reject: FT.DROPINDEX is arity 2,
    # FT.INFO is -2 (at least 2), FT._LIST is 1.
    @pytest.mark.parametrize(
        "command",
        [
            ["FT.DROPINDEX"],  # too few
            ["FT.DROPINDEX", "idx", "extra"],  # too many
            ["FT.INFO"],  # too few
            ["FT._LIST", "extra"],  # too many
        ],
    )
    def test_wrong_arity_is_rejected(self, command):
        with pytest.raises(
            valkey.exceptions.ResponseError, match="wrong number of arguments"
        ):
            self.client.execute_command(*command)
