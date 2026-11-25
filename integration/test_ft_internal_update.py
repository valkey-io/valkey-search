from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkey.exceptions import ResponseError
import pytest


class TestFTInternalUpdate(ValkeySearchTestCaseBase):
    
    def test_admin_acl_required(self):
        """Test that FT.INTERNAL_UPDATE requires admin ACL permissions"""
        # Create a user without admin permissions
        self.client.execute_command("ACL", "SETUSER", "testuser", "on", ">password", "+@read", "+@write", "-@admin")
        
        # Connect as the restricted user  
        restricted_client = self.server.get_new_client()
        restricted_client.execute_command("AUTH", "testuser", "password")
        
        # Try to call FT.INTERNAL_UPDATE - should fail with ACL error
        with pytest.raises(ResponseError) as exc_info:
            restricted_client.execute_command("FT.INTERNAL_UPDATE", "test_id", "invalid_data", "invalid_header")
        
        assert "no permissions" in str(exc_info.value)
        
        # Cleanup
        self.client.execute_command("ACL", "DELUSER", "testuser")
    


    @pytest.mark.parametrize(
        "rules",
        [["+@write"], ["+@search"], ["+@fast"], ["+@all", "-@dangerous"]],
    )
    def test_not_granted_by_non_admin_categories(self, rules):
        """FT.INTERNAL_UPDATE is only in @admin and @dangerous."""
        self.client.execute_command(
            "ACL", "SETUSER", "testuser", "on", ">password", *rules
        )
        restricted_client = self.server.get_new_client()
        restricted_client.execute_command("AUTH", "testuser", "password")

        with pytest.raises(ResponseError) as exc_info:
            restricted_client.execute_command("FT.INTERNAL_UPDATE", "x")
        assert "no permissions" in str(exc_info.value)
        assert self.client.ping()

        self.client.execute_command("ACL", "DELUSER", "testuser")

    def test_wrong_arity_returns_error(self):
        """A wrong argument count is an error, not a server abort."""
        for args in (["x"], ["a", "b", "c", "d"]):
            with pytest.raises(ResponseError) as exc_info:
                self.client.execute_command("FT.INTERNAL_UPDATE", *args)
            assert "wrong number of arguments" in str(exc_info.value)
        assert self.client.ping()
