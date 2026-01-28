import valkey
from absl.testing import absltest
from absl.testing import parameterized
import utils


class FTInternalUpdateIntegrationTest(parameterized.TestCase):
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Use existing valkey instance
        cls.port = 6379
        cls.host = 'localhost'
    
    def get_valkey_connection(self):
        return valkey.Valkey(host=self.host, port=self.port, decode_responses=True)
    
    def test_admin_acl_required(self):
        """Test that FT.INTERNAL_UPDATE requires admin ACL permissions"""
        r = self.get_valkey_connection()
        
        # Create a user without admin permissions
        r.execute_command("ACL", "SETUSER", "testuser", "on", ">password", "+@read", "+@write", "-@admin")
        
        # Connect as the restricted user  
        restricted_client = valkey.Valkey(host=self.host, port=self.port, username='testuser', password='password')
        
        # Try to call FT.INTERNAL_UPDATE - should fail with ACL error
        with self.assertRaises(valkey.ResponseError) as cm:
            restricted_client.execute_command("FT.INTERNAL_UPDATE", "test_id", "invalid_data", "invalid_header")
        
        self.assertIn("NOPERM", str(cm.exception))
        
        # Cleanup
        r.execute_command("ACL", "DELUSER", "testuser")


if __name__ == '__main__':
    absltest.main()
