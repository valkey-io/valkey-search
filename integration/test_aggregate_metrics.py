from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestAggregateMetrics(ValkeySearchTestCaseBase):

    def test_aggregate_stage_metrics(self):
        """Test that FT.AGGREGATE metrics are incremented correctly for each stage type."""
        client: Valkey = self.server.get_new_client()
        assert (
            client.execute_command(
                "CONFIG SET search.info-developer-visible yes"
            )
            == b"OK"
        )
        # Create index with numeric and tag fields
        client.execute_command(
            "FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:",
            "SCHEMA", "price", "NUMERIC", "rating", "NUMERIC", "category", "TAG"
        )
        
        # Load test data
        for i in range(20):
            client.execute_command(
                "HSET", f"product:{i}",
                "price", str(100 + i * 10),
                "rating", str(3.0 + (i % 3)),
                "category", f"cat{i % 3}"
            )
        
        # Get initial metrics
        info = client.info("SEARCH")
        initial_limit = int(info.get("search_agg_limit_stages", 0))
        initial_groupby = int(info.get("search_agg_group_by_stages", 0))
        initial_apply = int(info.get("search_agg_apply_stages", 0))
        initial_sortby = int(info.get("search_agg_sort_by_stages", 0))
        initial_filter = int(info.get("search_agg_filter_stages", 0))
        initial_input_records = int(info.get("search_agg_input_records", 0))
        initial_output_records = int(info.get("search_agg_output_records", 0))
        initial_limit_input = int(info.get("search_agg_limit_input_records", 0))
        initial_limit_output = int(info.get("search_agg_limit_output_records", 0))
        initial_filter_input = int(info.get("search_agg_filter_input_records", 0))
        initial_filter_output = int(info.get("search_agg_filter_output_records", 0))
        initial_groupby_input = int(info.get("search_agg_group_by_input_records", 0))
        initial_groupby_output = int(info.get("search_agg_group_by_output_records", 0))
        
        # Test LIMIT stage
        client.execute_command("FT.AGGREGATE", "products", "@rating:[-inf inf]", "LIMIT", "0", "5")
        info = client.info("SEARCH")
        assert int(info["search_agg_limit_stages"]) == initial_limit + 1
        assert int(info["search_agg_limit_input_records"]) == initial_limit_input + 20
        assert int(info["search_agg_limit_output_records"]) == initial_limit_output + 5
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == initial_output_records + 5
        initial_limit_input = int(info["search_agg_limit_input_records"])
        initial_limit_output = int(info["search_agg_limit_output_records"])
        initial_input_records = int(info["search_agg_input_records"])
        initial_output_records = int(info["search_agg_output_records"])
        
        # Test GROUPBY stage
        client.execute_command("FT.AGGREGATE", "products", "@rating:[-inf inf]", "load", "1", "category", "GROUPBY", "1", "@category") # , "REDUCE", "COUNT", "0", "AS", "count")
        info = client.info("SEARCH")
        assert int(info["search_agg_group_by_stages"]) == initial_groupby + 1
        assert int(info["search_agg_group_by_input_records"]) == initial_groupby_input + 20
        assert int(info["search_agg_group_by_output_records"]) == (initial_groupby_output + 3)  # 3 categories
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == (initial_output_records + 3)
        initial_groupby_input = int(info["search_agg_group_by_input_records"])
        initial_groupby_output = int(info["search_agg_group_by_output_records"])
        initial_input_records = int(info["search_agg_input_records"])
        initial_output_records = int(info["search_agg_output_records"])
        
        # Test APPLY stage
        client.execute_command("FT.AGGREGATE", "products", "@rating:[-inf inf]", "APPLY", "@price * 2", "AS", "double_price")
        info = client.info("SEARCH")
        assert int(info["search_agg_apply_stages"]) == initial_apply + 1
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == initial_output_records + 20
        initial_input_records = int(info["search_agg_input_records"])
        initial_output_records = int(info["search_agg_output_records"])
        
        # Test SORTBY stage
        client.execute_command("FT.AGGREGATE", "products", "@rating:[-inf inf]", "load", "1", "price", "SORTBY", "2", "@price", "DESC", "max", "20")
        info = client.info("SEARCH")
        assert int(info["search_agg_sort_by_stages"]) == initial_sortby + 1
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == initial_output_records + 20
        initial_input_records = int(info["search_agg_input_records"])
        initial_output_records = int(info["search_agg_output_records"])
        
        # Test FILTER stage
        client.execute_command("FT.AGGREGATE", "products", "@rating:[-inf inf]", "load", "1", "price", "FILTER", "@price >= 150")
        info = client.info("SEARCH")
        assert int(info["search_agg_filter_stages"]) == initial_filter + 1
        assert int(info["search_agg_filter_input_records"]) == initial_filter_input + 20
        assert int(info["search_agg_filter_output_records"]) == initial_filter_output + 15  # 15 products with price >= 150
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == initial_output_records + 15
        initial_filter_input = int(info["search_agg_filter_input_records"])
        initial_filter_output = int(info["search_agg_filter_output_records"])
        initial_input_records = int(info["search_agg_input_records"])
        initial_output_records = int(info["search_agg_output_records"])
        
        # Test multiple stages in one query
        initial_limit = int(info["search_agg_limit_stages"])
        initial_filter = int(info["search_agg_filter_stages"])
        initial_sortby = int(info["search_agg_sort_by_stages"])
        
        client.execute_command(
            "FT.AGGREGATE", "products", "@rating:[-inf inf]",
            "load", "2", "price", "rating",
            "FILTER", "@price > 100",
            "SORTBY", "2", "@rating", "ASC", "max", "100",
            "LIMIT", "0", "10"
        )
        
        info = client.info("SEARCH")
        assert int(info["search_agg_filter_stages"]) == initial_filter + 1
        assert int(info["search_agg_sort_by_stages"]) == initial_sortby + 1
        assert int(info["search_agg_limit_stages"]) == initial_limit + 1
        # Filter: 20 in, 19 out (price > 100)
        assert int(info["search_agg_filter_input_records"]) == initial_filter_input + 20
        assert int(info["search_agg_filter_output_records"]) == initial_filter_output + 19
        # Limit: 19 in, 10 out
        assert int(info["search_agg_limit_input_records"]) == initial_limit_input + 19
        assert int(info["search_agg_limit_output_records"]) == initial_limit_output + 10
        # Overall: 20 in, 10 out
        assert int(info["search_agg_input_records"]) == initial_input_records + 20
        assert int(info["search_agg_output_records"]) == initial_output_records + 10
