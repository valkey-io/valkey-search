from typing import Any, Dict, List, Optional, Union
import re

class InfoSearchParser:
    """
    Parser for Redis Search statistics/metrics responses.
    
    This parser handles the metrics format that contains:
    - Section headers marked with '# section_name'
    - Key:value pairs for metrics
    - Support for percentile values (p50, p99, p99.9)
    - Human-readable memory values
    
    Example usage:
        stats_response = client.execute_command("FT._LIST_STATS")  # or similar command
        parser = InfoSearchParser(stats_response)
        
        # Access metrics by section
        print(f"Total documents: {parser.get_metric('search_index_stats', 'search_total_indexed_documents')}")
        
        # Access commonly used properties
        print(f"Active indexes: {parser.active_indexes}")
        print(f"Memory usage: {parser.memory_usage_human}")
    """
    
    def __init__(self, stats_response: Union[bytes, str, List[bytes]]):
        """
        Initialize the parser with the raw statistics response.
        
        Args:
            stats_response: The raw response from Redis stats command
                           Can be bytes, string, or list of byte strings
        """
        self.raw_response = stats_response
        self.sections = self._parse_response(stats_response)
        self.flat_metrics = self._flatten_metrics()
    
    def _parse_response(self, response: Union[bytes, str, List[bytes]]) -> Dict[str, Dict[str, Any]]:
        """Parse the response into sections and metrics."""
        # Handle different input types
        if isinstance(response, list):
            # If it's a list of bytes, decode each and join
            if response and isinstance(response[0], bytes):
                # Decode each bytes item
                text = ''
                for item in response:
                    decoded = item.decode('utf-8') if isinstance(item, bytes) else str(item)
                    text += decoded
            else:
                text = '\n'.join(str(item) for item in response)
        elif isinstance(response, bytes):
            # Direct decode for bytes
            text = response.decode('utf-8')
        else:
            text = str(response)
        
        # Clean up the text - handle various line endings
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        sections = {}
        current_section = None
        
        # Split into lines
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Check for section header
            if line.startswith('#'):
                section_name = line[1:].strip()
                # Replace hyphens with underscores for consistency
                section_name = section_name.replace('-', '_')
                current_section = section_name
                if current_section not in sections:
                    sections[current_section] = {}
                continue
            
            # Parse key:value pairs (only if we have a current section)
            if ':' in line and current_section is not None:
                # Split only on first colon to handle values with colons
                parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    
                    # Parse the value
                    parsed_value = self._parse_value(value)
                    
                    sections[current_section][key] = parsed_value
        
        return sections
    
    def _flatten_metrics(self) -> Dict[str, Any]:
        """Create a flat dictionary of all metrics for easy access."""
        flat = {}
        for section, metrics in self.sections.items():
            for key, value in metrics.items():
                flat[key] = value
                # Also store with section prefix for disambiguation
                flat[f"{section}.{key}"] = value
        return flat
    
    def _parse_value(self, value: str) -> Any:
        """Parse a value string into appropriate type."""
        if not value:
            return None
        
        # Check for percentile values (e.g., "p50=43.263,p99=66.047")
        if 'p50=' in value or 'p99=' in value:
            return self._parse_percentiles(value)
        
        # Check for boolean-like values
        if value.upper() in ['YES', 'TRUE']:
            return True
        if value.upper() in ['NO', 'FALSE', 'NO_ACTIVITY']:
            return False
        
        # Try to parse as number
        try:
            # Check if it's an integer
            if '.' not in value:
                return int(value)
            else:
                return float(value)
        except ValueError:
            # Return as string if not a number
            return value
    
    def _parse_percentiles(self, value: str) -> Dict[str, float]:
        """Parse percentile values like 'p50=43.263,p99=66.047'."""
        percentiles = {}
        parts = value.split(',')
        
        for part in parts:
            if '=' in part:
                percentile, val = part.split('=')
                try:
                    percentiles[percentile.strip()] = float(val.strip())
                except ValueError:
                    percentiles[percentile.strip()] = val.strip()
        
        return percentiles
    
    def _decode_value(self, value: Any) -> str:
        """Decode byte strings to UTF-8."""
        if isinstance(value, bytes):
            # Remove carriage returns and decode
            return value.replace(b'\r', b'').decode('utf-8')
        return str(value)
    
    # Section accessors
    def get_section(self, section_name: str) -> Dict[str, Any]:
        """
        Get all metrics for a specific section.
        
        Args:
            section_name: Name of the section (e.g., 'search_index_stats')
            
        Returns:
            Dictionary of metrics for that section, or empty dict if not found
        """
        return self.sections.get(section_name, {})
    
    def get_metric(self, section: str, metric: str, default: Any = None) -> Any:
        """
        Get a specific metric from a section.
        
        Args:
            section: Section name
            metric: Metric name
            default: Default value if metric not found
            
        Returns:
            The metric value or default
        """
        return self.sections.get(section, {}).get(metric, default)
    
    def get_flat_metric(self, metric: str, default: Any = None) -> Any:
        """
        Get a metric by name without specifying section.
        
        Args:
            metric: Metric name (can include section prefix)
            default: Default value if metric not found
            
        Returns:
            The metric value or default
        """
        return self.flat_metrics.get(metric, default)
    
    # Coordinator metrics properties
    @property
    def coordinator_port(self) -> int:
        """Get the search coordinator listening port."""
        return self.get_metric('search_coordinator', 'search_coordinator_server_listening_port', 0)
    
    @property
    def coordinator_bytes_in(self) -> int:
        """Get bytes received by coordinator."""
        return self.get_metric('search_coordinator', 'search_coordinator_bytes_in', 0)
    
    @property
    def coordinator_bytes_out(self) -> int:
        """Get bytes sent by coordinator."""
        return self.get_metric('search_coordinator', 'search_coordinator_bytes_out', 0)
    
    @property
    def coordinator_latencies(self) -> Dict[str, float]:
        """Get coordinator latency percentiles."""
        return self.get_metric(
            'search_coordinator',
            'search_coordinator_server_get_global_metadata_success_latency_usec',
            {}
        )
    
    # Index statistics properties
    @property
    def active_indexes(self) -> int:
        """Get number of active indexes."""
        return self.get_metric('search_index_stats', 'search_number_of_active_indexes', 0)
    
    @property
    def total_indexes(self) -> int:
        """Get total number of indexes."""
        return self.get_metric('search_index_stats', 'search_number_of_indexes', 0)
    
    @property
    def total_documents(self) -> int:
        """Get total number of indexed documents."""
        return self.get_metric('search_index_stats', 'search_total_indexed_documents', 0)
    
    @property
    def total_attributes(self) -> int:
        """Get total number of attributes."""
        return self.get_metric('search_index_stats', 'search_number_of_attributes', 0)
    
    @property
    def active_write_threads(self) -> int:
        """Get number of active write threads."""
        return self.get_metric('search_index_stats', 'search_total_active_write_threads', 0)
    
    @property
    def indexing_time(self) -> int:
        """Get total indexing time."""
        return self.get_metric('search_index_stats', 'search_total_indexing_time', 0)
    
    @property
    def indexes_currently_indexing(self) -> int:
        """Get number of indexes currently indexing."""
        return self.get_metric('search_index_stats', 'search_number_of_active_indexes_indexing', 0)
    
    @property
    def indexes_running_queries(self) -> int:
        """Get number of indexes currently running queries."""
        return self.get_metric('search_index_stats', 'search_number_of_active_indexes_running_queries', 0)
    
    # Memory metrics properties
    @property
    def memory_usage_bytes(self) -> int:
        """Get memory usage in bytes."""
        return self.get_metric('search_memory', 'search_used_memory_bytes', 0)
    
    @property
    def memory_usage_human(self) -> str:
        """Get human-readable memory usage."""
        return self.get_metric('search_memory', 'search_used_memory_human', '0')
    
    @property
    def reclaimable_memory(self) -> int:
        """Get reclaimable memory in bytes."""
        return self.get_metric('search_memory', 'search_index_reclaimable_memory', 0)
    
    # Query metrics properties
    @property
    def successful_requests(self) -> int:
        """Get count of successful search requests."""
        return self.get_metric('search_query', 'search_successful_requests_count', 0)
    
    @property
    def failed_requests(self) -> int:
        """Get count of failed search requests."""
        return self.get_metric('search_query', 'search_failure_requests_count', 0)
    
    @property
    def hybrid_requests(self) -> int:
        """Get count of hybrid search requests."""
        return self.get_metric('search_query', 'search_hybrid_requests_count', 0)
    
    # HNSW (vector) metrics properties
    @property
    def hnsw_exceptions(self) -> Dict[str, int]:
        """Get all HNSW exception counts."""
        return {
            'add': self.get_metric('search_hnswlib', 'search_hnsw_add_exceptions_count', 0),
            'create': self.get_metric('search_hnswlib', 'search_hnsw_create_exceptions_count', 0),
            'modify': self.get_metric('search_hnswlib', 'search_hnsw_modify_exceptions_count', 0),
            'remove': self.get_metric('search_hnswlib', 'search_hnsw_remove_exceptions_count', 0),
            'search': self.get_metric('search_hnswlib', 'search_hnsw_search_exceptions_count', 0),
        }
    
    def has_hnsw_errors(self) -> bool:
        """Check if there are any HNSW exceptions."""
        exceptions = self.hnsw_exceptions
        return any(count > 0 for count in exceptions.values())
    
    # Indexing status properties
    @property
    def background_indexing_status(self) -> str:
        """Get background indexing status."""
        status = self.get_metric('search_indexing', 'search_background_indexing_status', 'UNKNOWN')
        # Convert boolean False to string representation
        if status is False:
            return 'NO_ACTIVITY'
        return str(status)
    
    def is_indexing_active(self) -> bool:
        """Check if background indexing is active."""
        status = self.background_indexing_status
        return status not in ['NO_ACTIVITY', 'FALSE', 'NO']
    
    # Thread pool metrics
    @property
    def query_queue_size(self) -> int:
        """Get current query queue size."""
        return self.get_metric('search_thread-pool', 'search_query_queue_size', 0)
    
    @property
    def writer_queue_size(self) -> int:
        """Get current writer queue size."""
        return self.get_metric('search_thread-pool', 'search_writer_queue_size', 0)
    
    @property
    def read_cpu_usage(self) -> float:
        """Get read CPU usage."""
        return self.get_metric('search_thread-pool', 'search_used_read_cpu', 0.0)
    
    @property
    def write_cpu_usage(self) -> float:
        """Get write CPU usage."""
        return self.get_metric('search_thread-pool', 'search_used_write_cpu', 0.0)
    
    # Fanout metrics
    @property
    def fanout_failures(self) -> int:
        """Get fanout failure count."""
        return self.get_metric('search_fanout', 'search_info_fanout_fail_count', 0)
    
    @property
    def fanout_retries(self) -> int:
        """Get fanout retry count."""
        return self.get_metric('search_fanout', 'search_info_fanout_retry_count', 0)
    
    # Utility methods
    def get_health_summary(self) -> Dict[str, Any]:
        """
        Get a summary of system health indicators.
        
        Returns:
            Dictionary with health status information
        """
        return {
            'active_indexes': self.active_indexes,
            'total_documents': self.total_documents,
            'memory_usage': self.memory_usage_human,
            'failed_requests': self.failed_requests,
            'successful_requests': self.successful_requests,
            'query_queue': self.query_queue_size,
            'writer_queue': self.writer_queue_size,
            'hnsw_errors': self.has_hnsw_errors(),
            'indexing_active': self.is_indexing_active(),
            'fanout_failures': self.fanout_failures,
            'cpu_read': self.read_cpu_usage,
            'cpu_write': self.write_cpu_usage
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance-related metrics.
        
        Returns:
            Dictionary with performance metrics
        """
        return {
            'latencies': self.coordinator_latencies,
            'query_queue_size': self.query_queue_size,
            'writer_queue_size': self.writer_queue_size,
            'read_cpu': self.read_cpu_usage,
            'write_cpu': self.write_cpu_usage,
            'active_write_threads': self.active_write_threads,
            'indexing_time': self.indexing_time,
            'bytes_in': self.coordinator_bytes_in,
            'bytes_out': self.coordinator_bytes_out
        }
    
    def get_section_names(self) -> List[str]:
        """Get all available section names."""
        return list(self.sections.keys())
    
    def has_section(self, section_name: str) -> bool:
        """Check if a section exists."""
        return section_name in self.sections
    
    def search_metrics(self, pattern: str) -> Dict[str, Any]:
        """
        Search for metrics matching a pattern.
        
        Args:
            pattern: Regex pattern to match metric names
            
        Returns:
            Dictionary of matching metrics
        """
        regex = re.compile(pattern, re.IGNORECASE)
        results = {}
        
        for key, value in self.flat_metrics.items():
            if regex.search(key):
                results[key] = value
        
        return results
    
    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Return all sections and metrics as a dictionary."""
        return self.sections.copy()
    
    def to_flat_dict(self) -> Dict[str, Any]:
        """Return all metrics as a flat dictionary."""
        return self.flat_metrics.copy()
    
    def __str__(self) -> str:
        """String representation of the parser."""
        return (f"InfoSearchParser(indexes={self.active_indexes}, "
                f"documents={self.total_documents}, "
                f"memory={self.memory_usage_human})")
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return (f"InfoSearchParser(sections={len(self.sections)}, "
                f"metrics={len(self.flat_metrics)}, "
                f"indexes={self.active_indexes})")
    
    def pretty_print(self) -> str:
        """
        Return a pretty-printed string representation of the statistics.
        """
        lines = ["Redis Search Statistics"]
        lines.append("=" * 50)
        
        # Index Statistics
        lines.append("\nIndex Statistics:")
        lines.append(f"  Active Indexes: {self.active_indexes}")
        lines.append(f"  Total Indexes: {self.total_indexes}")
        lines.append(f"  Total Documents: {self.total_documents}")
        lines.append(f"  Total Attributes: {self.total_attributes}")
        lines.append(f"  Currently Indexing: {self.indexes_currently_indexing}")
        lines.append(f"  Running Queries: {self.indexes_running_queries}")
        
        # Memory Usage
        lines.append("\nMemory Usage:")
        lines.append(f"  Used: {self.memory_usage_human} ({self.memory_usage_bytes} bytes)")
        lines.append(f"  Reclaimable: {self.reclaimable_memory} bytes")
        
        # Query Statistics
        lines.append("\nQuery Statistics:")
        lines.append(f"  Successful Requests: {self.successful_requests}")
        lines.append(f"  Failed Requests: {self.failed_requests}")
        lines.append(f"  Hybrid Requests: {self.hybrid_requests}")
        
        # Performance
        lines.append("\nPerformance:")
        lines.append(f"  Query Queue: {self.query_queue_size}")
        lines.append(f"  Writer Queue: {self.writer_queue_size}")
        lines.append(f"  Read CPU: {self.read_cpu_usage}")
        lines.append(f"  Write CPU: {self.write_cpu_usage}")
        
        # Coordinator
        if self.coordinator_latencies:
            lines.append("\nCoordinator Latencies (μs):")
            for percentile, value in self.coordinator_latencies.items():
                lines.append(f"  {percentile}: {value}")
        
        # Health Indicators
        lines.append("\nHealth Indicators:")
        lines.append(f"  Background Indexing: {self.background_indexing_status}")
        lines.append(f"  HNSW Errors: {'Yes' if self.has_hnsw_errors() else 'No'}")
        if self.has_hnsw_errors():
            for error_type, count in self.hnsw_exceptions.items():
                if count > 0:
                    lines.append(f"    {error_type}: {count}")
        lines.append(f"  Fanout Failures: {self.fanout_failures}")
        lines.append(f"  Fanout Retries: {self.fanout_retries}")
        
        # Thread Pool
        lines.append("\nThread Pool:")
        lines.append(f"  Active Write Threads: {self.active_write_threads}")
        lines.append(f"  Worker Pool Suspensions: {self.get_flat_metric('search_worker_pool_suspend_cnt', 0)}")
        
        return "\n".join(lines)
    
    def debug_parse(self) -> None:
        """
        Debug method to show what was parsed.
        Useful for troubleshooting parsing issues.
        """
        print("=" * 60)
        print("DEBUG: Parser Contents")
        print("=" * 60)
        
        print(f"\nNumber of sections found: {len(self.sections)}")
        print(f"Section names: {list(self.sections.keys())}")
        
        print(f"\nTotal metrics parsed: {len(self.flat_metrics)}")
        
        if not self.sections:
            print("\nWARNING: No sections were parsed!")
            print("Raw response type:", type(self.raw_response))
            if isinstance(self.raw_response, bytes):
                print("First 200 chars of raw response:", self.raw_response[:200])
        else:
            print("\nFirst 5 metrics from each section:")
            for section_name, metrics in self.sections.items():
                print(f"\n[{section_name}] ({len(metrics)} metrics)")
                for i, (key, value) in enumerate(metrics.items()):
                    if i >= 5:
                        print(f"  ... and {len(metrics) - 5} more")
                        break
                    print(f"  {key}: {value} (type: {type(value).__name__})")
        
        print("\n" + "=" * 60)
        """
        Check for potential health issues.
        
        Returns:
            List of warning/error messages
        """
        issues = []
        
        # Check for failed requests
        if self.failed_requests > 0:
            issues.append(f"Failed requests detected: {self.failed_requests}")
        
        # Check for HNSW errors
        if self.has_hnsw_errors():
            issues.append(f"HNSW exceptions detected: {self.hnsw_exceptions}")
        
        # Check queue sizes
        if self.query_queue_size > 100:
            issues.append(f"High query queue size: {self.query_queue_size}")
        
        if self.writer_queue_size > 100:
            issues.append(f"High writer queue size: {self.writer_queue_size}")
        
        # Check fanout issues
        if self.fanout_failures > 0:
            issues.append(f"Fanout failures: {self.fanout_failures}")
        
        # Check CPU usage
        if self.read_cpu_usage > 80:
            issues.append(f"High read CPU usage: {self.read_cpu_usage}%")
        
        if self.write_cpu_usage > 80:
            issues.append(f"High write CPU usage: {self.write_cpu_usage}%")
        
        # Check if no indexes
        if self.active_indexes == 0 and self.total_indexes > 0:
            issues.append("No active indexes despite having defined indexes")
        
        return issues
