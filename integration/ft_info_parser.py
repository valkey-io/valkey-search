from typing import Any, Dict, List, Optional, Union


class FTInfoParser:
    """
    Parser for Redis FT.INFO command responses.

    FT.INFO returns a flat list where even indices are keys and odd indices are values.
    This class parses that structure into a more accessible format.

    Example usage:
        info_response = client.execute_command("FT.INFO", "my_index")
        parser = FTInfoParser(info_response)
        print(f"Index: {parser.index_name}")
        print(f"Documents: {parser.num_docs}")
        print(f"State: {parser.state}")
    """

    def __init__(self, ft_info_response: List[Any]):
        """
        Initialize the parser with the raw FT.INFO response.

        Args:
            ft_info_response: The raw response from FT.INFO command
        """
        self.raw_response = ft_info_response
        self.parsed_data = self._parse_response(ft_info_response)

    def _parse_response(self, response: List[Any]) -> Dict[str, Any]:
        """Parse the flat key-value list into a structured dictionary."""
        if not isinstance(response, list) or len(response) % 2 != 0:
            raise ValueError(
                "FT.INFO response must be a list with even number of elements"
            )

        result = {}
        for i in range(0, len(response), 2):
            key = self._decode_value(response[i])
            value = self._decode_value(response[i + 1])

            # Special handling for nested structures
            if key == "index_definition" and isinstance(value, list):
                result[key] = self._parse_key_value_list(value)
            elif key == "attributes" and isinstance(value, list):
                result[key] = self._parse_attributes(value)
            else:
                result[key] = value

        return result

    def _parse_key_value_list(
        self, data: List[Any]
    ) -> Union[Dict[str, Any], List[Any]]:
        """Parse a nested key-value list."""
        if len(data) % 2 != 0:
            return data  # Return as-is if not a proper key-value structure

        result = {}
        for i in range(0, len(data), 2):
            key = self._decode_value(data[i])
            value = self._decode_value(data[i + 1])
            result[key] = value
        return result

    def _parse_attributes(self, attributes: List[Any]) -> List[Dict[str, Any]]:
        """Parse the attributes section which contains field definitions."""
        result = []
        for attr in attributes:
            if isinstance(attr, list):
                parsed_attr = self._parse_key_value_list(attr)
                # Only proceed if we got a dictionary back
                if isinstance(parsed_attr, dict):
                    # Special handling for the 'index' field within attributes
                    if "index" in parsed_attr and isinstance(
                        parsed_attr["index"], list
                    ):
                        index_parsed = self._parse_key_value_list(parsed_attr["index"])
                        if isinstance(index_parsed, dict):
                            parsed_attr["index"] = index_parsed
                            # Parse algorithm sub-section if present
                            if "algorithm" in parsed_attr["index"] and isinstance(
                                parsed_attr["index"]["algorithm"], list
                            ):
                                algorithm_parsed = self._parse_key_value_list(
                                    parsed_attr["index"]["algorithm"]
                                )
                                if isinstance(algorithm_parsed, dict):
                                    parsed_attr["index"]["algorithm"] = algorithm_parsed
                    result.append(parsed_attr)
                else:
                    # If it's not a dict, append as-is (shouldn't happen normally)
                    result.append(parsed_attr)
            else:
                result.append(attr)
        return result

    def _decode_value(self, value: Any) -> Any:
        """Decode byte strings and convert numeric strings to appropriate types."""
        if isinstance(value, bytes):
            decoded = value.decode("utf-8")
            # Try to convert numeric strings to appropriate types
            if decoded.isdigit():
                return int(decoded)
            try:
                # Try float conversion for decimal numbers
                float_val = float(decoded)
                # Only return float if it actually has decimal places
                if "." in decoded:
                    return float_val
                return int(float_val)
            except ValueError:
                return decoded
        elif isinstance(value, list):
            return [self._decode_value(item) for item in value]
        return value

    # Easy access properties
    @property
    def index_name(self) -> str:
        """Get the index name."""
        return self.parsed_data.get("index_name", "")

    @property
    def index_definition(self) -> Dict[str, Any]:
        """Get the index definition."""
        return self.parsed_data.get("index_definition", {})

    @property
    def attributes(self) -> List[Dict[str, Any]]:
        """Get all field attributes."""
        return self.parsed_data.get("attributes", [])

    @property
    def num_docs(self) -> int:
        """Get the number of documents in the index."""
        return self.parsed_data.get("num_docs", 0)

    @property
    def num_records(self) -> int:
        """Get the number of records in the index."""
        return self.parsed_data.get("num_records", 0)

    @property
    def hash_indexing_failures(self) -> int:
        """Get the number of hash indexing failures."""
        return self.parsed_data.get("hash_indexing_failures", 0)

    @property
    def backfill_in_progress(self) -> bool:
        """Check if backfill is in progress."""
        return bool(self.parsed_data.get("backfill_in_progress", 0))

    @property
    def backfill_complete_percent(self) -> float:
        """Get the backfill completion percentage."""
        return self.parsed_data.get("backfill_complete_percent", 0.0)

    @property
    def mutation_queue_size(self) -> int:
        """Get the mutation queue size."""
        return self.parsed_data.get("mutation_queue_size", 0)

    @property
    def recent_mutations_queue_delay(self) -> str:
        """Get the recent mutations queue delay."""
        return self.parsed_data.get("recent_mutations_queue_delay", "0 sec")

    @property
    def state(self) -> str:
        """Get the index state."""
        return self.parsed_data.get("state", "")

    # Utility methods
    def get_attribute_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get an attribute by its identifier name.

        Args:
            name: The attribute identifier to search for

        Returns:
            The attribute dictionary if found, None otherwise
        """
        for attr in self.attributes:
            if attr.get("identifier") == name:
                return attr
        return None

    def get_attributes_by_type(self, field_type: str) -> List[Dict[str, Any]]:
        """
        Get all attributes of a specific type.

        Args:
            field_type: The field type to filter by (e.g., 'VECTOR', 'TEXT', 'NUMERIC')

        Returns:
            List of attributes matching the specified type
        """
        return [attr for attr in self.attributes if attr.get("type") == field_type]

    @property
    def vector_attributes(self) -> List[Dict[str, Any]]:
        """Get all vector field attributes."""
        return self.get_attributes_by_type("VECTOR")

    @property
    def text_attributes(self) -> List[Dict[str, Any]]:
        """Get all text field attributes."""
        return self.get_attributes_by_type("TEXT")

    @property
    def numeric_attributes(self) -> List[Dict[str, Any]]:
        """Get all numeric field attributes."""
        return self.get_attributes_by_type("NUMERIC")

    def has_indexing_failures(self) -> bool:
        """Check if there are any indexing failures."""
        return self.hash_indexing_failures > 0

    def is_ready(self) -> bool:
        """Check if the index is in ready state."""
        return self.state.lower() == "ready"

    def is_backfill_complete(self) -> bool:
        """Check if backfill is complete."""
        return not self.backfill_in_progress and self.backfill_complete_percent >= 1.0

    def get_text_min_stem_size(self, field_name: str) -> Optional[int]:
        """
        Get the min_stem_size of a text field.

        Args:
            field_name: The name of the text field

        Returns:
            The min_stem_size if found, None otherwise
        """
        attr = self.get_attribute_by_name(field_name)
        if attr and attr.get("type") == "TEXT":
            return attr.get("MIN_STEM_SIZE")
        return None

    def get_text_no_stem(self, field_name: str) -> Optional[bool]:
        """
        Get the no_stem setting of a text field.

        Args:
            field_name: The name of the text field

        Returns:
            True if no_stem is enabled, False if not, None if field not found
        """
        attr = self.get_attribute_by_name(field_name)
        if attr and attr.get("type") == "TEXT":
            return bool(attr.get("NO_STEM", 0))
        return None

    def get_text_with_suffix_trie(self, field_name: str) -> Optional[bool]:
        """
        Get the with_suffix_trie setting of a text field.

        Args:
            field_name: The name of the text field

        Returns:
            True if suffix trie is enabled, False if not, None if field not found
        """
        attr = self.get_attribute_by_name(field_name)
        if attr and attr.get("type") == "TEXT":
            return bool(attr.get("WITH_SUFFIX_TRIE", 0))
        return None

    @property
    def language(self) -> Optional[str]:
        """Get the language setting for text indexes."""
        return self.parsed_data.get("language")

    @property
    def punctuation(self) -> Optional[str]:
        """Get the punctuation characters for text indexes."""
        return self.parsed_data.get("punctuation")

    @property
    def stop_words(self) -> Optional[List[str]]:
        """Get the stop words list for text indexes."""
        return self.parsed_data.get("stop_words")

    @property
    def with_offsets(self) -> Optional[bool]:
        """Get the with_offsets setting for text indexes."""
        offsets = self.parsed_data.get("with_offsets")
        return bool(offsets) if offsets is not None else None

    def get_vector_dimensions(self, field_name: str) -> Optional[int]:
        """
        Get the dimensions of a vector field.

        Args:
            field_name: The name of the vector field

        Returns:
            The number of dimensions, or None if field not found or not a vector
        """
        attr = self.get_attribute_by_name(field_name)
        if attr and attr.get("type") == "VECTOR":
            index_info = attr.get("index", {})
            return index_info.get("dimensions")
        return None

    def get_vector_algorithm(self, field_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the algorithm configuration for a vector field.

        Args:
            field_name: The name of the vector field

        Returns:
            The algorithm configuration dict, or None if not found
        """
        attr = self.get_attribute_by_name(field_name)
        if attr and attr.get("type") == "VECTOR":
            index_info = attr.get("index", {})
            return index_info.get("algorithm")
        return None

    def __str__(self) -> str:
        """String representation of the parsed data."""
        return f"FTInfoParser(index='{self.index_name}', docs={self.num_docs}, state='{self.state}')"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"FTInfoParser(index_name='{self.index_name}', num_docs={self.num_docs}, state='{self.state}', attributes_count={len(self.attributes)})"

    def to_dict(self) -> Dict[str, Any]:
        """Return the parsed data as a dictionary."""
        return self.parsed_data.copy()

    def pretty_print(self) -> str:
        """
        Return a pretty-printed string representation of the index information.
        """
        lines = [f"Index Information for '{self.index_name}':"]
        lines.append("=" * (len(lines[0])))

        # Basic info
        lines.append(f"State: {self.state}")
        lines.append(f"Documents: {self.num_docs}")
        lines.append(f"Terms: {self.num_terms}")
        lines.append(f"Records: {self.num_records}")

        if self.has_indexing_failures():
            lines.append(f"Indexing Failures: {self.hash_indexing_failures}")

        # Index definition
        if self.index_definition:
            lines.append("\nIndex Definition:")
            for key, value in self.index_definition.items():
                lines.append(f"  {key}: {value}")

        # Attributes
        if self.attributes:
            lines.append(f"\nAttributes ({len(self.attributes)}):")
            for i, attr in enumerate(self.attributes):
                lines.append(f"  [{i+1}] {attr.get('identifier', 'Unknown')}")
                lines.append(f"      Type: {attr.get('type', 'Unknown')}")
                if attr.get("type") == "VECTOR" and "index" in attr:
                    index_info = attr["index"]
                    lines.append(
                        f"      Dimensions: {index_info.get('dimensions', 'Unknown')}"
                    )
                    lines.append(
                        f"      Distance Metric: {index_info.get('distance_metric', 'Unknown')}"
                    )
                    if "algorithm" in index_info:
                        algo = index_info["algorithm"]
                        lines.append(f"      Algorithm: {algo.get('name', 'Unknown')}")

        # Backfill status
        if self.backfill_in_progress:
            lines.append(
                f"\nBackfill in progress: {self.backfill_complete_percent:.1%}"
            )
        elif not self.is_backfill_complete():
            lines.append(f"\nBackfill paused at: {self.backfill_complete_percent:.1%}")

        return "\n".join(lines)
