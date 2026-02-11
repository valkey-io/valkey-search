### Valkey Search 1.2.0 RC1 - Release Notes

# \================================================================================ Valkey Search 1.2.0 RC1 - Released Tue 10 Feb 2026

This release candidate marks a major milestone in evolving Valkey Search from a specialized vector engine into a comprehensive search solution. Version 1.2 introduces native Full-Text Search (FTS), advanced Geospatial indexing, and powerful aggregation capabilities.

### Major API and Functionality

*   **Full-Text Search Support:**
    
    *   New `TEXT` field type for schema definitions.
        
    *   Support for advanced query operators: Wildcard matching, Fuzzy matching (Levenshtein automata), and Exact Phrase matching.
        
    *   Configurable "Slop" and "Inorder" parameters for precise phrase proximity control.
        
*   **Geospatial Indexing:**
    
    *   Introduced `GEO` and `GEOSHAPE` field types.
        
    *   Efficient spatial querying (radius and bounding box) powered by Boost.Geometry R-tree integration.
        
*   **Aggregation Engine:**
    
    *   New `FT.AGGREGATE` command allowing for multi-stage processing pipelines including `GROUPBY`, `REDUCE`, `SORTBY`, and `FILTER`.
        
*   **Optimized Indexing Architecture:**
    
    *   Implementation of a two-level hierarchy using Radix Trees and Postings lists for high-speed lexical iteration.
        
    *   Significant memory footprint reduction (up to 30% improvement) in prefix tree representation.
        

### Internal Improvements

*   **Enhanced RDB Format:** Robust RDB serialization versioning to support seamless rolling upgrades and forward/backward compatibility.
    
*   **Memory Management:** New defragmentation mechanisms for Postings objects to optimize long-term memory usage.
    
*   **Performance Simulation:** Integrated `scrape.py` utility for corpus-based memory consumption estimation.
    

### New Configurations

*   `max-fuzzy-distance`: Sets the maximum edit distance for fuzzy searches (Default: 2).
    
*   `max-wildcard-matches`: Limits the expansion of wildcard terms to prevent performance degradation.