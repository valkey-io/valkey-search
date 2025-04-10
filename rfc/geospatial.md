---
RFC: 87
Status: Proposed
---

# Title

Geospatial Indexing

## Abstract

This proposal introduces a new field types, `GEO` and `GEOSHAPE`, into the Valkey Search module. This enhancement enables efficient indexing and querying of geospatial data, supporting operations such as finding keys within a specified radius or bounding box. The geospatial indexing leverages Boost.Geometry's R-tree implementation to ensure high performance and scalability.

## Motivation

Efficient geospatial querying is essential for applications that handle location-based data, such as mapping services, ride-sharing platforms, and geographic information systems (GIS). Integrating geospatial indexing into Valkey Search will allow developers to perform complex location-based queries directly within Valkey, reducing the need for external processing and thereby improving performance and simplifying application architecture.

## Terminology

In the context of this RFC:

- **Key**: A Valkey key, which could be the name of the key or the contents of a HASH or JSON key.
- **Field**: A component of an index. Each field has a type and a path.
- **Index**: A collection of fields and field-indexes. The object created by the `FT.CREATE` command.
- **Field-Index**: A data structure associated with a field that accelerates the operation of search operators for this field type.
- **Geospatial Data**: Data that represents the geographic location and shape of objects.
- **R-tree**: A tree data structure used for spatial access methods, i.e., for indexing multi-dimensional information such as geographical coordinates.

## Design

### Field Type

Two new field types, `GEO` and `GEOSHAPE`, are introduced to represent geospatial data:

- **`GEO`**: This field type indexes geographic coordinates (latitude and longitude) and supports efficient spatial queries, such as radius searches and bounding box searches.
- **`GEOSHAPE`**: This field type is designed for indexing and querying complex geospatial shapes, such as polygons and multi-polygons. It supports advanced spatial queries like containment, intersection, and proximity. The `GEOSHAPE` field type can use either `FLAT` (Cartesian coordinates) or `SPHERICAL` (geographical coordinates), making it suitable for applications like mapping city boundaries or regions with flexible coordinate systems.

### Indexing

The `GEO` and `GEOSHAPE` fields utilize Boost.Geometry's R-tree implementation for spatial indexing. The R-tree is a self-balanced data structure that organizes spatial data in a way that minimizes the number of nodes traversed during searches, optimizing query performance.

### Packing Algorithms

Boost.Geometry implements several packing algorithms for its R-tree implementation, each with various advantages and disadvantages. To leverage this flexibility, an optional parameter `OPTIMIZED_FOR` is proposed to allow users to choose the optimal algorithm for their use case:

- `SEARCH`: Utilizes the **R\*-tree algorithm**, optimized for minimizing overlap between nodes and enhancing search performance. It typically offers significant speedup in query operations at the cost of slightly slower insertions and deletions.
- `MUTATION`: Employs the **Quadratic split algorithm**, optimized for rapid insertions, deletions, and updates. It significantly speeds up data mutation operations but may result in slightly slower search performance due to increased node overlap.

The default value for `OPTIMIZED_FOR` is `SEARCH`.

By selecting different algorithms, this option provides flexibility to optimize performance based on specific application needs.

### Querying

The `GEO` and `GEOSHAPE` fields support the following query operations:

- **Radius Search**: Finds keys within a specified radius from a given point.
- **Bounding Box Search**: Finds keys within a specified rectangular area.
- **Nearest Neighbor Search**: Finds the nearest keys to a given point.

These operations allow efficient retrieval of geospatial data based on proximity and spatial relationships.

## Commands

### FT.CREATE

The `FT.CREATE` command is extended to support the `GEO` and `GEOSHAPE` field types, along with the optional optimization parameter:

```
FT.CREATE <index_name> ON <data_type> PREFIX <prefix_count> <prefix> SCHEMA <field_name> GEO [OPTIMIZED_FOR SEARCH | MUTATION]
FT.CREATE <index_name> ON <data_type> PREFIX <prefix_count> <prefix> SCHEMA <field_name> GEOSHAPE [FLAT | SPHERICAL] [OPTIMIZED_FOR SEARCH | MUTATION]
```

The `GEO` field type is used for indexing geographic coordinates (latitude and longitude), while the `GEOSHAPE` field type is designed for indexing and querying complex geospatial shapes, such as polygons and multi-polygons. Both field types leverage Boost.Geometry's R-tree implementation for efficient spatial indexing.

The `GEOSHAPE` field type supports both `FLAT` (Cartesian coordinates) and `SPHERICAL` (geographical coordinates) options, making it suitable for applications like mapping city boundaries or regions with flexible coordinate systems.

#### Example with GEO:

```
FT.CREATE places_idx ON HASH PREFIX 1 "place:" SCHEMA location GEO OPTIMIZED_FOR SEARCH
```

#### Example with GEOSHAPE:

```
FT.CREATE regions_idx ON HASH PREFIX 1 "region:" SCHEMA boundary GEOSHAPE FLAT OPTIMIZED_FOR SEARCH
```

### FT.SEARCH

The `FT.SEARCH` command is extended to support geospatial queries:

```
FT.SEARCH <index_name> <GEO:[lon lat radius unit]>
FT.SEARCH <index_name> <GEOSHAPE:[{WITHIN | CONTAINS | INTERSECTS | DISJOINT} SHAPE]>
```

#### Example with GEO:

```
FT.SEARCH idx:pizza "@store_location:[-1.2568 30.624 10 mi]"
```

#### Example with GEOSHAPE:

```
FT.SEARCH idx:pizza "@store_zone:[WITHIN $zone] PARAMS 2 zone "POLYGON((-16 25, 35 39, 38 67, -24 60, -23 36))"
```

## Implementation Details

- **Boost.Geometry Integration**: Utilizes the R-tree implementation from Boost.Geometry for efficient spatial indexing.
- **Packing Algorithms**: Provides flexibility to select packing algorithms optimized either for search performance or data modifications.
- **Data Storage**: Geospatial data is stored in fields designated as `GEO` type or `GEOSHAPE` type, with the indexing mechanism parsing these fields to extract coordinates.
- **Performance Considerations**: Ensures efficient insertions, deletions, and queries, even with large datasets.

## Backward Compatibility

The introduction of the `GEO` and `GEOSHAPE` field types and associated commands is backward compatible. Existing functionality remains unchanged, and the new features are additive.

## Open Questions

- **Coordinate Reference Systems**: Should multiple coordinate reference systems be supported, or should the system standardize on WGS 84?
- **3D Geospatial Data**: Should three-dimensional data (latitude, longitude, altitude) be supported in the future?
- **Advanced Spatial Queries**: Should additional spatial predicates (e.g., intersects, contains) be supported beyond radius and bounding box searches?

Feedback and discussions are welcome to refine this proposal.
