---
RFC: 23
Status: Proposed
---

# RDB Format for ValkeySearch

## Abstract

To support rolling upgrade and downgrade, ValkeySearch needs to start with a robust RDB format that will support forwards and backwards compatibility.

## Motivation

Our existing RDB format is a good start, but it also is fairly rigid and won't work for the foreseeable future. Here are some examples of cases which would be difficult to implement with forwards and backwards compatibility in the current format:

 * Saving in-flight operations on index serialization
 * Saving non-vector contents (Tag, Numeric)
 * Saving "externalized" vector contents (those shared with the engine)
 * Supporting changes to the serialized index contents
 * Supporting new, large payloads alongside existing index payloads (i.e. chunked/streamed reading from RDB)

## Design considerations

- If no new features are used, we should support full forwards compatibility
  - If we cannot support full forwards compatibility, we will need to bump the major version in order to follow semantic versioning
  - Downgrade across semantic versions should be supported, but may require index rebuild
- If new features are used or the encoding of the index has changed, we should ensure this lack of compatibility is understood on previous versions, and fast fail the RDB load
  - A configuration should be exposed to toggle marking index contents as required in order to support downgrade-with-rebuild
- For upgrade, we need full backwards compatibility in all situations
- Large payloads that may not fit in memory need to be supported via chunking the contents into the RDB

## Specification

### RDB Integration

 - We will store all module data in aux metadata to prevent leaking indexes into the keyspace
 - We will use `aux_save2` style callbacks, so that if the module is completely unused, the RDB can be loaded on a Valkey server that has no ValkeySearch module installed.
 - We will store all schema contents in a single piece of aux metadata for the module type "SchMgr-VS".

### RDB Payload

Below is a diagram of the proposed payload design:

```
                                                                            Example Binary Dump
                                                                           ┌─────────────────────────────────────┐
                       Example Header                                      │ Chunk 1  Chunk 2  Chunk 3  EOF      │
             ┌──────────────────────────┐                                  │┌───────┐┌───────┐┌───────┐┌────────┐│
Unknown types│   Type    Header         │                                  ││...    ││...    ││...    ││        ││
 are skipped │  (enum)   Content        │                       ┌──────────│└───────┘└───────┘└───────┘└────────┘│
     │       │ ┌───────┐┌──────────────┐│                       │          └─────────────────────────────────────┘
     │       │ │ Index ││E.g, attribute││                       │
     └───────┼►│Content││name...       ││    ┌──────────────────┼────────────────────────────┐
             │ └───────┘└──────────────┘│    │ Header    Binary │     Header    Binary       │
             └──────────────────────────┘    │ Proto 1   Dump 1 ▼     Proto 2   Dump 2       │
                                   │         │┌────────┐┌───────────┐┌────────┐┌───────────┐ │
                                   └─────────►│        ││ ...       ││        ││ ...       │ │
                                             │└────────┘└───────────┘└────────┘└───────────┘ │
                                             └───────────────────────────────────────────────┘
                                                                Example Supplemental Content
                       RDB Aux Section                                          │
                  ┌─────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────┐
                  │ Module Type OpCode When Private Module Data                 │                                                        │
                  │┌───────────┐┌────┐┌────┐┌───────────────────────────────────┼───────────────────────────────────────────────────────┐│
                  ││           ││    ││    ││ Min. Sem. Section  VSRDBSection   │  Supplemental      VSRDBSection       Supplemental    ││
                  ││           ││    ││    ││ Version    Count  Proto Payload 1 │ Content for #1    Proto Payload 2    Content for #2   ││
                  ││"SchMgr-VS"││ 2  ││ 2  ││┌────────┐┌─────┐┌───────────────┐┌▼─────────────────┐┌───────────────┐┌──────────────────┐││
                  ││           ││    ││    │││  1.0.0 ││  2  ││               ││                  ││               ││                  │││
                  ││           ││    ││    ││└────────┘└─────┘└──────▲────────┘└──────────────────┘└───────────────┘└──────────────────┘││
                  │└───────────┘└────┘└────┘└────────────────────────┼──────────────────────────────────────────────────────────────────┘│
                  └──────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────┘
                                                                     │
                                                                     │
                                                                     │
                                                                 Example VSRDBSection
                                                               ┌───────────────────────────────────┐
                                                               │    Type              Supplemental │
                                                               │   (enum)                 Count    │
                                                               │ ┌────────┐┌─────────┐┌──────────┐ │
                                                             ┌─┼─► Schema ││<content>││    2     │ │
                                                             │ │ └────────┘└─────────┘└──────────┘ │
                                                             │ └───────────────────────────────────┘
                                                             │
                                                           Unknown types
                                                            are skipped
```

#### RDBSection

The primary unit of the RDB Payload is the RDBSection, which will have a proto definition similar to:

```
enum RDBSectionType {
   RDB_SECTION_INDEX_SCHEMA,
   ...
}

message RDBSection {
   RDBSectionType type = 1;
   oneof contents {
      IndexSchema index_schema_definition = 2;
      ...
   };
   uint32 supplemental_count = 3;
}
```

(`message IndexSchema` follows from the existing definition in src/index_schema.proto)

Before the `RDBSection`s, a single integer will be emitted using `ValekyModule_SaveUnsigned` to denote the number of `RDBSection`s emitted. Each `RDBSection` is then saved into the RDB using a single `ValkeyModule_SaveString` API call.

The goal of breaking into sections is to support skipping optional sections if they are not understood. New sections should be introduced in a manner where failure to understand the new section will generally load fine without loss. Any time that failure to load the section would result in some form of lossiness or inconsistency, we will need to bump the minimum semantic version to the minimum version that will be capable of reading it. When loading an RDB, if the semantic version is greater than our version, we can fail fast with a message. This would be desired in cases where operators have used new features and therefore need to think about the downgrade more critically, potentially removing (or, once supported, altering) indexes that will not downgrade gracefully.

#### Supplemental Contents

In addition to the contents in the `RDBSection`, we may have contents that are too large to serialize into a protocol buffer in memory. Namely, this would be contents such as "key to ID mappings" for the index, or the index contents itself. To support similar forwards and backwards compatibility characteristics as the RDBSection, we will include a supplemental count in the `RDBSection`, and upon load, we will begin loading supplemental contents following the load of the `RDBSection`.

Supplemental content is composed of a supplemental header (in protocol buffer format) and a raw chunked binary dump of the supplemental contents. The supplemental content header will be represented by a proto similar to:

```
enum SupplementalContentType {
   SUPPLEMENTAL_CONTENT_INDEX_CONTENT,
   SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP,
   ...
}

message IndexContentHeader {
   Attribute attribute = 1;
}

message SupplementalContentHeader {
   SupplementalContentType type = 1;
   oneof header {
      IndexContentHeader index_content_header = 2;
      ...
   };
}
```

Each `SupplementalContentHeader` will be emitted using a single `ValkeyModule_SaveString` call, similar to the `RDBSection` proto above. Following the `SupplementalContentHeader`, the binary payload will be emitted in a chunked manner, each individually saved with a call to `ValkeyModule_SaveString`, terminated with an EOF marker. See below for more details on the dump format.

There may be additional non-chunked header contents needed by different supplemental content types as well, e.g. for index contents, we will need to know which attribute the index is corresponding to (represented by `IndexContentHeader` above).

#### Binary Dump

With the current Valkey RDB APIs, modules only have the ability to perform a complete read or write of a certain type to the RDB, there is no streaming capabilities. If the module were to attempt to write a gigabyte of data, it requires the full gigabyte to be serialized in memory, then passed the RDB APIs to save into the RDB.

To prevent memory overhead for large binary dumps, we will implement chunking of binary data to reduce the size of the individual RDB write API calls. We will use a simple protocol buffer with the following format to represent a chunk in a binary dump:

```
message SupplementalContentChunk {
   bytes binary_content = 1;
}
```

Many `SupplementalContentChunk`s will follow a single `SupplementalContentHeader`. Each chunk will be emitted with a single call `ValkeyModule_SaveString`. Finally, after all `SupplementalContentChunk`s are written for the header, a single empty `SupplementalContentChunk` is emitted to denote EOF. We choose an EOF marker instead of a prefixed length, since precomputing the number of chunks for a large data type is difficult.

When a previous version sees a `SupplementalContentHeader` it doesn't understand and is optional, it will call `ValkeyModule_LoadString` and deserialize into a `SupplementalContentChunk`. The loading process can check the length of the content of the chunk and identify the EOF without understanding the contents of the dump itself. After EOF, the next item is either the next `SupplementalContentHeader`, or the next RDBSection if no more `SupplementalContentHeader`s exist for the current `RDBSection`.


### Semantic Versioning and Downgrade

Whenever the contents of the RDB are changed in a manner that an RDB could be produced on the new release that the immediate previous release could not load, we will need to bump the major version and add this to our release notes.

When we dump our RDB payload, we will also compute the minimum semantic version required to understand it. Typically, this will be a function of the feature usage and what version those features are introduced in. But it also leaves open the possibility to change the RDB format over time, either by breaking downgrade, or by employing strategies like skip versions (i.e. version 4.X.X may not understand 6.X.X, but 5.X.X can understand 6.X.X and output to 4.X.X).

If an RDB is loaded with a semantic version that is higher than the current version, the following log would be emitted and loading will fail:

```
ValkeySearch RDB contents require minimum version X.X.X. If you are downgrading, ensure all feature usage on the new version of ValkeySearch is supported by this version and retry.
```

### Coding Interface

#### SupplementalRDBWriter

The `SupplementalRDBWriter` interface provides methods to write and flush data.

##### `void setChunkSize(size_t chunk_size)`

Sets the default chunk size (i.e. chunk size if no calls to flush are made).

- **Parameters:**
  - `chunk_size` (`size_t`): The size of the chunk, or if not specified, defaults to 1MiB.

##### `void write(absl::string_view buf)`

Writes a chunk of data.

- **Parameters:**
  - `buf` (`absl::string_view`): The data buffer to write.
- **Behavior:**
  - The function copies the provided buffer, and might flush it to the RDB.

##### `void flush()`

Flushes any buffered data.

- **Behavior:**
  - Ensures that all written data is committed to the RDB.
  - Contents are automatically flushed on destruction.

##### `void close()`

Closes the writer.

- **Behavior:**
  - Emit an EOF marker to the RDB
  - Also called implicitly on destruction

#### SupplementalRDBReader

The `SupplementalRDBReader` interface provides a method to read chunks of data.

##### `std::string read()`

Reads a chunk of data.

- **Returns:**
  - `std::string`: A chunk of data that is read from the RDB.
- **Behavior:**
  - Returns the next available chunk of data.
  - If no data is available, the return value may be an empty `std::string`.

#### Example Usage

```

void IndexSchema::RDBSave() {
   data_model::RDBSection section;
   section.set_type(RdbSectionType::RDB_SECTION_INDEX_SCHEMA);
   section.set_index_schema_definition(this->ToProto());
   section.set_supplemental_count(this->GetAttributeCount() * 2);
   ValkeyModule_RDBSaveString(section.SerializeAsString());

   for (auto &attribute : attributes_) {
      data_model::SupplementalContent index_content;
      index_content.set_type(SUPPLEMENTAL_CONTENT_INDEX_CONTENT);
      data_model::IndexContentHeader index_content_header;
      index_content_header.set_attribute(attribute.second.ToProto());
      index_content.set_index_content_header(index_content_header);
      ValkeyModule_RDBSaveString(index_content.SerializeAsString());

      auto index_writer = std::make_unique<SupplementalRDBWriter>();
      attribute.second.GetIndex()->SaveIndex(std::move(index_writer));
      /* SaveIndex writes all index contents as binary, then the writer is destructed once done */

      data_model::SupplementalContent key_mapping;
      key_mapping.set_type(SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP);
      data_model::KeyToIdHeader key_to_id_header;
      key_to_id_header.set_attribute(attribute.second.ToProto());
      key_mapping.set_key_to_id_header(key_to_id_header);
      ValkeyModule_RDBSaveString(key_mapping.SerializeAsString());

      auto key_to_id_writer = std::make_unique<SupplementalRDBWriter>();
      attribute.second.GetIndex()->SaveMapping(std::move(index_writer));
      /* SaveMapping writes all key to id mappings as binary, then the writer is destructed once done */
   }
}
```

### Testing

Cross version testing will be a must to ensure that we don't get this wrong. We should expand the existing integration tests with such functionality.
