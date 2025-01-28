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
                       Example Header                                                     │ Chunk 1  Chunk 2  Chunk 3  EOF      │
             ┌───────────────────────────────────────────┐                                │┌───────┐┌───────┐┌───────┐┌────────┐│
Unknown types│   Type   Required  Enc.    Header         │                                ││...    ││...    ││...    ││        ││
 are skipped │  (enum)           Version  Content        │                     ┌──────────│└───────┘└───────┘└───────┘└────────┘│
     │       │ ┌───────┐┌──────┐┌───────┐┌──────────────┐│                     │          └─────────────────────────────────────┘
     │       │ │ Index ││      ││       ││E.g, attribute││                     │
     └───────┼►│Content││ True ││   1   ││name...       ││  ┌──────────────────┼────────────────────────────┐
             │ └───────┘└──────┘└───────┘└──────────────┘│  │ Header    Binary │     Header    Binary       │
             └─────────────────────┬─────────────────────┘  │ Proto 1   Dump 1 ▼     Proto 2   Dump 2       │
                                   │                        │┌────────┐┌───────────┐┌────────┐┌───────────┐ │
                                   └────────────────────────►│        ││ ...       ││        ││ ...       │ │
                                                            │└────────┘└───────────┘└────────┘└───────────┘ │
                                                            └───────────────────────────────────────────────┘
                                                            Example Supplemental Content
                       RDB Aux Section                                │
                  ┌───────────────────────────────────────────────────┼────────────────────────────────────────────────────────┐
                  │ Module Type OpCode When Private Module Data       │                                                        │
                  │┌───────────┐┌────┐┌────┐┌─────────────────────────┼───────────────────────────────────────────────────────┐│
                  ││           ││    ││    ││Section  VSRDBSection    │  Supplemental      VSRDBSection       Supplemental    ││
                  ││           ││    ││    ││ Count  Proto Payload 1  │ Content for #1    Proto Payload 2    Content for #2   ││
                  ││"SchMgr-VS"││ 2  ││ 2  ││┌─────┐┌───────────────┐┌▼─────────────────┐┌───────────────┐┌──────────────────┐││
                  ││           ││    ││    │││  2  ││               ││                  ││               ││                  │││
                  ││           ││    ││    ││└─────┘└──────▲────────┘└──────────────────┘└───────────────┘└──────────────────┘││
                  │└───────────┘└────┘└────┘└──────────────┼──────────────────────────────────────────────────────────────────┘│
                  └────────────────────────────────────────┼───────────────────────────────────────────────────────────────────┘
                                                           │
                                                           │
                                                           │
                                                       Example VSRDBSection
                                                     ┌───────────────────────────────────────────────────┐
                                                     │    Type   Required Enc.              Supplemental │
                                                     │   (enum)           Version               Count    │
                                                     │ ┌────────┐┌──────┐┌──────┐┌─────────┐┌──────────┐ │
                                                   ┌─┼─► Schema ││ True ││  1   ││<content>││    2     │ │
                                                   │ │ └────────┘└──────┘└──────┘└─────────┘└──────────┘ │
                                                   │ └───────────────────────────────────────────────────┘
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
   bool required;
   uint32 encoding_version;
   oneof contents {
      IndexSchema index_schema_definition = 2;
      ...
   };
   uint32 supplemental_count = 3;
}
```

`message IndexSchema` follows from the existing definition in src/index_schema.proto.

The goal of breaking into sections is to support skipping optional sections if they are not understood. New sections should be introduced in a manner where failure to understand the new section will generally load fine without loss. Any time that failure to load the section would result in some form of lossiness or inconsistency, we will mark `required` as true and it will result in a downgrade failure. This would only be desired in cases where operators have used new features, and need to think about the downgrade more critically, potentially removing (or, once supported, altering) indexes that will not downgrade gracefully. Similarly, for section types that would like to introduce new required fields, we will include an encoding version, which is conditionally bumped when these new fields are written.

#### Supplemental Contents

In addition to the contents in the RDBSection, we may have contents that are too large to serialize into a protocol buffer in memory. Namely, this would be contents such as "key to ID mappings" for the index, or the index contents itself. To support similar forwards and backwards compatibility characteristics as the RDBSection, we will include a supplemental count in the RDBSection, and upon load, we will begin loading supplemental contents following the load of the RDBSection.

Supplemental content is composed of a supplemental header (in protocol buffer format) and a raw binary dump of the supplemental contents. The supplemental content header will be represented by a proto similar to:

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
   bool required = 2;
   uint32 encoding_version = 3;
   oneof header {
      IndexContentHeader index_content_header = 4;
      ...
   };
}
```

The supplemental header will allow differing versions of the module to identify if this supplemental content is understood, and if not, whether that is okay. This is done through exposing a type (similar to RDBSection), an encoding version, and a `required` flag. There may be additional header contents needed by different supplemental content types as well, e.g. for index contents, we will need to know which attribute the index is corresponding to (represeneted by `IndexContentHeader` above).

When loading supplemental content, the content will be ignored if the type is unknown, or the encoding version is higher than we understand, and `required` is not true. If `required` is true, we will have to return an error if we can't understand the contents.

#### Binary Dump

With the current Valkey RDB APIs, modules only have the ability to perform a complete read or write of a certain type to the RDB, there is no streaming capabilities. If the module were to attempt to write a gigabyte of data, it requires the full gigabyte to be serialized in memory, then passed the RDB APIs to save into the RDB.

To prevent memory overhead for large binary dumps, we will implement chunking of binary data to reduce the size of the individual RDB write API calls. We will use a simple protocol buffer with the following format to represent a chunk in a binary dump:

```
message SupplementalContentChunk {
   bytes binary_content = 1;
}
```

To support previous version's ability to skip binary contents contained in supplemental content sections, the end of a binary dump is marked by a single SupplementalContentChunk that has no data. This will signal EOF, and the loading procedure will know that the next item is either the next SupplementalContentHeader, or the next RDBSection if no more SupplementalContentHeaders exist for the current RDBSection.


### Semantic Versioning and Downgrade

Whenever the contents of the RDB are changed in a manner that an RDB could be produced on the new release that the immediate previous release could not load, we will need to bump the major version and add this to our release notes.

To support downgrade even if the index contents encoding has changed, we will support a configuration:

```
CONFIG SET valkeysearch-allow-downgrade-with-rebuild yes
```

When this configuration is provided, the produced dump will mark all index contents as not required. By default, the configuration will be set to `no`. When set to `no`, the RDB loading process will output the following error when the requried contents can not be loaded:

```
ValkeySearch RDB contents contain defintions for RDB sections that are not supported by this version. If you are downgrading, ensure all feature usage on the new version of ValkeySearch is supported by this version and retry. If you are okay with forcing a rebuild on downgrade, set "valkeysearch-allow-downgrade-with-rebuild" to "yes" on the process running the new ValkeySearch version and retrigger the RDB save.
```

### Example: Adding Vector Quantization

With the above design, suppose that we are substantially changing the index to support a vector quantization option on `FT.CREATE`. For simplicity, suppose this is just a boolean "on" or "off" flag.

On the old version, in the RDB, we would output something like the following:

```
RDBSection {
   type: RDB_SECTION_INDEX_SCHEMA,
   required: true,
   encoding_version: 1,
   index_schema_contents: {
      name: "my_index",
      attributes: [
         {
            identifier: "my_vector",
            index: {
               VectorIndex {
                  dimension_count: 100,
                  algorithm: {
                     HNSWAlgorithm {
                        ...
                     }
                  }
                  ...
               }
            }
         },
         {
            identifier: "my_tag",
            index: {
               TagIndex {
                  ...
               }
            }
         }
      ],
   }
   supplemental_count: 2,
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_KEY_TO_ID,
   required: true,
   enc_version: 1,
   key_to_id_header: {
      attribute_name: "my_vector"
   }
}
SupplementalContentChunk {
   contents: <key_to_id_dump_1>
}
SupplementalContentChunk {
   contents: <key_to_id_dump_2>
}
...
SupplementalContentChunk {
   contents: ""
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_INDEX_CONTENTS,
   required: true,
   enc_version: 1,
   index_contents_header {
      attribute_name: "my_vector",
   }
}
SupplementalContentChunk {
   contents: <my_vector_contents_1>
}
SupplementalContentChunk {
   contents: <my_vector_contents_2>
}
...
SupplementalContentChunk {
   contents: ""
}
```

Suppose that the new version introduces a new field in VectoIndex - `bool quantize`. Protocol buffers initialize the default values to a "zero-like" value, so this will be `false` if not previously set. We could also add it as `optional bool quantize`, and specifically check if the VectorIndex proto has the `quantize` field set explicitly. On the upgrade path - we will default initialize the value of `quantize` to false (or handle the default case as we see fit, if we use `optional`).

Suppose now the user recreates the index with `quantize` set to true. If they then choose to downgrade back to the previous version, we will output an RDB that looks like:

```
RDBSection {
   type: RDB_SECTION_INDEX_SCHEMA,
   required: true,
   enc_version: 2,
   index_schema_contents: {
      name: "my_index",
      attributes: [
         {
            identifier: "my_vector",
            index: {
               VectorIndex {
                  dimension_count: 100,
                  algorithm: {
                     HNSWAlgorithm {
                        ...
                     }
                  }
                  quantize: true
                  ...
               }
            }
         },
         {
            identifier: "my_tag",
            index: {
               TagIndex {
                  ...
               }
            }
         }
      ],
   }
   supplemental_count: 2,
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_KEY_TO_ID,
   required: true,
   enc_version: 1,
   key_to_id_header: {
      attribute_name: "my_vector"
   }
}
SupplementalContentChunk {
   contents: <key_to_id_dump_1>
}
SupplementalContentChunk {
   contents: <key_to_id_dump_2>
}
...
SupplementalContentChunk {
   contents: ""
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_INDEX_CONTENTS,
   required: true,
   enc_version: 2,
   index_contents_header {
      attribute_name: "my_vector",
   }
}
SupplementalContentChunk {
   contents: <my_vector_contents_1>
}
SupplementalContentChunk {
   contents: <my_vector_contents_2>
}
...
SupplementalContentChunk {
   contents: ""
}
```

On the new version, when the new feature `quantize` is used, we will bump the encoding version of the RDBSection containing the index schema definition (it now contains the `quantize` field, which will be lost on downgrade). Similarly, we will also bump the encoding version of the SupplementalContentHeader for the index contents - as the format has changed in a way that will not be understood by older versions. On loading this on the previous version, we will fail fast with a useful error message (documented above).

Upon reading this message, the user might recreate the index with `quantize` set to false. In this case, we will output the following RDB contents:

```
RDBSection {
   type: RDB_SECTION_INDEX_SCHEMA,
   required: true,
   enc_version: 1,
   index_schema_contents: {
      name: "my_index",
      attributes: [
         {
            identifier: "my_vector",
            index: {
               VectorIndex {
                  dimension_count: 100,
                  algorithm: {
                     HNSWAlgorithm {
                        ...
                     }
                  }
                  quantize: false
                  ...
               }
            }
         },
         {
            identifier: "my_tag",
            index: {
               TagIndex {
                  ...
               }
            }
         }
      ],
   }
   supplemental_count: 2,
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_KEY_TO_ID,
   required: true,
   enc_version: 1,
   key_to_id_header: {
      attribute_name: "my_vector"
   }
}
SupplementalContentChunk {
   contents: <key_to_id_dump_1>
}
SupplementalContentChunk {
   contents: <key_to_id_dump_2>
}
...
SupplementalContentChunk {
   contents: ""
}
SupplementalContentHeader {
   type: SUPPLEMENTAL_INDEX_CONTENTS,
   required: true,
   enc_version: 1,
   index_contents_header {
      attribute_name: "my_vector",
   }
}
SupplementalContentChunk {
   contents: <my_vector_contents_1>
}
SupplementalContentChunk {
   contents: <my_vector_contents_2>
}
...
SupplementalContentChunk {
   contents: ""
}
```

Upon retry, the RDB load will succeed.

### Testing

Cross version testing will be a must to ensure that we don't get this wrong. We should expand the existing integration tests with such functionality.
