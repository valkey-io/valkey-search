/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_VERSION_H_
#define VALKEYSEARCH_SRC_VERSION_H_

#include "utils.h"

//
// Set the module version to the current release
//
constexpr auto kModuleVersion = vmsdk::ValkeyVersion(1, 1, 0);

/* The release stage is used in order to provide release status information.
 * In unstable branch the status is always "dev".
 * During release process the status will be set to rc1,rc2...rcN.
 * When the version is released the status will be "ga". */
#define MODULE_RELEASE_STAGE "rc1"

//
// Set the minimum acceptable server version
//
constexpr auto kMinimumServerVersion = vmsdk::ValkeyVersion(8, 1, 1);

namespace valkey_search {

/*

Valkey's use of protobufs for metadata storage inherently provides substantial
powerful tools for inter-version compatibility.  However, these tools are not
enough and Valkey adds an additional versioning machinery on top of the protobuf
mechanisms.  This versioning mechanism is intended to provide the maximum amount
of forward and backward compatibility between code releases. A key goal of the
versioning mechanism is to allow higher Valkey releases to be compatible with
lower Valkey releases when possible.

Protobufs allow the addition of new fields that will be silently ignored by
older code versions.  What protobuf cannot do on it's own is to decide whether
the behavior of old code that ignores this new field is acceptable. Valkey
solves this problem by adding a versioning system on top of the entire metadata
system.

The Valkey versioning system dynamically computes a minimum version for each
metadata object, establishing the range of code releases (from the minimum
version to the current version) that are compatible with the metadata object.
The expectation is that each client object will examine it's current state and
indicate the minimum version associated with that. For example, DataBase numbers
in cluster mode weren't supported until Release 1.1
(MetadataVersion::kVersion2). The 1.1 code stamps the protobuf for each index as
either kVersion1 or kVersion2 depending on whether cluster mode is enabled and
the data base number is non-zero. Thus 1.0 compatible objects are written
from 1.1 code when new features are not used, enabling forward compatibility.

This per-object minimum version number is used in two different contexts.

1. Metadata messages sent over the wire have a minimum version number. This
version number is computed as the maximum of the version numbers of all metadata
objects currently defined. If the receiving code base has a version number less
than the minimum version then the entire Metadata message will be dropped.
(There's a counter and log message for this scenario, so it's detectable). This
scenario persists as long as there are incompatible versions within the cluster,
i.e., it can be corrected by updating the laggard nodes OR by deleting/modifying
the metadata object(s) that is responsible for the higher version number.

2. Just as with metadata messages on the wire, the Search module's portion of
the auxdata section of an RDB file also contains a minimum version number (NOT
the same as the Valkey encver). This is semantically the same as the minimum
version number of the metadata messages on the wire. However, due to legacy
issues, the RDB version number is encoded as a vmsdk::SemanticVersion starting
at 1.0.0 for the 1.0 release which has MetadataVersion::kVersion1(1). A simple
map converts MetadataVersion to SemanticVersion, the reverse mapping is not
needed.

*/

//
// These constants define the different versions of ValkeySearch metadata
// encoding. Generally, they correspond to releases of ValkeySearch, but minor
// releases may not increment the version if no metadata changes were made.
//

//
// Initial release 1.0
//
constexpr vmsdk::ValkeyVersion kRelease10(1, 0, 0);

//
// Release 1.0, added support for cluster mode with non-zero DB numbers
//
constexpr vmsdk::ValkeyVersion kRelease11(1, 1, 0);

}  // namespace valkey_search

#endif
