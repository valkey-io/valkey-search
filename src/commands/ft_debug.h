/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#pragma once

namespace valkey_search {

/// Get the current state of fanout force remote fail debug setting
/// @return true if fanout should force remote failures, false otherwise
bool GetFanoutForceRemoteFail();

}  // namespace valkey_search
