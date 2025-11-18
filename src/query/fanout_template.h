#pragma once

#include <netinet/in.h>

#include <functional>
#include <ostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/util.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query::fanout {

// Template class for fanout operations across cluster nodes
class FanoutTemplate {
 public:
  template <typename RequestT, typename ResponseT, typename TrackerT>
  void PerformRemoteRequest(
      std::unique_ptr<RequestT> request, const std::string& address,
      coordinator::ClientPool* coordinator_client_pool,
      std::shared_ptr<TrackerT> tracker,
      std::function<void(coordinator::Client*, std::unique_ptr<RequestT>,
                         std::function<void(grpc::Status, ResponseT&)>, int)>
          grpc_invoker,
      std::function<void(const grpc::Status&, ResponseT&,
                         std::shared_ptr<TrackerT>, const std::string&)>
          callback_logic,
      int timeout_ms = -1) {
    auto client = coordinator_client_pool->GetClient(address);

    grpc_invoker(
        client, std::move(request),
        [tracker, address, callback_logic](grpc::Status status,
                                           ResponseT& response) mutable {
          callback_logic(status, response, tracker, address);
        },
        timeout_ms);
  }
};

}  // namespace valkey_search::query::fanout
