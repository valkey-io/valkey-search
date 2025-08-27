#include "src/coordinator/info_converter.h"

namespace valkey_search::coordinator {

std::unique_ptr<InfoIndexPartitionRequest> CreateInfoIndexPartitionRequest(
    const std::string& index_name, uint64_t timeout_ms) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name(index_name);
  request->set_timeout_ms(timeout_ms);
  return request;
}

}  // namespace valkey_search::coordinator
