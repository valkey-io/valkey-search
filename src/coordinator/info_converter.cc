#include "src/coordinator/info_converter.h"

namespace valkey_search::coordinator {

std::unique_ptr<InfoIndexPartitionRequest> CreateInfoIndexPartitionRequest(const std::string& index_name) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name(index_name);
  return request;
}

}  // namespace valkey_search::coordinator
