#include "src/coordinator/info_converter.h"
#include <gtest/gtest.h>

namespace valkey_search::coordinator {

TEST(InfoConverterTest, CreateInfoIndexRequest) {
  std::string index_name = "test_index";
  
  auto request = CreateInfoIndexPartitionRequest(index_name);
  
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->index_name(), "test_index");
}

}  // namespace valkey_search::coordinator
