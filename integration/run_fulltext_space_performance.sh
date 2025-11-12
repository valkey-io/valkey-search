#!/bin/bash

# Script to run large-scale full-text search tests
# These tests create large volumes of documents to test indexing scalability
#
# This script uses the existing test framework infrastructure from run.sh

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Large-Scale Full-Text Search Tests${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Default: run all tests
TEST_PATTERN=""

# Parse command line arguments
if [ $# -eq 0 ]; then
    echo -e "${GREEN}Running all space performance tests...${NC}"
    TEST_PATTERN="test_fulltext_space_performance"
elif [ "$1" == "1" ]; then
    echo -e "${GREEN}Running Test 1: Single document with 1 million tokens...${NC}"
    TEST_PATTERN="test_single_document_million_tokens"
elif [ "$1" == "2" ]; then
    echo -e "${GREEN}Running Test 2: 1 million documents with single token...${NC}"
    TEST_PATTERN="test_million_documents_single_token"
elif [ "$1" == "3" ]; then
    echo -e "${GREEN}Running Test 3: 1 million documents with unique tokens...${NC}"
    TEST_PATTERN="test_million_documents_unique_tokens"
else
    echo -e "${RED}Error: Invalid argument${NC}"
    echo ""
    echo "Usage: $0 [test_number]"
    echo ""
    echo "  test_number: Optional parameter to run specific test"
    echo "    1 - Single document with 1 million 'b' tokens"
    echo "    2 - 1 million documents with single 'b' token (multi-client)"
    echo "    3 - 1 million documents with unique tokens (multi-client)"
    echo ""
    echo "  If no argument is provided, all tests will run"
    exit 1
fi

echo ""
echo -e "${YELLOW}Test Information:${NC}"
echo "  Test 1: Creates 1 document with 1 million 'b' tokens"
echo "  Test 2: Creates 1 million documents, each with single 'b' token (uses 10 concurrent clients)"
echo "  Test 3: Creates 1 million documents, each with unique token like 'a', 'b', 'aa', etc. (uses 10 concurrent clients)"
echo ""
echo -e "${YELLOW}Note: These tests may take several minutes to complete${NC}"
echo ""

# Run the tests using the existing test framework
echo -e "${GREEN}Starting test execution using run.sh framework...${NC}"
echo ""

cd "$SCRIPT_DIR"
export TEST_PATTERN="$TEST_PATTERN"
./run.sh

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}Some tests failed!${NC}"
    echo -e "${RED}========================================${NC}"
    exit $EXIT_CODE
fi
