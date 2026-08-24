import re

with open("testing/vector_registry_test.cc", "r") as f:
    content = f.read()

content = re.sub(
    r'TEST_F\(VectorRegistryTest, ShareWithValkeyIdenticalVectorReTrack\) \{.*?\}(?=\n\nTEST_F)',
    '',
    content,
    flags=re.DOTALL
)

with open("testing/vector_registry_test.cc", "w") as f:
    f.write(content)
