// Wenshao Zhong (wzhong20@uic.edu)

// Include the header file for the class you want to test
#include "table/block_based/disc_bit_block_index.h"

#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

std::string GenerateInternalKey(int primary_key, int secondary_key,
                                int padding_size, Random *rnd,
                                size_t ts_sz = 0) {
  char buf[50];
  char *p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += rnd->RandomString(padding_size);
  }
  AppendInternalKeyFooter(&k, 0 /* seqno */, kTypeValue);
  std::string key_with_ts;
  if (ts_sz > 0) {
    PadInternalKeyWithMinTimestamp(&key_with_ts, k, ts_sz);
    return key_with_ts;
  }

  return k;
}

// Generate random key value pairs.
// The generated key will be sorted. You can tune the parameters to generated
// different kinds of test key/value pairs for different scenario.
void GenerateRandomKVs(std::vector<std::string> *keys,
                       std::vector<std::string> *values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1, size_t ts_sz = 0) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      // `DataBlockIter` assumes it reads only internal keys.
      keys->emplace_back(GenerateInternalKey(i, j, padding_size, &rnd, ts_sz));

      // 100 bytes values
      values->emplace_back(rnd.RandomString(100));
    }
  }
}

// Define a test fixture
class DiscBitBlockIndexTest : public ::testing::Test {
 protected:
  // Set up the test fixture
  void SetUp() override {
    // Perform any necessary setup steps before each test
  }

  // Tear down the test fixture
  void TearDown() override {
    // Perform any necessary cleanup steps after each test
  }

  // Define any helper functions or member variables that you need for your
  // tests
};

// Define your test cases
TEST_F(DiscBitBlockIndexTest, TestName1) {
  // Arrange: Set up any necessary preconditions for the test

  // Act: Perform the operation you want to test

  // Assert: Check the expected results
  EXPECT_TRUE(true);  // Replace with your actual assertions
}

TEST_F(DiscBitBlockIndexTest, TestName2) {
  // Arrange: Set up any necessary preconditions for the test

  // Act: Perform the operation you want to test

  // Assert: Check the expected results
  EXPECT_TRUE(true);  // Replace with your actual assertions
}

TEST(DiscBitBlockIndex, PointQuery) {
  DiscBitBlockIndexBuilder builder;
  builder.Initialize();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  int num_keys = 100;
  GenerateRandomKVs(&keys, &values, 0, num_keys, 1, 0, 1);

  for (int i = 0; i < num_keys; i++) {
    builder.Add(Slice(keys[i]));
  }

  std::string buffer;
  builder.Finish(buffer);

  Slice data(buffer);
  DiscBitBlockIndex index;
  index.Initialize(data.data(), data.size(), num_keys);

  for (int i = 0; i < num_keys; i++) {
    size_t pos = index.Lookup(Slice(keys[i]));
    ASSERT_EQ(i, pos);
  }
}

TEST(DiscBitBlockIndex, RangeQuery) {
  DiscBitBlockIndexBuilder builder;
  builder.Initialize();

  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<std::string> inserted_keys;

  int num_keys = 100;
  GenerateRandomKVs(&keys, &values, 0, 3 * num_keys + 1, 1, 0, 1);

  for (int i = 0; i < num_keys; i++) {
    builder.Add(Slice(keys[3 * i]));
    inserted_keys.emplace_back(keys[3 * i]);
  }

  builder.Add(Slice(keys[3 * num_keys]));
  inserted_keys.emplace_back(keys[3 * num_keys]);

  std::string buffer;
  builder.Finish(buffer);

  Slice data(buffer);
  DiscBitBlockIndex index;
  index.Initialize(data.data(), data.size(), inserted_keys.size());

  const Comparator *icmp = BytewiseComparator();

  for (int i = 0; i < num_keys; i++) {
    uint64_t pkey = index.SliceExtract(Slice(keys[3 * i]));
    size_t pos = index.PartialKeyLookup(pkey);

    ASSERT_EQ(i, pos);

    // non-existing keys
    Slice query_key1(keys[3 * i + 1]);
    pkey = index.SliceExtract(query_key1);
    pos = index.PartialKeyLookup(pkey);

    // key access
    Slice probe_key1(inserted_keys[pos]);

    int cmp = icmp->Compare(probe_key1, query_key1);
    ASSERT_NE(cmp, 0);

    pos = index.FinishSeek(query_key1, probe_key1, pos, -cmp);
    ASSERT_EQ(pos, i + 1);

    Slice query_key2(keys[3 * i + 2]);
    pkey = index.SliceExtract(query_key2);
    pos = index.PartialKeyLookup(pkey);

    // key access
    Slice probe_key2(inserted_keys[pos]);

    cmp = icmp->Compare(probe_key2, query_key2);
    ASSERT_NE(cmp, 0);

    pos = index.FinishSeek(query_key2, probe_key2, pos, -cmp);
    ASSERT_EQ(pos, i + 1);
  }
}

// Add more test cases as needed
}  // namespace ROCKSDB_NAMESPACE

// Run the tests
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}