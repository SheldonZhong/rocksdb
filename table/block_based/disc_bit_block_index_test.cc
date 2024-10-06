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

void GenerateRandomSharedPrefixKeys(std::vector<std::string> *keys,
                                    const int num_keys) {
  Random rnd(404);

  int generated_keys = 0;
  while (generated_keys < num_keys) {
    int prefix = generated_keys;
    uint32_t num_shared_prefix = rnd.Uniform(10);
    for (uint32_t i = 0; i < num_shared_prefix; i++) {
      uint32_t var_len = rnd.Uniform(20);
      std::string key = GenerateInternalKey(prefix, i, var_len, &rnd);
      keys->emplace_back(key);
      generated_keys++;

      if (rnd.OneIn(2)) {
        key.append(rnd.RandomString(5));
        keys->emplace_back(key);
        generated_keys++;
      }
    }
  }
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
  size_t index_size = index.Initialize(data.data(), data.size(), num_keys);
  ASSERT_EQ(index_size, data.size());

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

TEST(DiscBitBlockIndex, PrefixKeys) {
  DiscBitBlockIndexBuilder builder;
  builder.Initialize();

  std::vector<std::string> keys;
  std::vector<std::string> inserted_keys;

  int num_keys = 300;
  GenerateRandomSharedPrefixKeys(&keys, num_keys);

  Random rnd(251);

  for (int i = 0; i < num_keys; i++) {
    if (rnd.OneIn(2)) {
      builder.Add(Slice(keys[i]));
      inserted_keys.emplace_back(keys[i]);
    }
  }

  const size_t nr_inserted = inserted_keys.size();

  std::string buffer;
  builder.Finish(buffer);

  Slice data(buffer);
  DiscBitBlockIndex index;
  index.Initialize(data.data(), data.size(), nr_inserted);

  const Comparator *icmp = BytewiseComparator();

  for (int i = 0; i < num_keys; i++) {
    Slice query_key(keys[i]);
    uint64_t pkey = index.SliceExtract(query_key);
    size_t pos = index.PartialKeyLookup(pkey);

    // key access
    Slice probe_key(inserted_keys[pos]);

    int cmp = icmp->Compare(probe_key, query_key);
    if (cmp == 0) {
      continue;
    }

    pos = index.FinishSeek(query_key, probe_key, pos, -cmp);

    ASSERT_TRUE(pos <= nr_inserted);

    if (pos == nr_inserted) {
      ASSERT_TRUE(icmp->Compare(query_key, Slice(inserted_keys[nr_inserted-1])) > 0);
      continue;
    }

    ASSERT_TRUE(icmp->Compare(query_key, Slice(inserted_keys[pos])) <= 0);
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