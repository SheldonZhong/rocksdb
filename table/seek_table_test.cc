#include <iostream>
#include "table/block_based/seek_table_builder.h"
#include "table/block_based/seek_table_reader.h"

#include "test_util/testutil.h"
#include "test_util/testharness.h"

namespace rocksdb
{

static std::string RandomString(Random *rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random *rnd) {
  char buf[50];
  char *p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += RandomString(rnd, padding_size);
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
                       const int keys_share_prefix = 1) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      keys->emplace_back(GenerateKey(i, j, padding_size, &rnd));

      // 100 bytes values
      values->emplace_back(RandomString(&rnd, 100));
    }
  }
}

class TableTest : public testing::Test {};

TEST_F(TableTest, SimpleTest) {
    Random rnd(301);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    int num_records = 100000;

    GenerateRandomKVs(&keys, &values, 0, num_records);

    std::unique_ptr<WritableFileWriter> file_writer_;
    std::unique_ptr<RandomAccessFileReader> file_reader_;
    const Comparator* cmp = BytewiseComparator();

    file_writer_.reset(test::GetWritableFileWriter(new test::StringSink(), ""));
    SeekTableBuilder builder(*cmp, file_writer_.get());

    for (int i = 0; i < num_records; i++) {
        builder.Add(keys[i], values[i]);
    }
    Status s = builder.Finish();
    ASSERT_TRUE(s.ok()) << s.ToString();
    file_writer_->Flush();
    EXPECT_EQ(static_cast<test::StringSink*>(file_writer_->writable_file())->contents().size(), 
        builder.FileSize());
    
    file_reader_.reset(test::GetRandomAccessFileReader(new test::StringSource(
      static_cast<test::StringSink*>(file_writer_->writable_file())->contents(),
      1, false)));
    
    std::unique_ptr<SeekTable> reader;

    SeekTable::Open(*cmp, std::move(file_reader_),
      static_cast<test::StringSink*>(file_writer_->writable_file())->contents().size(),
      &reader, 0);
    InternalIterator* iter = reader->NewIterator();

    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); count++, iter->Next()) {
      Slice k = iter->key();
      Slice v = iter->value();

      ASSERT_EQ(k.ToString().compare(keys[count]), 0);
      ASSERT_EQ(v.ToString().compare(values[count]), 0);
    }
    ASSERT_EQ(num_records, count);

    iter->SeekToFirst();

    for (int i = 0; i < num_records; i++) {
      // find a random key in the lookaside array
      int index = rnd.Uniform(num_records);
      Slice k(keys[index]);
      std::string expected = values[index];

      // search in block for this key
      iter->Seek(k);
      ASSERT_TRUE(iter->Valid());
      Slice v = iter->value();
      Slice key = iter->key();
      std::cout << key.ToString() << std::endl;
      ASSERT_EQ(v.ToString().compare(values[index]), 0);
    }
    delete iter;
}

} // namespace namerocksdb


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
