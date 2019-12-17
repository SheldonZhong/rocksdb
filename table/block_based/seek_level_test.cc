#include "test_util/testutil.h"
#include "test_util/testharness.h"

#include "table/block_based/seek_level_iterator.h"
#include "table/block_based/seek_table_builder.h"
#include "table/block_based/seek_table_reader.h"

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

class LevelTest : public testing::Test {};

TEST_F(LevelTest, SimpleTest) {
    Random rnd(444);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    int num_records = 100000;
    GenerateRandomKVs(&keys, &values, 0, num_records);

    std::vector<std::unique_ptr<WritableFileWriter>> file_writer_;
    std::vector<std::unique_ptr<RandomAccessFileReader>> file_reader_;
    const Comparator* cmp = BytewiseComparator();
    
    int layers = 5;
    file_writer_.resize(layers);
    file_reader_.resize(layers);

    SeekTable** readers = new SeekTable*[layers];

    SeekTableIterator** iter_list = new SeekTableIterator*[layers];

    for (int i_layer = 0; i_layer < layers; i_layer++) {
        file_writer_[i_layer].reset(test::GetWritableFileWriter(new test::StringSink(), ""));
        std::unique_ptr<WritableFileWriter>& file = file_writer_[i_layer];

        SeekTableBuilder* builder;
        if (i_layer == layers - 1) {
            builder = new SeekTableBuilder(*cmp, file.get(), readers+1, layers - 1);
        } else {
            builder = new SeekTableBuilder(*cmp, file.get());
        }
        for (int i = i_layer; i < num_records; i += layers) {
            builder->Add(keys[i], values[i]);
        }
        Status s = builder->Finish();
        ASSERT_TRUE(s.ok()) << s.ToString();
        file->Flush();
        EXPECT_EQ(static_cast<test::StringSink*>(file->writable_file())->contents().size(), 
            builder->FileSize());

        file_reader_[i_layer].reset(test::GetRandomAccessFileReader(new test::StringSource(
            static_cast<test::StringSink*>(file->writable_file())->contents(),
            1, false)));
        
        std::unique_ptr<RandomAccessFileReader>& file_reader = file_reader_[i_layer];
        std::unique_ptr<SeekTable> reader;
        SeekTable::Open(*cmp, std::move(file_reader),
            static_cast<test::StringSink*>(file->writable_file())->contents().size(),
            &reader, 0);
        
        readers[layers - i_layer - 1] = reader.release();
        iter_list[layers - i_layer - 1] = readers[layers - i_layer - 1]->NewSeekTableIter();

        delete builder;
    }

    std::unique_ptr<SeekLevelIterator> level_iter;
    level_iter.reset(new SeekLevelIterator(iter_list, layers, *cmp));

    int count = 0;
    for (level_iter->SeekToFirst(); level_iter->Valid(); count++, level_iter->Next()) {
        Slice k = level_iter->key();
        Slice v = level_iter->value();

        ASSERT_EQ(k.ToString().compare(keys[count]), 0);
        ASSERT_EQ(v.ToString().compare(values[count]), 0);
    }
    ASSERT_EQ(num_records, count);

    for (int i = 0; i < num_records; i++) {
        int index = rnd.Uniform(num_records);
        // int index = i;
        Slice k(keys[index]);

        level_iter->Seek(k);
        Slice seek_key = level_iter->key();
        int cmpresult = seek_key.ToString().compare(keys[index]);
        ASSERT_EQ(seek_key.ToString().compare(keys[index]), 0);
        ASSERT_TRUE(level_iter->Valid());
        Slice v = level_iter->value();
        ASSERT_EQ(v.ToString().compare(values[index]), 0);
    }

    delete[] iter_list;
    delete[] readers;
}

TEST_F(LevelTest, RandomInsertTest) {
    Random rnd(1113);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    int num_records = 100000;
    GenerateRandomKVs(&keys, &values, 0, num_records);

    int layers = 5;
    std::vector<int> idx;
    for (int i = 0; i < num_records; i++) {
        idx.push_back(rnd.Uniform(layers));
    }

    
    std::vector<std::unique_ptr<WritableFileWriter>> file_writer_;
    std::vector<std::unique_ptr<RandomAccessFileReader>> file_reader_;
    const Comparator* cmp = BytewiseComparator();
    
    file_writer_.resize(layers);
    file_reader_.resize(layers);

    SeekTable** readers = new SeekTable*[layers];

    SeekTableIterator** iter_list = new SeekTableIterator*[layers];

    for (int i_layer = 0; i_layer < layers; i_layer++) {
        file_writer_[i_layer].reset(test::GetWritableFileWriter(new test::StringSink(), ""));
        std::unique_ptr<WritableFileWriter>& file = file_writer_[i_layer];

        SeekTableBuilder* builder;

        if (i_layer == layers - 1) {
            builder = new SeekTableBuilder(*cmp, file.get(), readers + 1, layers - 1);
        } else {
            builder = new SeekTableBuilder(*cmp, file.get());
        }

        for (int i = 0; i < num_records; i++) {
            if (idx[i] == i_layer) {
                builder->Add(keys[i], values[i]);
            }
        }

        Status s = builder->Finish();
        ASSERT_TRUE(s.ok()) << s.ToString();
        file->Flush();
        EXPECT_EQ(static_cast<test::StringSink*>(file->writable_file())->contents().size(), 
            builder->FileSize());

        file_reader_[i_layer].reset(test::GetRandomAccessFileReader(new test::StringSource(
            static_cast<test::StringSink*>(file->writable_file())->contents(),
            1, false)));
        
        std::unique_ptr<RandomAccessFileReader>& file_reader = file_reader_[i_layer];
        std::unique_ptr<SeekTable> reader;
        SeekTable::Open(*cmp, std::move(file_reader),
            static_cast<test::StringSink*>(file->writable_file())->contents().size(),
            &reader, 0);
        
        readers[layers - i_layer - 1] = reader.release();
        iter_list[layers - i_layer - 1] = readers[layers - i_layer - 1]->NewSeekTableIter();

        delete builder;
    }

    std::unique_ptr<SeekLevelIterator> level_iter;
    level_iter.reset(new SeekLevelIterator(iter_list, layers, *cmp));

    int count = 0;
    for (level_iter->SeekToFirst(); level_iter->Valid(); count++, level_iter->Next()) {
        Slice k = level_iter->key();
        Slice v = level_iter->value();

        int cmprslt = k.ToString().compare(keys[count]);
        ASSERT_EQ(k.ToString().compare(keys[count]), 0);
        ASSERT_EQ(v.ToString().compare(values[count]), 0);
    }
    ASSERT_EQ(num_records, count);

    for (int i = 0; i < num_records; i++) {
        int index = rnd.Uniform(num_records);
        // int index = i;
        Slice k(keys[index]);

        level_iter->Seek(k);
        int next_index = index;
        int end = (index + 100) < num_records ? (index+100) : num_records;
        do {
            Slice seek_key = level_iter->key();
            int cmpresult = seek_key.ToString().compare(keys[next_index]);
            ASSERT_EQ(seek_key.ToString().compare(keys[next_index]), 0) 
                    << "occur at " << i << " index " << index << " next_index "
                    << next_index << std::endl;;
            ASSERT_TRUE(level_iter->Valid());
            Slice v = level_iter->value();
            ASSERT_EQ(v.ToString().compare(values[next_index]), 0);
            next_index++;
            level_iter->Next();
        } while(next_index < end);
    }

    delete[] iter_list;
    delete[] readers;
}

} // namespace namerocksdb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
