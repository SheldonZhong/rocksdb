#include "table/block_based/seek_level_iterator.h"
#include "table/block_based/seek_table_builder.h"
#include "table/merging_iterator.h"
#include "include/rocksdb/env.h"
#include "test_util/testutil.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <sstream>

namespace rocksdb
{

using json = nlohmann::json;
using nano_seconds = std::chrono::duration<double, std::ratio<1, 1000000000>>;
using micro_seconds = std::chrono::duration<double, std::ratio<1, 1000000>>;
using milli_seconds = std::chrono::duration<double, std::ratio<1, 1000>>;
using seconds = std::chrono::duration<double, std::ratio<1, 1>>;

static std::string RandomString(Random *rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
}
std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random *rnd) {
    char buf[50];
    char *p = &buf[0];
    snprintf(buf, sizeof(buf), "%6d%4d%s", primary_key, secondary_key,
                RandomString(rnd, rnd->Uniform(10) + 1).c_str());
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
        values->emplace_back(RandomString(&rnd, rnd.Uniform(100) + 20));
        }
    }
}

struct Timer {
    json& base_config;
    Timer(json& config)
    : base_config(config) {}

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point stop;

    void Tik() {
        start = std::chrono::high_resolution_clock::now();
    }

    void Tok() {
        stop = std::chrono::high_resolution_clock::now();
    }
    void Report() {
        nano_seconds elapsed = stop - start;

        std::cout.imbue(std::locale(""));
        if (elapsed < micro_seconds(1))
            std::cout << "time spent " << nano_seconds(elapsed).count() << " ns" << std::endl;
        else if (elapsed < milli_seconds(1))
            std::cout << "time spent " << micro_seconds(elapsed).count() << " \u03BCs" << std::endl;
        else if (elapsed < seconds(1))
            std::cout << "time spent " << milli_seconds(elapsed).count() << " ms" << std::endl;
        else
            std::cout << "time spent " << seconds(elapsed).count() << " s" << std::endl;
        
        json t_json;
        t_json["config"] = base_config;
        t_json["nano_sec"] = elapsed.count();
    }

    void Report(uint64_t nops) {
        Report();
        nano_seconds elapsed = stop - start;
        std::cout << nops << " operations" << std::endl;
        double IOPS = nops / elapsed.count() * nano_seconds(seconds(1)).count();
        std::cout << "OPS: " << IOPS << std::endl;

        base_config["nano_sec"] = elapsed.count();
        base_config["operations"] = nops;
        base_config["OPS"] = IOPS;
    }
};

struct Benchmark {
    Random rnd;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    int num_records;

    const Comparator* cmp;

    std::vector<std::unique_ptr<WritableFileWriter>> file_writer;
    std::vector<std::unique_ptr<RandomAccessFileReader>> file_reader;

    int layers;

    SeekTable** readers;
    SeekTableIterator** iters;

    std::vector<int> sequences;

    json base_config;
    Timer timer;

    std::string path;

    Benchmark(int _num_records = 100000, int _layers = 5, int _rnd = 1234, std::string _path = "./")
    : rnd(_rnd),
    num_records(_num_records),
    cmp(BytewiseComparator()),
    layers(_layers),
    readers(new SeekTable*[layers]),
    iters(new SeekTableIterator*[layers]),
    timer(base_config),
    path(_path)
    {
        GenerateRandomKVs(&keys, &values, 0, num_records);
        file_reader.resize(layers);
        file_writer.resize(layers);
        for (int i = 0; i < layers; i++) {
            file_writer[i].reset(test::GetWritableFileWriter(new test::StringSink(), ""));
        }
        for (int i = 0; i < num_records; i++) {
            sequences.push_back(rnd.Uniform(layers));
        }
        std::cout.imbue(std::locale(""));
        std::cout << "-----------------------------------------------------" << std::endl;
        std::cout << "number of records: " << num_records << std::endl;
        std::cout << "number of layers: " << layers << std::endl;
        std::cout << "random seed: " << _rnd << std::endl << std::endl;

        base_config["num_records"] = num_records;
        base_config["num_layers"] = layers;
        base_config["rnd_seed"] = _rnd;
    }

    virtual ~Benchmark() {
        delete cmp;
        for (int i = 0; i < layers; i++) {
            delete readers[i];
            delete iters;
        }
        delete[] readers;
        delete[] iters;
    }

    virtual void Prepare() = 0;
    virtual void Finish() = 0;
    virtual InternalIterator* GetIter() = 0;

    void Flush(int idx) {
        if (idx < 0 || idx >= layers) {
            return;
        }
        file_writer[idx]->Flush();
        file_reader[idx].reset(test::GetRandomAccessFileReader(new test::StringSource(
            static_cast<test::StringSink*>(file_writer[idx]->writable_file())->contents(),
            1, false)));
    }

    size_t size(int idx) {
        if (idx < 0 || idx >= layers) {
            return 0;
        }
        return static_cast<test::StringSink*>(file_writer[idx]->writable_file())->contents().size();
    }

    void Run() {
        InternalIterator* iter = GetIter();
        std::cout << "SeekToFirst next until the end" << std::endl;
        timer.Tik();
        int count = 0;
        for (iter->SeekToFirst(); iter->Valid(); count++, iter->Next()) {
            Slice k = iter->key();
            Slice v = iter->value();
            int cmprslt = k.ToString().compare(keys[count]);
            assert(cmprslt == 0);
            cmprslt = v.ToString().compare(values[count]);
            assert(cmprslt == 0);
        }
        assert(count == num_records);
        timer.Tok();
        base_config["type"] = "next";
        timer.Report(count);
        DumpJSON();
        
        for (int nexts = 0; nexts < 50; nexts += 10) {
            count = 0;
            std::cout << std::endl << "Random seek with " << nexts << " next" << std::endl;
            timer.Tik();
            for (int i = 0; i < num_records; i++) {
                int index = rnd.Uniform(num_records);
                
                iter->Seek(keys[index]);
                int next_index = index;
                int end = (index + nexts) < num_records ? (index + nexts) : num_records;
                do {
                    Slice k = iter->key();
                    Slice v = iter->value();
                    count++;
                    int cmprslt = k.ToString().compare(keys[next_index]);
                    assert(cmprslt == 0);
                    cmprslt = v.ToString().compare(values[next_index]);
                    assert(cmprslt == 0);
                    next_index++;
                    iter->Next();
                } while (next_index < end);
            }
            timer.Tok();
            base_config["type"] = "seek-next";
            base_config["num_nexts"] = nexts;
            timer.Report(count);
            DumpJSON();
        }
    }

    void DumpJSON() {
        std::string fname = GetCurrentTimeForFileName();
        std::ofstream out(path + fname + ".json");
        out << std::setw(4) << base_config << std::endl;
        out.close();
    }

    std::string GetCurrentTimeForFileName() {
        auto time = std::time(nullptr);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time), "%F_%T"); // ISO 8601 without timezone information.
        auto s = ss.str();
        std::replace(s.begin(), s.end(), ':', '_');
        return s;
    }
};

struct MergingBench : public Benchmark {
    MergingBench()
    : Benchmark() {}

    MergingBench(int _num, int _layers, int _rnd)
    : Benchmark(_num, _layers, _rnd) {}

    void Prepare() override {
        std::cout << "Merging seek benchmark" << std::endl;
        base_config["bench"] = "merging";
        for (int i = 0; i < layers; i++) {
            SeekTableBuilder* builder = new SeekTableBuilder(*cmp, file_writer[i].get());
            for (int idx = 0; idx < num_records; idx++) {
                if (sequences[idx] == i) {
                    builder->Add(keys[idx], values[idx]);
                }
            }
            Status s = builder->Finish();
            assert(s.ok());
            Flush(i);
            std::unique_ptr<RandomAccessFileReader>& file_reader_ = file_reader[i];
            std::unique_ptr<SeekTable> reader;
            SeekTable::Open(*cmp, std::move(file_reader_),
                            size(i), &reader, 0);
            readers[i] = reader.release();
            iters[i] = readers[i]->NewSeekTableIter();
            delete builder;
        }
    }
    void Finish() override {
    }

    InternalIterator* GetIter() override {
        InternalIterator** iiters = new InternalIterator*[layers];
        for (int i = 0; i < layers; i++) {
            iiters[i] = iters[i];
        }
        return NewMergingIterator(cmp, iiters, layers);
    }
};

struct SeekBench : public Benchmark {
    SeekBench()
    : Benchmark() {}

    SeekBench(int _num, int _layers, int _rnd)
    : Benchmark(_num, _layers, _rnd) {}

    void Prepare() override {
        std::cout << "Pilot block seek benchmark" << std::endl;
        base_config["bench"] = "seek";
        for (int i = 1; i < layers; i++) {
            SeekTableBuilder* builder = new SeekTableBuilder(*cmp, file_writer[i].get());
            for (int idx = 0; idx < num_records; idx++) {
                if (sequences[idx] == i) {
                    builder->Add(keys[idx], values[idx]);
                }
            }
            Status s = builder->Finish();
            assert(s.ok());
            Flush(i);
            std::unique_ptr<RandomAccessFileReader>& file_reader_ = file_reader[i];
            std::unique_ptr<SeekTable> reader;
            SeekTable::Open(*cmp, std::move(file_reader_),
                            size(i), &reader, 0);
            readers[i] = reader.release();
            iters[i] = readers[i]->NewSeekTableIter();
            delete builder;
        }
    }

    void Finish() override {
        SeekTableBuilder* builder = new SeekTableBuilder(*cmp, file_writer[0].get(),
                                                        readers + 1, layers - 1);
        for (int i = 0; i < num_records; i++) {
            if (sequences[i] == 0) {
                builder->Add(keys[i], values[i]);
            }
        }
        Status s = builder->Finish();
        assert(s.ok());
        Flush(0);
        std::unique_ptr<RandomAccessFileReader>& file_reader_ = file_reader[0];
        std::unique_ptr<SeekTable> reader;
        SeekTable::Open(*cmp, std::move(file_reader_),
                        size(0), &reader, 0);
        readers[0] = reader.release();
        iters[0] = readers[0]->NewSeekTableIter();
        delete builder;
    }

    InternalIterator* GetIter() override {
        return new SeekLevelIterator(iters, layers, *cmp);
    }
};

}; // namespace namerocksdb


int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << argv[0] << " [m/p] [num_records] [layers] [rnd] [path]" << std::endl;
        std::cout << "m for merging iterator" << std::endl;
        std::cout << "p for pilot guided iterator" << std::endl;
        exit(1);
    }
    int num_records = 100000;
    int layers = 5;
    int rnd = 4444;
    if (argc > 2) {
        num_records = std::atoi(argv[2]);
    }
    if (argc > 3) {
        layers = std::atoi(argv[3]);
    }
    if (argc > 4) {
        rnd = std::atoi(argv[4]);
    }

    
    std::string path = "./";
    if (argc > 5) {
        path.assign(argv[5]);
    }

    rocksdb::Benchmark* bench;
    switch (*argv[1])
    {
    case 'm':
        bench = new rocksdb::MergingBench(num_records, layers, rnd);
        break;
    case 'p':
        bench = new rocksdb::SeekBench(num_records, layers, rnd);
        break;
    default:
        std::cout << argv[0] << " [m/p]" << std::endl;
        exit(1);
        break;
    }

    bench->Prepare();
    bench->Finish();
    bench->Run();
}
