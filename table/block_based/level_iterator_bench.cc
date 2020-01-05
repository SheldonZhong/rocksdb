#include "table/block_based/seek_level_iterator.h"
#include "table/block_based/seek_table_builder.h"
#include "table/block_based/pilot_block_mars.h"
#include "table/merging_iterator.h"
#include "include/rocksdb/env.h"

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

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
  }
  return Slice(*dst);
}

static std::string RandomString(Random *rnd, int len) {
    std::string r;
    RandomString(rnd, len, &r);
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

    EnvOptions env_options_;
    Env* env_;

    Benchmark(int _num_records = 100000, int _layers = 5, int _rnd = 1234, std::string _path = "./")
    : rnd(_rnd),
    num_records(_num_records),
    cmp(BytewiseComparator()),
    layers(_layers),
    readers(new SeekTable*[layers]),
    iters(new SeekTableIterator*[layers]),
    timer(base_config),
    path(_path),
    env_(Env::Default())
    {
        GenerateRandomKVs(&keys, &values, 0, num_records);
        file_reader.resize(layers);
        file_writer.resize(layers);
        env_->OptimizeForManifestWrite(env_options_);
        for (int i = 0; i < layers; i++) {
            char buf[50];
            sprintf(buf, "/tmp/bench/table%d", i);
            std::unique_ptr<WritableFile> file;
            NewWritableFile(env_, buf, &file, env_options_);
            file_writer[i].reset(new WritableFileWriter(
                std::move(file), buf, env_options_, env_, nullptr));
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
            delete iters[i];
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
        std::unique_ptr<RandomAccessFile> file;
        std::string name = file_writer[idx]->file_name();
        env_options_.use_mmap_reads = true;
        env_->NewRandomAccessFile(name,
            &file, env_options_);
        file_reader[idx].reset(
            new RandomAccessFileReader(std::move(file), name, env_));
    }

    size_t size(int idx) {
        if (idx < 0 || idx >= layers) {
            return 0;
        }
        return file_writer[idx]->GetFileSize();
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
                    if (cmprslt != 0) {
                        std::cout << "panic!" << std::endl;
                    }
                    cmprslt = v.ToString().compare(values[next_index]);
                    if (cmprslt != 0) {
                        std::cout << "panic!" << std::endl;
                    }
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
struct MarsBench : public Benchmark {
    MarsBench()
    : Benchmark() {}

    MarsBench(int _num, int _layers, int _rnd)
    : Benchmark(_num, _layers, _rnd) {}

    void Prepare() override {
        std::cout << "Pilot block Mars benchmark" << std::endl;
        base_config["bench"] = "seek-mars";
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
        std::unique_ptr<WritableFile> file;
        char buf[50];
        sprintf(buf, "/tmp/bench/pilot");
        NewWritableFile(env_, buf, &file, env_options_);
        pilot_file.reset(new WritableFileWriter(
            std::move(file), buf, env_options_, env_, nullptr));
        PilotBlockMarsBuilder pilot_builder(*cmp, readers, layers, &counts, pilot_file.get());
        pilot_builder.Build();
        Status s = pilot_builder.Finish();
        assert(s.ok());
        pilot_file->Flush();

        std::unique_ptr<RandomAccessFile> read_file;
        env_options_.use_mmap_reads = true;
        env_->NewRandomAccessFile(buf,
            &read_file, env_options_);
        pilot_reader.reset(
            new RandomAccessFileReader(std::move(read_file), buf, env_));
        
        SeekTable::Open(*cmp, std::move(pilot_reader),
            pilot_file->GetFileSize(),
            &pilot_table, 0);
        std::unique_ptr<PilotBlockMarsIterator> level_iter;
        level_iter.reset(new PilotBlockMarsIterator(pilot_table.get(), &counts, iters, cmp));
    }

    InternalIterator* GetIter() override {
        return new PilotBlockMarsIterator(pilot_table.get(), &counts, iters, cmp);
    }

    std::unique_ptr<WritableFileWriter> pilot_file;
    std::unique_ptr<RandomAccessFileReader> pilot_reader;
    std::unique_ptr<SeekTable> pilot_table;
    std::vector<uint16_t*> counts;
};

}; // namespace namerocksdb


int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << argv[0] << " [m/p/r] [num_records] [layers] [rnd] [path]" << std::endl;
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
    case 'r':
        bench = new rocksdb::MarsBench(num_records, layers, rnd);
        break;
    default:
        std::cout << argv[0] << " [m/p/r]" << std::endl;
        exit(1);
        break;
    }

    bench->Prepare();
    bench->Finish();
    bench->Run();
}
