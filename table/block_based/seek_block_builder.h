#pragma once

#include <vector>
#include "rocksdb/slice.h"

namespace rocksdb
{

class SeekBlockBuilder {
    public:
        SeekBlockBuilder(const SeekBlockBuilder&) = delete;
        void operator=(const SeekBlockBuilder&) = delete;

        explicit SeekBlockBuilder();

        void Reset();

        void Add(const Slice& key, const Slice& value);

        Slice Finish();

        inline size_t CurrentSizeEstimate() const { return estimate_; }

        size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

        bool empty() const { return buffer_.empty(); }

    private:
        std::string buffer_;
        std::vector<uint32_t> restarts_;
        size_t estimate_;
        uint32_t num_entries;
        bool finished_;
};

} // namespace namer rocksdb

