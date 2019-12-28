#pragma once

#include "include/rocksdb/status.h"
#include "include/rocksdb/slice.h"
#include "util/coding.h"
#include <vector>

namespace rocksdb
{
struct PilotValue {
    // vector element must keep its order
    PilotValue(std::vector<uint16_t>& index_block,
                std::vector<uint16_t>& data_block,
                std::vector<uint8_t>& levels)
        : index_block_(index_block),
        data_block_(data_block),
        levels_(nullptr),
        levels_size_(0),
        pined(true) {
            assert(index_block_.size() == data_block_.size());
            levels_size_ = levels.size();
            levels_ = new uint8_t[levels_size_];
            for (size_t i = 0; i < levels.size(); i++) {
                levels_[i] = levels[i];
            }
    }

    PilotValue(std::vector<uint16_t>& index_block,
                std::vector<uint16_t>& data_block,
                uint8_t* levels, uint32_t n)
        : index_block_(index_block),
        data_block_(data_block),
        levels_(levels),
        levels_size_(n),
        pined(false) {}

    ~PilotValue() {
        if (levels_ != nullptr && pined) {
            delete[] levels_;
        }
    }

    PilotValue()
        : index_block_(),
        data_block_(),
        levels_(nullptr),
        levels_size_(0),
        pined(true) {}

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    std::vector<uint16_t> index_block_;
    std::vector<uint16_t> data_block_;
    // std::vector<uint8_t> levels_;
    uint8_t* levels_;
    uint32_t levels_size_;
    bool pined;
};
} // namespace namerrocksdb

