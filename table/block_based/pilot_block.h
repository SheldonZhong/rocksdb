#pragma once

#include <vector>
#include <memory>

#include "table/block_based/seek_block_builder.h"
#include "include/rocksdb/status.h"

namespace rocksdb {

struct PilotValue {
    // vector element must keep its order
    PilotValue(std::vector<uint32_t>& index_block,
                std::vector<uint32_t>& data_block,
                std::vector<uint8_t>& levels)
        : index_block_(index_block),
        data_block_(data_block),
        levels_(levels) {
            assert(index_block_.size() == data_block_.size());
    }

    PilotValue()
        : index_block_(),
        data_block_(),
        levels_() {}

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    std::vector<uint32_t> index_block_;
    std::vector<uint32_t> data_block_;
    std::vector<uint8_t> levels_;
};

class PilotBlockBuilder {
    public:
        PilotBlockBuilder(const PilotBlockBuilder&) = delete;
        PilotBlockBuilder& operator=(const PilotBlockBuilder&) = delete;

        PilotBlockBuilder();

        void AddPilotEntry(const Slice& key,
                            std::vector<uint32_t>& index_block,
                            std::vector<uint32_t>& data_block,
                            std::vector<uint8_t>& levels);

        void AddFirstEntry(std::vector<uint8_t>& levels);

        Slice Finish();

        bool empty() const { return pilot_block_->empty(); }

    private:
        std::unique_ptr<SeekBlockBuilder> pilot_block_;
};

class PilotBlock {

};

} // namespace rocksdb
