#pragma once

#include <vector>
#include <memory>

#include "table/block_based/block_builder.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class PilotBlockBuilder {
    public:
        PilotBlockBuilder(const PilotBlockBuilder&) = delete;
        PilotBlockBuilder& operator=(const PilotBlockBuilder&) = delete;

        PilotBlockBuilder();

        void MarkUp();
        void MarkDown();

        Slice Finish();

    private:
        void advance();
        std::unique_ptr<BlockBuilder> pilot_block_;
        std::vector<uint8_t> pilot_;
        uint32_t length;
        uint8_t buff;
};

} // namespace rocksdb
