#pragma once

#include <map>

#include "table/format.h"
#include "table/block_based/seek_block_builder.h"
#include "util/kv_map.h"

namespace rocksdb
{
class SeekMetaIndexBuilder {
    public:
        SeekMetaIndexBuilder(const SeekMetaIndexBuilder&) = delete;
        SeekMetaIndexBuilder& operator=(const SeekMetaIndexBuilder&) = delete;

        SeekMetaIndexBuilder();
        void Add(const std::string& key, const BlockHandle& handle);

        Slice Finish();

    private:
        stl_wrappers::KVMap meta_block_handles_;
        std::unique_ptr<SeekBlockBuilder> meta_index_block_;
};
} // namespace namerockrocksdb

