#pragma once

#include "table/block_based/index_builder.h"
#include "table/block_based/seek_block_builder.h"

namespace rocksdb
{

class SeekIndexBuilder {
    public:

    struct IndexBlocks {
        Slice index_block_contents;
        std::unordered_map<std::string, Slice> meta_blocks;
    };

        explicit SeekIndexBuilder(const Comparator* comparator)
            : comparator_(comparator) {}

        void AddIndexEntry(std::string* last_key_in_current_block,
                            const Slice* first_key_index_next_block,
                            const BlockHandle& block_handle);

        Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle);

        inline Status Finish(IndexBlocks* index_blocks) {
            BlockHandle last_partition_block_handle;
            return Finish(index_blocks, last_partition_block_handle);
        }
        
        void OnKeyAdded(const Slice& key);

        size_t IndexSize() const { return index_size_; }

    private:
        const Comparator* comparator_;
        SeekBlockBuilder index_block_builder_;
        std::string current_block_first_internal_key_;
        size_t index_size_ = 0;
};

} // namespace namerocksdb

