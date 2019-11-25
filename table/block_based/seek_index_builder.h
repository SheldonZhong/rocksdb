#pragma once

#include "table/block_based/index_builder.h"
#include "table/block_based/seek_block_builder.h"

namespace rocksdb
{

class SeekIndexBuilder : public IndexBuilder {
    public:
        explicit SeekIndexBuilder(const InternalKeyComparator* comparator);
        void AddIndexEntry(std::string* last_key_in_current_block,
                            const Slice* first_key_index_next_block,
                            const BlockHandle& block_handle) override;

        Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle) override;
        
        void OnKeyAdded(const Slice& key) override;

        size_t IndexSize() const override { return index_size_; }

    private:
        SeekBlockBuilder index_block_builder_;
        std::string current_block_first_internal_key_;
};

} // namespace namerocksdb

