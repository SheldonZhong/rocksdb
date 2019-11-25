#include "table/block_based/seek_index_builder.h"

namespace rocksdb
{

Status SeekIndexBuilder::Finish(IndexBlocks* index_blocks,
                                const BlockHandle& /* last_partition_block_handle */) {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
}

void SeekIndexBuilder::OnKeyAdded(const Slice& key) {
    if(current_block_first_internal_key_.empty()) {
        current_block_first_internal_key_.assign(key.data(), key.size());
    }
}

void SeekIndexBuilder::AddIndexEntry(std::string* last_key_in_current_block,
                                    const Slice* first_key_in_next_block,
                                    const BlockHandle& block_handle) {
    Slice sep = Slice(*last_key_in_current_block);
    IndexValue entry(block_handle, current_block_first_internal_key_);
    std::string encoded_entry;
    entry.EncodeTo(&encoded_entry, false, nullptr);
    index_block_builder_.Add(sep, encoded_entry);
    current_block_first_internal_key_.clear();
}

} // namespace namerocksdb
