#include "table/seek_meta_blocks.h"

namespace rocksdb
{
SeekMetaIndexBuilder::SeekMetaIndexBuilder()
    : meta_index_block_(new SeekBlockBuilder()) {}

void SeekMetaIndexBuilder::Add(const std::string& key,
                                const BlockHandle& handle) {
    std::string handle_encoding;
    handle.EncodeTo(&handle_encoding);
    meta_block_handles_.insert({key, handle_encoding});
}

Slice SeekMetaIndexBuilder::Finish() {
    for (const auto& metablock : meta_block_handles_) {
        meta_index_block_->Add(metablock.first, metablock.second);
    }
    return meta_index_block_->Finish();
}

} // namespace namerocksdb

