#include "table/block_based/seek_block_builder.h"

#include "util/coding.h"

namespace rocksdb
{

SeekBlockBuilder::SeekBlockBuilder()
    : restarts_(),
    num_entries(0),
    finished_(false) {

    restarts_.push_back(0);
    // one for restart array length
    // the other for restart 0 entry
    estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
}

size_t SeekBlockBuilder::EstimateSizeAfterKV(const Slice& key, const Slice& value) const {
    size_t estimate = CurrentSizeEstimate();

    estimate += key.size();
    estimate += value.size();

    estimate += sizeof(uint32_t); // restart entry

    estimate += VarintLength(key.size());
    estimate += VarintLength(value.size());

    return estimate;
}

void SeekBlockBuilder::Add(const Slice& key, const Slice& value) {
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    estimate_ += sizeof(uint32_t); // for one restart entry

    size_t curr_size = buffer_.size();

    PutVarint32Varint32(&buffer_, static_cast<uint32_t>(key.size()),
                        static_cast<uint32_t>(value.size()));

    buffer_.append(key.data(), key.size());

    buffer_.append(value.data(), value.size());
    num_entries++;
    estimate_ += buffer_.size() - curr_size;
}

void SeekBlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    estimate_ = sizeof(uint32_t) + sizeof(uint32_t); // why there are two??
    num_entries = 0;
    finished_ = false;
}

Slice SeekBlockBuilder::Finish() {
    for (size_t i = 0; i < restarts_.size(); i++) {
        PutFixed32(&buffer_, restarts_[i]);
    }
    
    // num_restarts == num_entries
    // uint32_t num_restarts = static_cast<uint32_t>(restarts_.size());
    PutFixed32(&buffer_, num_entries);
    finished_ = true;
    return Slice(buffer_);
}

} // namespace namerocksdb

