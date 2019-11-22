#include "table/block_based/seek_block.h"

namespace rocksdb
{

SeekBlock::SeekBlock(BlockContents&& contents)
        : contents_(std::move(contents)),
        data_(contents_.data.data()),
        size_(contents_.data.size()),
        restart_offset_(0),
        num_entries_(0) {
    if (size_ < sizeof(uint32_t)) {
        size_ = 0;
    } else {
        num_entries_ = NumEntries();
        restart_offset_ = static_cast<uint32_t>(size_) -
                            (1 + num_entries_) * sizeof(uint32_t);
    }
}

uint32_t SeekBlock::NumEntries() const {
    assert(size_ >= 2 * sizeof(uint32_t));
    uint32_t num_entries = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    return num_entries;
}

SeekDataBlockIter* SeekBlock::NewDataIterator(const Comparator* cmp, SeekDataBlockIter* iter) {
    SeekDataBlockIter* ret_iter;
    if (iter != nullptr){
        ret_iter = iter;
    } else {
        ret_iter = new SeekDataBlockIter;
    }
    if (size_ < 2 * sizeof(uint32_t)) {
        // do not contain a restart entry and length
        ret_iter->Invalidate(Status::Corruption("bad block contents"));
    }
    if (num_entries_ == 0) {
        // empty block
        ret_iter->Invalidate(Status::OK());
        return ret_iter;
    } else {
        ret_iter->Initialize(cmp, data_, restart_offset_, num_entries_);
    }

    return ret_iter;
}

static inline const char* DecodeEntry(const char* p, const char* limit,
                                        uint32_t* key_length,
                                        uint32_t* value_length) {
    assert(limit - p >= 2);
    // fast path for varint
    // 2 bytes 
    if ((p = GetVarint32Ptr(p, limit, key_length)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;

    // enough room for the actual key and value
    assert(!(static_cast<uint32_t>(limit - p) < (*key_length + *value_length)));
    return p;
}

void SeekDataBlockIter::CorruptionError() {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.Clear();
    value_.clear();
}

bool SeekDataBlockIter::ParseNextDataKey(const char* limit) {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    if (!limit) {
        // if limit is not specified
        // the limit goes all the way to the end
        limit = data_ + restarts_; //
    }

    if (p >= limit) {
        // no more entries, mark as invalid
        current_ = restarts_;
        restart_index_ = num_restarts_;
        return false;
    }
    
    uint32_t key_length, value_length;
    p = DecodeEntry(p, limit, &key_length, &value_length);

    if (p == nullptr) {
        CorruptionError();
        return false;
    }

    key_.SetKey(Slice(p, key_length), true /* copy */);
    value_ = Slice(p + key_length, value_length);

    restart_index_++;

    return true;
}

void SeekDataBlockIter::SeekToFirst() {
    if (data_ == nullptr) {
        return;
    }
    SeekToRestartPoint(0);
    ParseNextDataKey();
}

void SeekDataBlockIter::SeekToLast() {
    if (data_ == nullptr) {
        return;
    }
    SeekToRestartPoint(num_restarts_ - 1);
}

void SeekDataBlockIter::Next() {
    assert(Valid());
    ParseNextDataKey();
}

bool SeekDataBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                uint32_t right, uint32_t* index,
                                const Comparator* comp) {
    assert(left <= right);
    while (left < right) {
        uint32_t mid = (left + right + 1) / 2;
        // so left and right should be the sequence nubmer
        // of the restart points
        uint32_t region_offset = GetRestartPoint(mid);
        uint32_t key_length, value_length;
        const char* key_ptr = DecodeEntry(
            data_ + region_offset, data_ + restarts_, &key_length, &value_length);
        if (key_ptr == nullptr) {
            CorruptionError();
            return false;
        }
        Slice mid_key(key_ptr, key_length);
        int cmp = comp->Compare(mid_key, target);
        if (cmp < 0) {
            // Key at "mid" is smaller than "target". Therefore all
            // blocks before "mid" are uninteresting.
            left = mid;
        } else if (cmp > 0) {
            // Key at "mid" is >= "target". Therefore all blocks at or
            // after "mid" are uninteresting.
            right = mid - 1;
        } else {
            left = right = mid;
        }
    }
    *index = left;
    return true;
}

void SeekDataBlockIter::Seek(const Slice& target) {
    if (data_ == nullptr) {
        return;
    }
    uint32_t index = 0;
    bool ok = BinarySeek(target, 0, num_restarts_ - 1, &index, comparator_);
    if (!ok) {
        return;
    }
    SeekToRestartPoint(index);
    assert(Compare(key_, target));
    ParseNextDataKey();
}

} // namespace namerorocksdb

