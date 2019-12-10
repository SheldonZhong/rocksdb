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

void SeekDataBlockIter::CorruptionError() {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.clear();
    value_.clear();
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
    ParseNextDataKey();
}

void SeekDataBlockIter::Next() {
    assert(Valid());
    ParseNextDataKey();
}

void SeekDataBlockIter::SeekForPrev(const Slice& target) {
    if (data_ == nullptr) {
        return;
    }

    uint32_t index = 0;
    bool ok = BinarySeek(target, 0, num_restarts_, &index, comparator_);
    if (!ok) {
        return;
    }
    if (index >= num_restarts_) {
        SeekToLast();
        return;
    }

    SeekToRestartPoint(index);
    ParseNextDataKey();
    if (Compare(key_, target) > 0) {
        Prev();
    }
}

void SeekDataBlockIter::Prev() {
    assert(Valid());
    const uint32_t original = current_;
    while (GetRestartPoint(restart_index_) >= original) {
        if (restart_index_ == 0) {
            // no more entries to scan
            current_ = restarts_;
            restart_index_ = num_restarts_;
            return;
        }
        restart_index_--;
    }
    SeekToRestartPoint(restart_index_);
    do {
        // linear scan, only loops when there are more than 1 keys in restart interval
        if (!ParseNextDataKey()) {
            break;
        }
    } while (NextEntryOffset() < original);
}

bool SeekDataBlockIter::BinarySeek(const Slice& target, uint32_t left,
                                uint32_t right, uint32_t* index,
                                const Comparator* comp) {
    assert(left <= right);
    while (left < right) {
        uint32_t mid = (left + right) / 2;
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
            left = mid + 1;
        } else if (cmp > 0) {
            // Key at "mid" is >= "target". Therefore all blocks at or
            // after "mid" are uninteresting.
            right = mid;
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
    bool ok = BinarySeek(target, 0, num_restarts_, &index, comparator_);
    if (!ok) {
        return;
    }
    // out of bound
    if (index >= num_restarts_) {
        // mark invalid
        current_ = restarts_;
        restart_index_ = num_restarts_;
        key_.clear();
        value_.clear();
        return;
    }
    SeekToRestartPoint(index);
    ParseNextDataKey();
}

} // namespace namerorocksdb

