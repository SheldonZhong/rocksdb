#include "table/block_based/seek_level_iterator.h"

namespace rocksdb
{

SeekLevelIterator::SeekLevelIterator(
                                SeekTableIterator** iters,
                                int n,
                                const Comparator& comp)
                                : comp_(comp) {
    assert(iters != nullptr && n > 0);
    for (int i = 0; i < n; i++) {
        iters_.push_back(iters[i]);
    }
}

void SeekLevelIterator::Sync(int i) {
    SeekTableIterator* iter = iters_[i + 1];
    if (pilot_.index_block_[i] & 0x8000) {
        iter->index_iter_->SeekToLast();
    } else {
        iter->index_iter_->SeekToRestartPoint(
                            static_cast<uint32_t>(pilot_.index_block_[i]));
        bool ok = iter->index_iter_->ParseNextDataKey();
        assert(ok);
    }
    iter->InitDataBlock();
    if (pilot_.data_block_[i] & 0x8000) {
        iter->block_iter_.SeekToLast();
    } else {
        iter->block_iter_.SeekToRestartPoint(
                            static_cast<uint32_t>(pilot_.data_block_[i]));
        bool ok = iter->block_iter_.ParseNextDataKey();
        assert(ok);
    }
}

void SeekLevelIterator::Seek(const Slice& target) {
    SeekTableIterator* index_level = iters_[0];
    // TODO: add extra check to decide whether re-seek
    index_level->SeekForPrev(target);
    if (index_level->Valid()) {
        index_level->FollowAndGetPilot(&pilot_);
    } else {
        index_level->GetFirstPilot(&pilot_);
    }
    size_t n = pilot_.index_block_.size();
    assert(n == pilot_.data_block_.size());
    for (size_t i = 0; i < n; i++) {
        Sync(i);
    }

    if (n == 0) {
        // seek the key before first key in top level
        if (pilot_.levels_.size() > 0) {
            for (auto iter : iters_) {
                iter->SeekToFirst();
            }
            current_iter_ = iters_[pilot_.levels_[0] + 1];
        } else {
            current_iter_ = iters_[0];
        }
    } else {
        current_iter_ = iters_[0];
    }

    current_ = 0;
    // should be seek for previous
    if (pilot_.levels_.size() > kBinarySeekThreshold) {
        if (comp_.Compare(key(), target) < 0) {
            uint32_t i;
            bool s = BinarySeek(target, 0, pilot_.levels_.size(), &i, &comp_, n == 0);
            assert(s);
            current_ = i;
            current_iter_ = iters_[pilot_.levels_[i] + 1];
        }
    } else {
        while (comp_.Compare(key(), target) < 0) {
            Next();
        }
    }
    // guarantee that scan to first key > target
}

// tries to access key at idx
// lazily count idx-th key
// the key is occur_[idx]-th key in count_[level]
// where level = pilot_.levels_[idx]
void SeekLevelIterator::lazyCount(uint32_t idx) {
    if (idx >= occur_.size()) {
        // scan more
        for (size_t i = occur_.size(); i <= idx; i++) {
            uint8_t lvl = pilot_.levels_[i];
            if (count_.count(lvl) == 0) {
                count_[lvl] = 0;
            } else {
                count_[lvl]++;
            }
            occur_.push_back(count_[lvl]);
        }
    }
}

void SeekLevelIterator::pushCursor(uint32_t left, bool first) {
    count_.erase(pilot_.levels_[left]);
    if (!first) {
        iters_[0]->Next();
    }

    for (uint32_t i = left + 1; i < occur_.size(); i++) {
        uint8_t lvl = pilot_.levels_[i];
        if (count_[lvl] == 0) {
            count_.erase(lvl);
        } else {
            count_[lvl]--;
        }
    }
    // may overflow
    for (uint32_t i = left; i != 0; i--) {
        uint8_t lvl = pilot_.levels_[i];
        if (count_.count(lvl) != 0) {
            count_.erase(lvl);
            iters_[lvl + 1]->Next(occur_[i] + 1);
        }
    }

    if (!count_.empty() || left == 0) {
        for (auto cc : count_) {
            iters_[cc.first + 1]->Next();
        }
    }
}

bool SeekLevelIterator::BinarySeek(const Slice& target, uint32_t left,
                                    uint32_t right, uint32_t* index, 
                                    const Comparator* comp, bool first) {
    // prepare occurrence array
    // this could be moved out and cache if we don't need to re-seek
    if (pilot_.levels_.size() == 0) {
        *index = 0;
        return false;
    }

    SeekTableIterator* iter = nullptr;
    assert(left <= right);
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        lazyCount(mid);
        size_t kth = occur_[mid];
        uint8_t i = pilot_.levels_[mid];
        iter = iters_[i + 1];
        // save iterator states
        const uint32_t index_state = iter->GetIndexBlock();
        const uint32_t data_state = iter->GetDataBlock();
        iter->Next(kth);
        bool restore_index = index_state != iter->GetIndexBlock();
        // calculate this is k-th keys in this iter
        // where k is the number of occurrence in levels_
        Slice mid_key = iter->key();
        int cmp = comp->Compare(mid_key, target);
        if (cmp < 0) {
            left = mid + 1;
        } else if (cmp > 0) {
            right = mid;
        } else {
            left = right = mid;
            break;
        }
        
        if (restore_index) {
            // restore iterator states
            iter->index_iter_->SeekToRestartPoint(index_state);
            iter->index_iter_->ParseNextDataKey();
            iter->InitDataBlock();
        }
        // restoring states might have high overhead
        iter->block_iter_.SeekToRestartPoint(data_state);
        iter->block_iter_.ParseNextDataKey();
    }
    *index = left;

    assert(iter != nullptr);
    pushCursor(left, first);
    // TODO: could be cached
    count_.clear();
    occur_.clear();
    return true;
}

void SeekLevelIterator::SeekToFirst() {
    current_iter_ = iters_[0];
    current_iter_->pilot_iter_->SeekToFirst();
    for (auto iter : iters_) {
        iter->SeekToFirst();
    }
    current_iter_->GetPilot(&pilot_);
    current_iter_->pilot_iter_->Next();
    assert(pilot_.data_block_.size() == 
            pilot_.index_block_.size());
    assert(pilot_.data_block_.size() == 0);

    current_ = 0;
    if (!pilot_.levels_.empty()) {
        current_iter_ = iters_[pilot_.levels_[0] + 1];
    } else {
        current_iter_->GetPilot(&pilot_);
    }
}

Slice SeekLevelIterator::key() const {
    assert(Valid());
    return current_iter_->key();
}

Slice SeekLevelIterator::value() const {
    assert(Valid());
    return current_iter_->value();
}

bool SeekLevelIterator::Valid() const {
    if (current_iter_ == nullptr) {
        return false;
    }
    if (current_iter_->pilot_iter_ != nullptr &&
        !current_iter_->pilot_iter_->Valid()) {
        return false;
    }
    return current_iter_->Valid();
}

Status SeekLevelIterator::status() const {
    return Status::OK();
}

} // namespace namerocksdb

