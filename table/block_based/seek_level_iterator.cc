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

void SeekLevelIterator::Seek(const Slice& target) {
    SeekTableIterator* index_level = iters_[0];
    // TODO: add extra check to decide whether re-seek
    index_level->SeekForPrev(target);
    PilotValue pilot;
    if (index_level->Valid()) {
        index_level->FollowAndGetPilot(&pilot);
    } else {
        index_level->GetFirstPilot(&pilot);
    }
    size_t n = pilot.index_block_.size();
    assert(n == pilot.data_block_.size());
    for (size_t i = 0; i < n; i++) {
        SeekTableIterator* iter = iters_[i + 1];
        if (pilot.index_block_[i] & 0x8000) {
            iter->index_iter_->SeekToLast();
        } else {
            iter->index_iter_->SeekToRestartPoint(
                                static_cast<uint32_t>(pilot.index_block_[i]));
            bool ok = iter->index_iter_->ParseNextDataKey();
            assert(ok);
        }
        iter->InitDataBlock();
        if (pilot.data_block_[i] & 0x8000) {
            iter->block_iter_.SeekToLast();
        } else {
            iter->block_iter_.SeekToRestartPoint(
                                static_cast<uint32_t>(pilot.data_block_[i]));
            bool ok = iter->block_iter_.ParseNextDataKey();
            assert(ok);
        }
    }

    if (n == 0) {
        // seek the key before first key in top level
        if (pilot.levels_.size() > 0) {
            for (auto iter : iters_) {
                iter->SeekToFirst();
            }
            current_iter_ = iters_[pilot.levels_[0] + 1];
        } else {
            current_iter_ = iters_[0];
        }
    } else {
        current_iter_ = iters_[0];
    }

    current_ = 0;
    levels_ = pilot.levels_;
    // should be seek for previous
    // while (comp_.Compare(key(), target) < 0) {
    //     Next();
    // }
    if (comp_.Compare(key(), target) < 0) {
        uint32_t i;
        bool s = BinarySeek(target, 0, levels_.size(), &i, &comp_, n == 0);
        assert(s);
        current_ = i;
        current_iter_ = iters_[levels_[i] + 1];
    }
    // guarantee that scan to first key > target
}

bool SeekLevelIterator::BinarySeek(const Slice& target, uint32_t left,
                                    uint32_t right, uint32_t* index, 
                                    const Comparator* comp, bool first) {
    // prepare occurrence array
    // this could be moved out and cache if we don't need to re-seek
    if (levels_.size() == 0) {
        *index = 0;
        return false;
    }
    std::vector<size_t> occur(levels_.size());
    std::map<uint8_t, size_t> count;

    for (size_t i = 0; i < levels_.size(); i++) {
        uint8_t lvl = levels_[i];
        if (count.count(lvl) == 0) {
            count[lvl] = 0;
        } else {
            count[lvl]++;
        }
        occur[i] = count[lvl];
    }

    SeekTableIterator* iter = nullptr;
    assert(left <= right);
    while (left < right) {
        uint32_t mid = (left + right) / 2;

        uint8_t i = levels_[mid];
        size_t kth = occur[mid];
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
    // synchronize iterators
    count.erase(levels_[left]);
    if (!first) {
        iters_[0]->Next();
    }
    for (uint32_t i = left; i < levels_.size() && !count.empty(); i++) {
        if (count.count(levels_[i]) != 0) {
            count.erase(levels_[i]);
            iters_[levels_[i] + 1]->Next(occur[i]);
        }
    }
    if (!count.empty()) {
        for (auto cc : count) {
            iters_[cc.first + 1]->Next(cc.second + 1);
        }
    }
    return true;
}

void SeekLevelIterator::SeekToFirst() {
    current_iter_ = iters_[0];
    current_iter_->pilot_iter_->SeekToFirst();
    for (auto iter : iters_) {
        iter->SeekToFirst();
    }
    PilotValue pilot;
    current_iter_->GetPilot(&pilot);
    current_iter_->pilot_iter_->Next();
    assert(pilot.data_block_.size() == 
            pilot.index_block_.size());
    assert(pilot.data_block_.size() == 0);

    current_ = 0;
    levels_ = pilot.levels_;
    if (!levels_.empty()) {
        current_iter_ = iters_[levels_[0] + 1];
    } else {
        current_iter_->GetPilot(&pilot);
        levels_ = pilot.levels_;
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

