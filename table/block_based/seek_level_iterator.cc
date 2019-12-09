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
        if (pilot.index_block_[i] & 0x70000000) {
            iter->index_iter_->SeekToLast();
        } else {
            iter->index_iter_->SeekToRestartPoint(pilot.index_block_[i]);
            bool ok = iter->index_iter_->ParseNextDataKey();
            assert(ok);
        }
        iter->InitDataBlock();
        if (pilot.data_block_[i] & 0x70000000) {
            iter->block_iter_.SeekToLast();
        } else {
            iter->block_iter_.SeekToRestartPoint(pilot.data_block_[i]);
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
    levels_ = std::move(pilot.levels_);
    // should be seek for previous
    while (comp_.Compare(key(), target) < 0) {
        Next();
    }
    // guarantee that scan to first key > target
}

// inline in .cc never define
// void SeekLevelIterator::Next() {
// }

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
    levels_ = std::move(pilot.levels_);
    if (!levels_.empty()) {
        current_iter_ = iters_[levels_[0] + 1];
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

