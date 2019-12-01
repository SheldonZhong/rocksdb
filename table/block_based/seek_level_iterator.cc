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

    index_level->Seek(target);
    PilotValue pilot;
    index_level->FollowAndGetPilot(&pilot);
    size_t n = pilot.index_block_.size();
    assert(n == pilot.data_block_.size());
    assert(n == (iters_.size() - 1));
    for (size_t i = 1; i < n; i++) {
        size_t pilot_idx = i - 1;
        iters_[i]->index_iter_->SeekToRestartPoint(pilot.index_block_[pilot_idx]);
        bool ok = iters_[i]->index_iter_->ParseNextDataKey(nullptr);
        assert(ok);
        iters_[i]->block_iter_.SeekToRestartPoint(pilot.data_block_[pilot_idx]);
        ok = iters_[i]->block_iter_.ParseNextDataKey(nullptr);
        assert(ok);
    }

    current_iter_ = iters_[0];
    current_ = 0;
    levels_ = std::move(pilot.levels_);
    // should be seek for previous
    while (comp_.Compare(key(), target) < 0) {
        Next();
    }
    // guarantee that scan to first key > target
}

void SeekLevelIterator::Next() {
    current_iter_->Next();
    current_++;
    assert(current_ < levels_.size());
    size_t iter_index = static_cast<size_t>(levels_[current_]);
    assert(iter_index < iters_.size());
    current_iter_ = iters_[iter_index];
}

void SeekLevelIterator::SeekToFirst() {
    current_iter_ = iters_[0];
    current_iter_->pilot_iter_->SeekToFirst();
    for (auto iter : iters_) {
        iter->SeekToFirst();
    }
    PilotValue pilot;
    current_iter_->GetPilot(&pilot);
    assert(pilot.data_block_.size() == 
            pilot.index_block_.size());
    assert(pilot.data_block_.size() == 0);

    current_ = 0;
    levels_ = std::move(pilot.levels_);
    current_iter_ = iters_[levels_[0]];
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

    return current_iter_->Valid();
}

Status SeekLevelIterator::status() const {
    return Status::OK();
}

} // namespace namerocksdb

