#pragma once

#include "table/block_based/seek_table_reader.h"
#include "table/block_based/seek_block.h"

namespace rocksdb
{

const size_t kBinarySeekThreshold = 8;

class SeekLevelIterator : public InternalIterator {
    public:
        // keep the elements in order
        explicit SeekLevelIterator(SeekTableIterator** iters,
                                    int n,
                                    const Comparator& comp);

        bool Valid() const;
        void SeekToFirst() override;
        void SeekToLast() override {}
        void Seek(const Slice& target) override;
        inline void Next() override {
            current_iter_->Next();
            if (current_iter_ != iters_[0]) {
                current_++;
            }
            if (current_ >= pilot_.levels_.size()) {
                current_iter_ = iters_[0];
                current_ = 0;
                // follow and get has extra seek in pilot_iter
                // there should be a function directly advanced the pointer
                if (current_iter_->pilot_iter_ != nullptr &&
                    current_iter_->pilot_iter_->Valid()) {

                    current_iter_->GetPilot(&pilot_);
                }
                
                size_t n = pilot_.data_block_.size();
                assert(n == pilot_.index_block_.size());

                return;
            }
            size_t iter_index = static_cast<size_t>(pilot_.levels_[current_] + 1);
            assert(iter_index < iters_.size());
            current_iter_ = iters_[iter_index];
        }

        inline bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                                uint32_t* index, const Comparator* cmp, bool first = false);

        void Prev() override {}
        void SeekForPrev(const Slice& target) override {}

        Slice key() const override;
        Slice value() const override;

        Status status() const override;

    private:
        inline void Sync(int i);
        inline void lazyCount(uint32_t i);
        inline void pushCursor(uint32_t left, bool first = false);
        std::vector<SeekTableIterator*> iters_;
        const Comparator& comp_;

        // these could be cached
        PilotValue pilot_;
        std::vector<size_t> occur_;
        std::map<uint8_t, size_t> count_;

        size_t current_;
        SeekTableIterator* current_iter_;
};

} // namespace namerocksdb

