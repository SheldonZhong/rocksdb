#pragma once

#include "table/block_based/pilot_block.h"

namespace rocksdb
{
class PilotBlockMarsBuilder {
    public:
        PilotBlockMarsBuilder(const Comparator& comp,
                        SeekTable** levels, int n,
                        std::vector<uint16_t*>* counts);
        
        void Build();

        Slice Finish();
    
    private:
        const Comparator& comparator_;
        SeekTable** levels_;
        int num_levels_;
        std::unique_ptr<SeekBlockBuilder> pilot_block_;
        std::vector<uint16_t*>* counts_;
};

class PilotBlockMarsIterator : public InternalIterator {
    public:
        PilotBlockMarsIterator(
            BlockContents contents,
            std::vector<uint16_t*>* counts,
            SeekTableIterator** iters,
            const Comparator* comp);
        void SeekToFirst() override;
        void SeekToLast() override {}
        void Seek(const Slice& target) override;
        void Next() override;
        void Prev() override {}
        void SeekForPrev(const Slice& target) override {}
        Slice key() const override;
        Slice value() const override;
        Status status() const override;

        void ParsePilot() {
            assert(pilot_iter_->Valid());
            Slice v = pilot_iter_->value();
            Status s = entry_.DecodeFrom(&v);
            assert(s.ok());
        }

        bool Valid() const;
    
    private:
        SeekBlock pilot_block_;
        SeekDataBlockIter* pilot_iter_;
        std::vector<uint16_t*>* counts_;
        SeekTableIterator** iters_;
        size_t num_levels_;
        size_t current_;
        SeekTableIterator* current_iter_;
        PilotValue entry_;
        const Comparator* comp_;
};

} // namespace namerocksdb

