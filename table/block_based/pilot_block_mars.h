#pragma once

#include "table/block_based/pilot_block.h"
#include "table/block_based/seek_table_builder.h"
#include "table/block_based/seek_table_reader.h"

namespace rocksdb
{
class PilotBlockMarsBuilder {
    public:
        PilotBlockMarsBuilder(const Comparator& comp,
                        SeekTable** levels, int n,
                        std::vector<uint16_t*>* counts,
                        WritableFileWriter* file);
        
        void Build();

        Status Finish();

        uint64_t FileSize() const { return pilot_block_->FileSize(); }
    
    private:
        const Comparator& comparator_;
        SeekTable** levels_;
        int num_levels_;
        std::unique_ptr<SeekTableBuilder> pilot_block_;
        std::vector<uint16_t*>* counts_;
};

class PilotBlockMarsIterator : public InternalIterator {
    public:
        PilotBlockMarsIterator(
            SeekTable* contents,
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
        SeekTable* pilot_block_;
        SeekTableIterator* pilot_iter_;
        std::vector<uint16_t*>* counts_;
        SeekTableIterator** iters_;
        size_t num_levels_;
        size_t current_;
        SeekTableIterator* current_iter_;
        PilotValue entry_;
        const Comparator* comp_;
        std::vector<uint16_t> index_left_;
        std::vector<uint16_t> data_left_;
        std::vector<uint16_t> index_right_;
        std::vector<uint16_t> data_right_;
};

} // namespace namerocksdb

