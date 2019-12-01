#pragma once

#include "table/block_based/seek_table_reader.h"
#include "table/block_based/seek_block.h"

namespace rocksdb
{

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
        inline void Next() override;

        void Prev() override {}
        void SeekForPrev(const Slice& target) override {}

        Slice key() const override;
        Slice value() const override;

        Status status() const override;

    private:
        std::vector<SeekTableIterator*> iters_;
        const Comparator& comp_;
        std::vector<uint8_t> levels_;
        size_t current_;
        SeekTableIterator* current_iter_;
};

} // namespace namerocksdb

