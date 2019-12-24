#pragma once

#include <vector>
#include <memory>

#include "table/block_based/seek_table_reader.h"
#include "table/block_based/seek_block_builder.h"
#include "table/block_based/pilot_value.h"
#include "include/rocksdb/status.h"
#include "util/heap.h"

namespace rocksdb {

struct IterComparator {
    IterComparator(const Comparator& _comparator)
        : comparator(_comparator) {}

    // implement greater than
    inline bool operator()(SeekTableIterator* a,
                            SeekTableIterator* b) const {
        return comparator.Compare(a->key(), b->key()) > 0;
    }

    const Comparator& comparator;
};

typedef BinaryHeap<SeekTableIterator*, IterComparator> minHeap;

class PilotBlockBuilder {
    public:
        PilotBlockBuilder(const PilotBlockBuilder&) = delete;
        PilotBlockBuilder& operator=(const PilotBlockBuilder&) = delete;

        PilotBlockBuilder(const Comparator& comp,
                        SeekTable** levels, int n);

        void AddPilotEntry(const Slice& key,
                            std::vector<uint16_t>& index_block,
                            std::vector<uint16_t>& data_block,
                            std::vector<uint8_t>& levels);

        void AddFirstEntry(std::vector<uint8_t>& levels);

        void BuildPilot(const Slice* key = nullptr);

        Slice Finish();

        bool empty() const { return pilot_block_->empty(); }

    private:
        const Comparator& comparator_;
        std::vector<SeekTable*> children_;
        std::vector<SeekTableIterator*> children_iter_;
        std::unique_ptr<minHeap> iter_heap_;
        std::map<SeekTableIterator*, uint8_t> iter_map_;

        std::vector<uint16_t> pending_data_block_;
        std::vector<uint16_t> pending_index_block_;
        std::unique_ptr<SeekBlockBuilder> pilot_block_;
        std::string last_key_;
};

class PilotBlock {

};

} // namespace rocksdb
