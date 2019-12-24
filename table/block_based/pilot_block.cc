#include "table/block_based/pilot_block.h"
#include "util/coding.h"

namespace rocksdb {

PilotBlockBuilder::PilotBlockBuilder(const Comparator& comparator,
                                    SeekTable** levels, int n)
            : comparator_(comparator),
            iter_heap_(new minHeap(comparator_)),
            pilot_block_(new SeekBlockBuilder) {
    children_.resize(n);
    children_iter_.resize(n);
    if (levels != nullptr && n > 0) {
        for (int i = 0; i < n; i++) {
            children_[i] = levels[i];
            children_iter_[i] = children_[i]->NewSeekTableIter();
            children_iter_[i]->SeekToFirst();
            iter_map_[children_iter_[i]] = static_cast<uint8_t>(i);
            iter_heap_->push(children_iter_[i]);
        }
    }
}

void PilotBlockBuilder::BuildPilot(const Slice* key) {
    assert(!children_iter_.empty());

    std::vector<uint8_t> levels;
    while (!iter_heap_->empty()) {
        SeekTableIterator* ptr = iter_heap_->top();
        if (key == nullptr || comparator_.Compare(ptr->key(), *key) < 0) {
            ptr->Next();
            if (ptr->Valid()) {
                iter_heap_->replace_top(ptr);
            } else {
                iter_heap_->pop();
            }
            uint8_t idx = iter_map_[ptr];
            levels.push_back(idx);
        } else {
            break;
        }
    }

    if (empty()) {
        AddFirstEntry(levels);
    } else {
        // should use the previous index and data offset
        // since the iterators are now behind key instead of r->last_key
        AddPilotEntry(last_key_, pending_index_block_,
                                        pending_data_block_, levels);
    }
    if (key != nullptr) {
        last_key_.assign(key->data(), key->size());
    }

    pending_index_block_.clear();
    pending_data_block_.clear();
    if (!iter_heap_->empty()) {
        for (size_t i = 0; i < children_iter_.size(); i++) {
            uint16_t index = 0x8000;
            uint16_t data = 0x8000;
            if (children_iter_[i]->Valid()) {
                uint32_t idx = children_iter_[i]->GetIndexBlock();
                assert((idx & 0xFFFF0000) == 0);
                index = static_cast<uint16_t>(idx);
                idx = children_iter_[i]->GetDataBlock();
                assert((idx & 0xFFFF0000) == 0);
                data = static_cast<uint16_t>(idx);
            }
            pending_index_block_.push_back(index);
            pending_data_block_.push_back(data);
        }
    }
}

void PilotBlockBuilder::AddPilotEntry(
                                const Slice& key,
                                std::vector<uint16_t>& index_block,
                                std::vector<uint16_t>& data_block,
                                std::vector<uint8_t>& levels) {
    PilotValue entry(index_block, data_block, levels);
    std::string entry_encoded;
    entry.EncodeTo(&entry_encoded);

    pilot_block_->Add(key, entry_encoded);
}

void PilotBlockBuilder::AddFirstEntry(std::vector<uint8_t>& levels) {
    std::vector<uint16_t> index;
    std::vector<uint16_t> data;
    AddPilotEntry("\0", index, data, levels);
}

// TODO: the code does not look good
Slice PilotBlockBuilder::Finish() {
    return pilot_block_->Finish();
}

} // namespace rocksdb