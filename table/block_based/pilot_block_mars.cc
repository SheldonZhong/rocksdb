#include "table/block_based/pilot_block_mars.h"

namespace rocksdb
{

PilotBlockMarsBuilder::PilotBlockMarsBuilder(
            const Comparator& comp,
            SeekTable** levels,
            int n,
            std::vector<uint16_t*>* counts,
            WritableFileWriter* file)
            : comparator_(comp),
            levels_(nullptr),
            num_levels_(0),
            pilot_block_(new SeekTableBuilder(
                comparator_, file
            )),
            counts_(counts) {
    if (levels != nullptr && n > 0) {
        levels_ = levels;
        num_levels_ = n;
    }
}

void PilotBlockMarsBuilder::Build() {
    std::unique_ptr<minHeap> iter_heap_(new minHeap(comparator_));
    std::vector<SeekTableIterator*> iters;

    std::map<SeekTableIterator*, uint8_t> iter_map;
    // export the counts array
    // std::vector<uint16_t*> counts;
    std::vector<size_t> counts_size;

    iters.resize(num_levels_);
    counts_->resize(num_levels_);
    counts_size.resize(num_levels_);
    for (int i = 0; i < num_levels_; i++) {
        iters[i] = levels_[i]->NewSeekTableIter();
        iters[i]->SeekToFirst();
        iters[i]->SeekToLast();
        uint32_t n = iters[i]->index_count_;
        counts_->operator[](i) = new uint16_t[n];

        iter_map[iters[i]] = static_cast<uint8_t>(i);
        iters[i]->SeekToFirst();
        iter_heap_->push(iters[i]);
    }

    std::string last_key;
    SeekTableIterator* ptr = iter_heap_->top();
    last_key.assign(ptr->key().data(), ptr->key().size());

    std::vector<uint16_t> last_data_block(num_levels_);
    std::vector<uint16_t> last_index_block(num_levels_);

    for (int i = 0; i < num_levels_; i++) {
        last_data_block[i] = iters[i]->GetDataBlock();
        last_index_block[i] = iters[i]->GetIndexBlock();
    }

    const size_t kSpace = 256;
    uint8_t* jTable = new uint8_t[kSpace];
    size_t jcnt = 0;

    while (!iter_heap_->empty()) {
        ptr = iter_heap_->top();
        ptr->Next();
        if (ptr->Valid()) {
            iter_heap_->replace_top(ptr);
        } else {
            iter_heap_->pop();
        }

        uint8_t level = iter_map[ptr];
        uint16_t* count = counts_->at(level);
        size_t size = counts_size[level];
        count[size] = jcnt;
        // counts[level][counts_size[level]++] = jcnt;
        counts_size[level]++;
        jTable[jcnt++] = level;
        if (jcnt >= kSpace) {
            // insert new pointer
            std::string encoded;
            PilotValue entry(last_index_block, last_data_block, jTable, kSpace);
            entry.EncodeTo(&encoded);
            pilot_block_->Add(last_key, encoded);
            jcnt = 0;

            if (iter_heap_->empty()) {
                return;
            }
            ptr = iter_heap_->top();
            last_key.assign(ptr->key().data(), ptr->key().size());
            for (int i = 0; i < num_levels_; i++) {
                last_data_block[i] = iters[i]->GetDataBlock();
                last_index_block[i] = iters[i]->GetIndexBlock();
            }
        }
    }
    // insert new pointer
    std::string encoded;
    PilotValue entry(last_index_block, last_data_block, jTable, jcnt);
    entry.EncodeTo(&encoded);
    pilot_block_->Add(last_key, encoded);
    jcnt = 0;

    // last_key.assign(ptr->key().data(), ptr->key().size());
    for (int i = 0; i < num_levels_; i++) {
        last_data_block[i] = iters[i]->GetDataBlock();
        last_index_block[i] = iters[i]->GetIndexBlock();
    }

    delete[] jTable;
    // caller free counts_
    // for (auto p : counts) {
    //     delete[] p;
    // }
    for (auto p : iters) {
        delete p;
    }
}

Status PilotBlockMarsBuilder::Finish() {
    return pilot_block_->Finish();
}

// reader part

PilotBlockMarsIterator::PilotBlockMarsIterator(
        SeekTable* contents,
        std::vector<uint16_t*>* counts,
        SeekTableIterator** iters,
        const Comparator* comp)
    : pilot_block_(contents),
    pilot_iter_(nullptr),
    counts_(counts),
    iters_(iters),
    num_levels_(counts_->size()),
    current_(0),
    current_iter_(nullptr),
    comp_(comp),
    index_left_(num_levels_),
    data_left_(num_levels_),
    index_right_(num_levels_),
    data_right_(num_levels_) {

    pilot_iter_ = pilot_block_->NewSeekTableIter();

}

void PilotBlockMarsIterator::SeekToFirst() {
    pilot_iter_->SeekToFirst();
    for (size_t i = 0; i < num_levels_; i++) {
        iters_[i]->SeekToFirst();
    }
    ParsePilot();
    current_ = 0;
    size_t iter_index = static_cast<size_t>(entry_.levels_[current_]);
    assert(iter_index < num_levels_);
    current_iter_ = iters_[iter_index];
}

void PilotBlockMarsIterator::Seek(const Slice& target) {
    pilot_iter_->SeekForPrev(target);
    ParsePilot();

    index_left_ = entry_.index_block_;
    data_left_ = entry_.data_block_;
    if (pilot_iter_->index_iter_->GetRestartIndex() + 2 <= 
            pilot_iter_->index_iter_->num_restarts_ &&
            pilot_iter_->block_iter_.GetRestartIndex() + 2 <=
            pilot_iter_->block_iter_.num_restarts_) {
    // if (pilot_iter_->Valid()) {
        pilot_iter_->Next();
        ParsePilot();
        index_right_ = entry_.index_block_;
        data_right_ = entry_.data_block_;
        pilot_iter_->Prev();
        ParsePilot();
    } else {
        // the logic for the last key should be simplified
        for (size_t i = 0; i < num_levels_; i++) {
            iters_[i]->SeekToLast();
            index_right_[i] = iters_[i]->index_iter_->num_restarts_;
            data_right_[i] = iters_[i]->block_iter_.num_restarts_;
        }
        // pilot_iter_->SeekForPrev(target);
    }

    current_ = std::numeric_limits<decltype(current_)>::max();
    Slice key;
    bool first = true;
    for (size_t i = 0; i < num_levels_; i++) {
        iters_[i]->HintedSeek(target, index_left_[i], index_right_[i],
                                data_left_[i], data_right_[i]);
        
        // if (pos < current_) {
        if (iters_[i]->Valid()) {
            if (first || comp_->Compare(key, iters_[i]->key()) > 0) {
                uint32_t index = iters_[i]->Count();
                uint32_t pos = counts_->at(i)[index];
                first = false;
                key = iters_[i]->key();
                current_ = pos;
                // assert(i == static_cast<size_t>(entry_.levels_[current_]));
                current_iter_ = iters_[i];
            }
        }
    }

    // while ()
}

void PilotBlockMarsIterator::Next() {
    current_iter_->Next();
    current_++;
    if (current_ >= entry_.levels_size_) {
        pilot_iter_->Next();
        if (!pilot_iter_->Valid()) {
            return;
        }
        ParsePilot();
        current_ = 0;
    }
    size_t iter_index = static_cast<size_t>(entry_.levels_[current_]);
    assert(iter_index < num_levels_);
    current_iter_ = iters_[iter_index];
}

Slice PilotBlockMarsIterator::key() const {
    assert(Valid());
    return current_iter_->key();
}

Slice PilotBlockMarsIterator::value() const {
    assert(Valid());
    return current_iter_->value();
}

Status PilotBlockMarsIterator::status() const {
    assert(Valid());
    return current_iter_->status();
}

bool PilotBlockMarsIterator::Valid() const {
    if (current_iter_ != nullptr) {
        return current_iter_->Valid();
    }
    return false;
}

} // namespace namerocksdb
