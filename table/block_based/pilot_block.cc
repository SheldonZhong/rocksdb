#include "table/block_based/pilot_block.h"
#include "util/coding.h"

namespace rocksdb {

void PilotValue::EncodeTo(std::string* dst) const {
    PutVarint32(dst, static_cast<uint32_t>(index_block_.size()));
    for (size_t i = 0; i < index_block_.size(); i++) {
        PutFixed32(dst, index_block_[i]);
        PutFixed32(dst, data_block_[i]);
    }

    PutVarint32(dst, static_cast<uint32_t>(levels_.size()));
    char put[levels_.size()];
    for (size_t i = 0; i < levels_.size(); i++) {
        put[i] = static_cast<char>(levels_[i]);
    }
    dst->append(put, levels_.size());
}

Status PilotValue::DecodeFrom(Slice* input) {
    uint32_t num_levels;
    if (!GetVarint32(input, &num_levels)) {
        return Status::Corruption("bad encode pilot value entry start");
    }
    index_block_.resize(num_levels);
    data_block_.resize(num_levels);

    for (uint32_t i = 0; i < num_levels; i++) {
        uint32_t buf;
        if (!GetFixed32(input, &buf)) {
            return Status::Corruption("bad encode pilot value index_block");
        }
        index_block_[i] = buf;
        if (!GetFixed32(input, &buf)) {
            return Status::Corruption("bad encode pilot value data_block");
        }
    }

    uint32_t n;
    if (!GetVarint32(input, &n)) {
        return Status::Corruption("bad encode pilot value level start");
    }
    levels_.resize(n);
    for (uint32_t i = 0; i < n; i++) {
        uint8_t buf;
        if (input->size() < sizeof(uint8_t)) {
            return Status::Corruption("bad encode pilot value levels");
        }
        memcpy(&buf, input->data(), sizeof(buf));
        input->remove_prefix(sizeof(uint8_t));
        levels_[i] = buf;
    }

    return Status::OK();
}

PilotBlockBuilder::PilotBlockBuilder()
    : pilot_block_() {}

void PilotBlockBuilder::AddPilotEntry(
                                const Slice& key,
                                std::vector<uint32_t>& index_block,
                                std::vector<uint32_t>& data_block,
                                std::vector<uint8_t>& levels) {
    PilotValue entry(index_block, data_block, levels);
    std::string entry_encoded;
    entry.EncodeTo(&entry_encoded);

    pilot_block_->Add(key, entry_encoded);
}

void PilotBlockBuilder::AddFirstEntry(std::vector<uint8_t>& levels) {
    std::vector<uint32_t> index;
    std::vector<uint32_t> data;
    AddPilotEntry("\0", index, data, levels);
}

// TODO: the code does not look good
Slice PilotBlockBuilder::Finish() {
    return pilot_block_->Finish();
}

} // namespace rocksdb