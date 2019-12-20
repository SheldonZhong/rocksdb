#include "table/block_based/pilot_block.h"
#include "util/coding.h"

namespace rocksdb {

void PilotValue::EncodeTo(std::string* dst) const {
    PutVarint32(dst, static_cast<uint32_t>(index_block_.size()));
    for (size_t i = 0; i < index_block_.size(); i++) {
        PutFixed16(dst, index_block_[i]);
        PutFixed16(dst, data_block_[i]);
    }

    PutVarint32(dst, levels_size_);
    dst->append((char*)(levels_), levels_size_);
}

Status PilotValue::DecodeFrom(Slice* input) {
    uint32_t num_levels;
    if (!GetVarint32(input, &num_levels)) {
        return Status::Corruption("bad encode pilot value entry start");
    }
    index_block_.resize(num_levels);
    data_block_.resize(num_levels);

    for (uint32_t i = 0; i < num_levels; i++) {
        uint16_t buf;
        if (!GetFixed16(input, &buf)) {
            return Status::Corruption("bad encode pilot value index_block");
        }
        index_block_[i] = buf;
        if (!GetFixed16(input, &buf)) {
            return Status::Corruption("bad encode pilot value data_block");
        }
        data_block_[i] = buf;
    }

    uint32_t n;
    if (!GetVarint32(input, &n)) {
        return Status::Corruption("bad encode pilot value level start");
    }
    levels_size_ = n;
    if (levels_ != nullptr) {
        delete[] levels_;
    }
    levels_ = new uint8_t[n];
    if (input->size() < sizeof(uint8_t) * n) {
        return Status::Corruption("bad encode pilot value levels");
    }
    memcpy(levels_, input->data(), sizeof(uint8_t) * n);
    input->remove_prefix(sizeof(uint8_t) * n);

    return Status::OK();
}

PilotBlockBuilder::PilotBlockBuilder()
    : pilot_block_(new SeekBlockBuilder) {}

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