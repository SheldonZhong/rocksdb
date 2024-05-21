#include "table/block_based/disc_bit_block_index.h"

#include <unordered_map>

#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

uint8_t GetDiscBitMask(const Slice& key1, const Slice& key2, size_t shared) {
  // ordering assumption
  assert(key1.compare(key2) < 0);
  const size_t len = (key1.size() < key2.size()) ? key1.size() : key2.size();

  uint8_t diff = 0;
  if (shared == len) {
    // with the ordering assumption, key1 is the prefix of key2
    assert(len == key1.size());
    diff = key2[len];
  } else {
    const uint8_t byte1 = key1[shared];
    const uint8_t byte2 = key2[shared];
    diff = byte1 ^ byte2;
  }
  assert(diff != 0);
  const int shift = FloorLog2<uint8_t>(diff);
  assert(shift < 8);
  return 1u << shift;
}

void DiscBitBlockIndexBuilder::Add(const Slice& key) {
  // the first key in this block
  if (counter_ > 0) {
    const size_t shared = key.difference_offset(last_key_);
    const uint8_t mask = GetDiscBitMask(last_key_, key, shared);
    lcp_mask_pairs_.emplace_back(shared, mask);

    if ((shared + 1) > partial_mask_.size()) {
      partial_mask_.resize(shared + 1);
    }
    if ((partial_mask_[shared] & mask) == 0) {
      unique_++;
    }
    partial_mask_[shared] |= mask;
  }
  last_key_.assign(key.data(), key.size());
  assert(key.size() > 0);
  counter_++;
}

void DiscBitBlockIndexBuilder::Reset() {
  partial_mask_.clear();
  lcp_mask_pairs_.clear();
  last_key_.clear();
  unique_ = 0;
  counter_ = 0;
}

void DiscBitBlockIndexBuilder::Initialize() {
  valid_ = true;
}

size_t DiscBitBlockIndexBuilder::EstimateSize() const {
  return 0;
}

bool DiscBitBlockIndexBuilder::Valid() const {
  return valid_;
}

void DiscBitBlockIndexBuilder::Finish(std::string& buffer) {
  std::unordered_map<size_t, uint8_t> pos_rank_map;
  // this map seems unnecessary
  // we could use a vector of the pairs, and the index is the rank

  uint8_t rank = 0;
  for (size_t i = 0; i < partial_mask_.size(); i++) {
    if (partial_mask_[i] == 0) {
      continue;
    }
    for (int shift = 0; shift < 8; shift++) {
      const uint8_t mask = (0x80 >> shift);
      if (partial_mask_[i] & mask) {
        const size_t pos = i * 8 + (7 - shift);
        pos_rank_map.emplace(pos, rank);
        assert(rank <= UINT8_MAX);
        rank++;
      }
    }
  }

  for (auto const& lcp_mask : lcp_mask_pairs_) {
    const size_t lcp = lcp_mask.first;
    const uint8_t mask = lcp_mask.second;
    const size_t pos = lcp * 8 + CountTrailingZeroBits(mask);

    auto const& it = pos_rank_map.find(pos);
    assert(it != pos_rank_map.end());

    rank = it->second;
    buffer.append(reinterpret_cast<const char*>(&rank),
                  sizeof(rank));
  }

  buffer.append(partial_mask_.data(), partial_mask_.size());
  PutFixed16(&buffer, static_cast<uint16_t>(partial_mask_.size()));
}

// returns how many bytes it uses
size_t DiscBitBlockIndex::Initialize(const char* data, size_t size,
                                    uint32_t num_restarts) {
  num_restarts_ = num_restarts;
  num_ranks_ = num_restarts - 1;
  uint16_t mask_size = DecodeFixed16(data + size - sizeof(uint16_t));
  const char* const partial_mask = data + size - sizeof(uint16_t) - mask_size;
  partial_mask_.append(partial_mask, mask_size);
  ranks_ = reinterpret_cast<const uint8_t*>(partial_mask) - num_ranks_;

  for (int i = 0; i < mask_size; i++) {
    const uint8_t mask = partial_mask_[i];
    max_rank_ += BitsSetToOne(mask);
  }

  return mask_size + sizeof(uint16_t) + num_ranks_;
}

uint64_t DiscBitBlockIndex::SliceExtract(const Slice& key) const {
  const size_t mask_len = partial_mask_.size();
  uint64_t out = 0;

  for (size_t i = 0; i < mask_len; i++) {
    if (partial_mask_[i] == 0) {
      continue;
    }

    const uint8_t mask = partial_mask_[i];
    const int shifts = BitsSetToOne(mask);
    out <<= shifts;

    if (i < key.size()) {
      const uint8_t byte = key[i];
      const uint8_t extract = ParallelExtract(byte, mask);
      out |= extract;
    }
  }

  return out;
}

size_t DiscBitBlockIndex::PartialKeyLookup(uint64_t pkey) const {
  size_t pos = 0;
  for (size_t i = 0; i < num_ranks_;) {
    const uint8_t rank = ranks_[i];
    const uint64_t mask = 1lu << (max_rank_ - 1 - rank);

    if (pkey & mask) {
      i++;
      pos = i;
    } else {
      while (i < num_ranks_ && ranks_[i] >= rank) {
        i++;
      }
    }
  }

  return pos;
}

// cmp should be the result of cmp(key, probe_key)
size_t DiscBitBlockIndex::FinishSeek(const Slice& key,
                                      const Slice& probe_key,
                                      size_t probe_pos,
                                      int cmp) const {
  if (cmp == 0) {
    return probe_pos;
  }
  size_t pkey_lcp = PartialKeyLCP(key, probe_key);
  // ignoring all corner cases

  size_t pos = probe_pos;

  if (cmp > 0) {
    // probe_key < key
    while (pos < num_ranks_) {
      const uint8_t rank = ranks_[pos];
      pos++;
      if (rank < pkey_lcp) {
        return pos;
      }
    }
    pos++;
  } else {
    // probe_key > key
    while (pos > 0) {
      const uint8_t rank = ranks_[pos - 1];
      if (rank < pkey_lcp) {
        break;
      }
      pos--;
    }
  }
  return pos;
}

size_t DiscBitBlockIndex::PartialKeyLCP(const Slice& target, const Slice& key) const {
  const size_t mask_len = partial_mask_.size();
  size_t lcp = 0;

  const Slice& shorter = (key.size() < target.size()) ? key : target;

  for (size_t i = 0; i < mask_len; i++) {
    if (partial_mask_[i] == 0) {
      continue;
    }

    const uint8_t m = partial_mask_[i];
    const int shifts = BitsSetToOne(m);

    if (i < key.size() && i < target.size()) {
      const uint8_t byte1 = key[i];
      const uint8_t byte2 = target[i];

      const uint8_t x = byte1 ^ byte2;
      if (x != 0) {
        // objective: find the first 1 in x
        // need a mask that is 1 before the first 1 in x
        // equivalent to: ~((1 << (FloorLog2(x) + 1)) - 1)
        // example 1: FloorLog2(1) = 0, 1 << (0 + 1) = 0b10
        // 0b10 - 1 = 0b1, and reverse it: 0b1111110
        // example 2: FloorLog2(1001) = 3, 1 << (3 + 1) = 0b10000
        // 0b10000 - 1 = 0b1111, and reverse it: 0b1111110000
        const int log2 = FloorLog2(x);
        const uint8_t mask = (1 << (log2+1)) - 1;
        lcp += BitsSetToOne(m & (~mask));
        return lcp;
      }
      lcp += shifts;
    } else {
      if (i < shorter.size()) {
        const uint8_t byte = shorter[i];
        if (byte != 0) {
          const int log2 = FloorLog2(byte);
          const uint8_t mask = (1 << (log2+1)) - 1;
          lcp += BitsSetToOne(m & (~mask));
          return lcp;
        }
      }
      lcp += shifts;
    }
  }

  return lcp;
}

size_t DiscBitBlockIndex::Lookup(const Slice& key) const {
  uint64_t pkey = SliceExtract(key);
  size_t pos = PartialKeyLookup(pkey);
  return pos;
}

}  // namespace ROCKSDB_NAMESPACE