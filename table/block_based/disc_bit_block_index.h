// Wenshao Zhong (wzhong20@uic.edu)

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
// This feature aims to reduce the CPU cost of range-queries within a block.
// This is used in all blocks.
// Not only does it help point queries BlockBasedTable::Get()
// but it also supports efficient range queries [function name here]
// such that a range query only does one key comparison
// instead of log(n) key comparisons in binary search.
// A compact index is appended to the blocks. The new block format would be:
//
// block: [RI RI RI ... RI RESTARTS RANKS MASK FOOTER]
// 
// RI:       Restart interval (the same as the original block format)
// RESTARTS: Restart array (the same as the original block format)
// RANKS (1-byte each):
//        An array that records the ranks of discriminative bits
//           that only takes one byte for each pair of neighboring keys
// MASK (variable + 2-byte length):
//        An encoding of the bitmap that records the positions of
//           discriminative bits.
// FOOTER (4-byte):
//        The block footer that records the number of restarts in the block.
//
//

class DiscBitBlockIndexBuilder {
 public:
  DiscBitBlockIndexBuilder()
  : unique_(0),
    counter_(0) {}

  void Initialize();

  void Add(const Slice& key);

  void Finish(std::string& buffer);

  bool Valid() const;

  void Reset();

  size_t EstimateSize() const;

 private:
  std::string partial_mask_;
  std::vector<std::pair<size_t, uint8_t>> lcp_mask_pairs_;
  std::string last_key_;

  size_t estimate_size_;
  int unique_;
  int counter_;
};

class DiscBitBlockIndex {
 public:
  DiscBitBlockIndex() 
  : ranks_(nullptr),
    max_rank_(0),
    num_restarts_(0)
  {}

  size_t Initialize(const char* data, size_t size,
                    uint32_t num_restarts);

  bool Valid() const { return num_restarts_ > 0; }

  size_t Lookup(const Slice& key) const;

  int64_t FinishSeek(const Slice& key, const Slice& probe_key,
                size_t probe_pos, int cmp) const;

  size_t PartialKeyLookup(uint64_t pkey) const;

  uint64_t SliceExtract(const Slice& key) const;

 private:
  
  const uint8_t* ranks_; // ranks array
  size_t num_ranks_;
  uint8_t max_rank_;
  size_t num_restarts_;
  std::string partial_mask_;

  int64_t PartialKeyLCP(const Slice& target, const Slice& key) const;
};

}