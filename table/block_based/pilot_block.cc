#include "table/block_based/pilot_block.h"
#include "util/coding.h"

namespace rocksdb {

PilotBlockBuilder::PilotBlockBuilder()
  : pilot_block_(),
  length(0), buff(0) {}

void PilotBlockBuilder::MarkUp() {
  buff |= (1 << (length & 7));
  advance();
}

void PilotBlockBuilder::MarkDown() {
  advance();
}

void PilotBlockBuilder::advance() {
  length++;
  if (length & 7) {
    pilot_.push_back(buff);
    buff = 0;
  }
}

// TODO: the code does not look good
Slice PilotBlockBuilder::Finish() {
  std::string dst;
  PutVarint32(&dst, length);
  // the keys added must in order
  pilot_block_->Add("length", dst);
  char* data = new char[length];

  assert(data);
  Slice s = Slice(data, length/8 + 1);
  pilot_block_->Add("pilot", s);

  return pilot_block_->Finish();
}

} // namespace rocksdb