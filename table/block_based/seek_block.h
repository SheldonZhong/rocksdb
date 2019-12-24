#pragma once

#include "table/format.h"
#include "table/internal_iterator.h"

namespace rocksdb
{

static inline const char* DecodeEntry(const char* p, const char* limit,
                                        uint32_t* key_length,
                                        uint32_t* value_length) {
    assert(limit - p >= 2);
    // fast path for varint
    // 2 bytes 
    if ((p = GetVarint32Ptr(p, limit, key_length)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;

    // enough room for the actual key and value
    assert(!(static_cast<uint32_t>(limit - p) < (*key_length + *value_length)));
    return p;
}

class SeekDataBlockIter final : public InternalIteratorBase<Slice> {
    public:
        SeekDataBlockIter()
        : data_(nullptr) {};

        virtual Slice value() const override {
            assert(Valid());
            return value_;
        }

        virtual bool Valid() const override { return current_ < restarts_; }

        virtual void Seek(const Slice& target) override;

        void HintedSeek(const Slice& target, uint32_t left, uint32_t right);

        virtual void SeekForPrev(const Slice& target) override;

        virtual void Prev() override;

        virtual void Next() final override;

        virtual void SeekToFirst() override;

        virtual void SeekToLast() override;

        virtual Slice key() const override {
            assert(Valid());
            return key_;
        }

        virtual Status status() const override { return status_; }

        void Invalidate(Status s) {
            if (data_ != nullptr) {
                delete[] data_;
            }
            data_ = nullptr;
            current_ = restarts_;
            status_ = s;

            // call cleanup callbacks.
            Cleanable::Reset();
        }

        void Initialize(const Comparator* comparator, const char* data, uint32_t restarts, uint32_t num_restarts) {
            comparator_ = comparator;
            if (data_ != nullptr) {
                delete[] data_;
            }
            data_ = data;
            restarts_ = restarts;
            current_ = restarts_;
            num_restarts_ = num_restarts;
            restart_index_ = num_restarts_; // unknown
        }

        uint32_t GetRestartPoint(uint32_t index) {
            assert(index < num_restarts_);
            // add pointer to start of restart array
            // get index-th restart point
            return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
        }

        void SeekToRestartPoint(uint32_t index) {
            key_.clear();
            restart_index_ = index;

            uint32_t offset = GetRestartPoint(index);
            // length is not given (0)
            // ParseNextKey() starts at the end of value_
            value_ = Slice(data_ + offset, 0);
        }

        // return the offset in data_ just past the end of current entry
        // the first one / after call to SeekToRestartPoint
        // would be (data_ + offset) + 0 - data_
        // so the entry pointing to must be a key length
        inline uint32_t NextEntryOffset() const {
            return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
        }

        void CorruptionError();

        inline bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                                uint32_t* index, const Comparator* comp);
        
        inline int Compare(const Slice& key, const Slice& b) const {
            return comparator_->Compare(key, b);
        }

        inline bool ParseNextDataKey(const char* limit = nullptr) {
            current_ = NextEntryOffset();
            const char* p = data_ + current_;
            if (!limit) {
                // if limit is not specified
                // the limit goes all the way to the end
                limit = data_ + restarts_; //
            }

            if (p >= limit) {
                // no more entries, mark as invalid
                current_ = restarts_;
                restart_index_ = num_restarts_;
                return false;
            }
            
            uint32_t key_length, value_length;
            p = DecodeEntry(p, limit, &key_length, &value_length);

            if (p == nullptr) {
                CorruptionError();
                return false;
            }

            key_ = Slice(p, key_length);
            value_ = Slice(p + key_length, value_length);

            while (restart_index_ + 1 <= num_restarts_ &&
                    GetRestartPoint(restart_index_) < current_) {

                restart_index_++;
            }

            return true;
        }

        inline uint32_t GetRestartIndex() const { return restart_index_; }
    protected:
        uint32_t num_restarts_;
        friend class SeekTableIterator;
    private:
        const Comparator* comparator_;
        const char* data_; // block contents

        uint32_t restart_index_; // current index

        uint32_t restarts_; // offset of restart array (where does restarts array start)
        uint32_t current_; // offset in data_ of current entry.
        Slice key_;
        Slice value_;
        Status status_;
};


class SeekBlock {
    public:
        explicit SeekBlock(BlockContents&& contents);

        ~SeekBlock() {}


        size_t size() const { return size_; }
        const char* data() const { return data_; }
        size_t usable_size() const { return contents_.usable_size(); }
        uint32_t NumEntries() const;

        SeekDataBlockIter* NewDataIterator(const Comparator* comparator,
                                        SeekDataBlockIter* iter = nullptr);

    private:
        BlockContents contents_;
        const char* data_;
        size_t size_;
        uint32_t restart_offset_;
        uint32_t num_entries_;

        // No copying allowed
        SeekBlock(const SeekBlock&) = delete;
        void operator=(const SeekBlock&) = delete;
};
} // namespace namerockrocksdb
