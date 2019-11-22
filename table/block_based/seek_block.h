#pragma once

#include "table/format.h"
#include "table/internal_iterator.h"

namespace rocksdb
{

class SeekDataBlockIter final : public InternalIteratorBase<Slice> {
    public:
        SeekDataBlockIter() {};

        virtual Slice value() const override {
            assert(Valid());
            return value_;
        }

        virtual bool Valid() const override { return current_ < restarts_; }

        virtual void Seek(const Slice& target) override;

        virtual void SeekForPrev(const Slice& target) override {}

        virtual void Prev() override {}

        virtual void Next() final override;

        virtual void SeekToFirst() override;

        virtual void SeekToLast() override;

        virtual Slice key() const override {
            assert(Valid());
            return key_.GetKey();
        }

        virtual Status status() const override { return status_; }

        void Invalidate(Status s) {
            data_ = nullptr;
            current_ = restarts_;
            status_ = s;

            // call cleanup callbacks.
            Cleanable::Reset();
        }

        void Initialize(const Comparator* comparator, const char* data, uint32_t restarts, uint32_t num_restarts) {
            comparator_ = comparator;
            data_ = data;
            restarts_ = restarts;
            current_ = restarts_;
            num_restarts_ = num_restarts;
            restart_index_ = num_restarts_; // unknown
            key_.SetIsUserKey(false);
        }

        uint32_t GetRestartPoint(uint32_t index) {
            assert(index < num_restarts_);
            // add pointer to start of restart array
            // get index-th restart point
            return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
        }

        void SeekToRestartPoint(uint32_t index) {
            key_.Clear();
            // what for?
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
        
        inline int Compare(const IterKey& ikey, const Slice& b) const {
            return comparator_->Compare(ikey.GetInternalKey(), b);
        }

    private:
        const Comparator* comparator_;
        const char* data_; // block contents
        uint32_t num_restarts_;

        uint32_t restart_index_; // unknown in rocksdb

        uint32_t restarts_; // offset of restart array (where does restarts array start)
        uint32_t current_; // offset in data_ of current entry.
        IterKey key_;
        Slice value_;
        Status status_;

        inline bool ParseNextDataKey(const char* limit = nullptr);
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
