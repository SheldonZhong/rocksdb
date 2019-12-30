#pragma once

#include "table/table_reader.h"
#include "table/block_based/seek_block.h"
#include "table/block_based/pilot_value.h"

namespace rocksdb
{

class SeekTableIterator;
class SeekTable {
    public:
        InternalIterator* NewIterator() const ;
        SeekTableIterator* NewSeekTableIter() const;

        uint64_t ApproximateOffsetOf(const Slice& key);

        void SetupForCompaction();

        void Prepare(const Slice& target);

        Status Get(const Slice& key);

        InternalIterator* NewDataBlockIterator(
                const BlockHandle& block_handle,
                SeekDataBlockIter* input_iter = nullptr) const;

        void MultiGet();

        Status DumpTable(WritableFile* out_file);

        static Status Open(const Comparator& comparator,
                    std::unique_ptr<RandomAccessFileReader>&& file,
                    uint64_t file_size,
                    std::unique_ptr<SeekTable>* table_reader,
                    int level);

        class IndexReader {
            public:
                IndexReader(const SeekTable* t,
                            SeekBlock* index_block)
                    : table_(t), index_block_(index_block) {
                    assert(table_);
                }

                SeekDataBlockIter* NewIterator(
                    SeekDataBlockIter* iter
                );

                static Status ReadIndexBlock(const SeekTable* table,
                                            SeekBlock** index_block);

                static Status Create(
                                SeekTable* table,
                                std::unique_ptr<IndexReader>* index_reader);

            private:
                const SeekTable* table_;
                std::unique_ptr<SeekBlock> index_block_;
        };
    
        struct Rep;

        Rep* get_rep() { return rep_; }
        const Rep* get_rep() const { return rep_; }

    private:
        Rep* rep_;
        explicit SeekTable(Rep* rep) : rep_(rep) {}

        Status CreateIndexReader(std::unique_ptr<IndexReader>* index_reader);
        Status ReadMetaBlock(std::unique_ptr<SeekBlock>* meta_block,
                            std::unique_ptr<InternalIterator>* iter);

        Status ReadPilotBlock(const BlockHandle& handle,
                            std::unique_ptr<SeekBlock>* pilot_block,
                            std::unique_ptr<SeekDataBlockIter>* iter);

        Status RetrieveBlock(const BlockHandle& handle,
                            BlockContents* contents,
                            bool* pined = nullptr) const;

        SeekDataBlockIter* NewIndexIterator() const;
};

class SeekTableIterator : public InternalIteratorBase<Slice> {
    public:
        SeekTableIterator(const SeekTable* table,
                            const Comparator& comp,
                            SeekDataBlockIter* index_iter,
                            SeekDataBlockIter* pilot_iter = nullptr)
                        : table_(table),
                        comp_(comp),
                        index_iter_(index_iter),
                        block_iter_points_to_real_block_(false),
                        pilot_iter_(pilot_iter),
                        index_count_(0),
                        data_count_(0)
                        {}

        void Seek(const Slice& target) override;
        void SeekForPrev(const Slice& target) override;
        void SeekToFirst() override;
        void SeekToLast() override;
        // Next() should be able to use index information to jump to different levels
        void Next() final override;
        bool NextAndGetResult(IterateResult* result) override;
        void Prev() override;
        bool Valid() const override;
        Slice key() const override;
        Slice value() const override;
        Status status() const override;

        void Next(int k);
        void HintedSeek(const Slice& target,
                    uint32_t index_left, uint32_t index_right,
                    uint32_t data_left, uint32_t data_right);

        void ResetDataIter() {
            if (block_iter_points_to_real_block_) {
                block_iter_.Invalidate(Status::OK());
                block_iter_points_to_real_block_ = false;
            }
        }

        uint32_t Count() const;

        friend class SeekLevelIterator;
        friend class SeekTableBuilder;
        friend class PilotBlockBuilder;
        friend class PilotBlockMarsBuilder;
        friend class PilotBlockMarsIterator;
        
    private:
        const SeekTable* table_;
        const Comparator& comp_;
        SeekDataBlockIter* index_iter_;
        bool block_iter_points_to_real_block_;
        SeekDataBlockIter block_iter_;
        SeekDataBlockIter* pilot_iter_;

        uint32_t index_count_;
        uint32_t data_count_;

        uint32_t GetIndexBlock() const;
        uint32_t GetDataBlock() const;
        void GetPilot(PilotValue* pilot);
        void GetFirstPilot(PilotValue* pilot);
        void FollowAndGetPilot(PilotValue* pilot);

        void SeekImpl(const Slice* target);
        void InitDataBlock();
};

} // namespace name rocksdb

