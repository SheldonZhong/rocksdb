#pragma once

#include "table/block_based/seek_block_builder.h"
#include "table/seek_meta_blocks.h"
#include "table/block_based/seek_table_reader.h"

#include "table/format.h"
#include "table/table_builder.h"

namespace rocksdb
{
extern const uint64_t kSeekTableMagicNumber;

class SeekTableBuilder : public TableBuilder {
    public:
        SeekTableBuilder() {}

        SeekTableBuilder(
            const Comparator& comparator,
            WritableFileWriter* file,
            SeekTable** lower_tables = nullptr, int n = 0);

        ~SeekTableBuilder() {}

        SeekTableBuilder(const SeekTableBuilder&) = delete;
        SeekTableBuilder& operator=(const SeekTableBuilder&) = delete;

        void Add(const Slice& key, const Slice& value) override;

        Status status() const override;

        Status Finish() override;

        void Abandon() override;

        uint64_t NumEntries() const override;

        uint64_t FileSize() const override;

        bool NeedCompact() const override { return false; }

        TableProperties GetTableProperties() const override;

        void AddNext(int level, uint32_t index);

    private:
        bool ok() const { return status().ok(); }

        struct Rep;

        Rep* rep_;

        void Flush();

        void WriteBlock(SeekBlockBuilder* block, BlockHandle* handle, bool is_data_block);
        void WriteBlock(const Slice& block_contents, BlockHandle* handle, bool is_data_block);

        void WriteRawBlock(const Slice& data, BlockHandle* handle, bool is_data_block = false);

        void WriteFooter(BlockHandle& metaindex_block_handle,
                            BlockHandle& index_block_handle);
        
        void WriteIndexBlock(SeekMetaIndexBuilder* meta_index_builder,
                                BlockHandle* index_block_handle);

        void WritePilotBlock(SeekMetaIndexBuilder* metaindex_block_handle);
};

} // namespace namerocksdb

