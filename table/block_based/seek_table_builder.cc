#include "table/block_based/seek_table_builder.h"
#include "table/block_based/seek_block_builder.h"
#include "table/block_based/index_builder.h"
#include "table/block_based/seek_index_builder.h"
#include "table/seek_meta_blocks.h"
#include "include/rocksdb/flush_block_policy.h"

namespace rocksdb
{

const uint64_t kSeekTableMagicNumber = 0xdbbad01beefe0f44ull;

struct SeekTableBuilder::Rep {

    const InternalKeyComparator& internal_comparator;
    WritableFileWriter* file;
    uint64_t offset = 0;
    SeekBlockBuilder data_block;
    Status status;

    std::unique_ptr<IndexBuilder> index_builder;
    
    std::string last_key;

    TableProperties props;

    BlockHandle pending_handle;

    enum class State {
        kOpened,
        kClosed,
    };
    State state;

    uint64_t creation_time = 0;
    uint64_t oldest_key_time = 0;
    const uint64_t target_file_size;
    uint64_t file_creation_time = 0;

    uint64_t block_size;

    Rep(const InternalKeyComparator& icomparator, WritableFileWriter* f,
        const uint64_t _creation_time, const uint64_t _target_file_size,
        const uint64_t _file_creation_time, const uint64_t _block_size)
        : internal_comparator(icomparator),
        file(f),
        data_block(),
        state(State::kOpened),
        creation_time(_creation_time),
        target_file_size(_target_file_size),
        file_creation_time(_file_creation_time),
        block_size(_block_size) {

    index_builder.reset(new SeekIndexBuilder(&internal_comparator));
}

    Rep(const Rep&) = delete;
    Rep& operator=(const Rep&) = delete;

    ~Rep() {}
};

Status SeekTableBuilder::Finish() {
    Rep* r = rep_;
    assert(r->state != Rep::State::kClosed);
    bool empty_data_block = r->data_block.empty();
    Flush();

    SeekMetaIndexBuilder meta_index_builder;
    BlockHandle metaindex_block_handle, index_block_handle;

    WriteIndexBlock(&meta_index_builder, &index_block_handle);
    WriteRawBlock(meta_index_builder.Finish(), &metaindex_block_handle);
    WriteFooter(metaindex_block_handle, index_block_handle);
}

void SeekTableBuilder::WriteFooter(BlockHandle& metaindex_block_handle,
                                    BlockHandle& index_block_handle) {
    Rep* r = rep_;
    Footer footer(kSeekTableMagicNumber, 5);
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    footer.set_checksum(kCRC32c);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    assert(r->status.ok());
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
        r->offset += footer_encoding.size();
    }
}
void SeekTableBuilder::WriteIndexBlock(
    SeekMetaIndexBuilder* meta_index_builder, BlockHandle* index_block_handle) {
    IndexBuilder::IndexBlocks index_blocks;
    Status index_builder_status = rep_->index_builder->Finish(&index_blocks);
    assert(index_builder_status.ok());
    for (const auto& item : index_blocks.meta_blocks) {
        BlockHandle block_handle;
        WriteBlock(item.second, &block_handle, false /* is_data_block */);
        meta_index_builder->Add(item.first, block_handle);
    }
    WriteRawBlock(index_blocks.index_block_contents, index_block_handle);
}

void SeekTableBuilder::Abandon() {
    assert(rep_->state != Rep::State::kClosed);
    rep_->state = Rep::State::kClosed;
}

uint64_t SeekTableBuilder::NumEntries() const { return rep_->props.num_entries; }

uint64_t SeekTableBuilder::FileSize() const { return rep_->offset; }

void SeekTableBuilder::Flush() {
    Rep* r = rep_;
    assert(r->state != Rep::State::kClosed);
    if (!ok()) return;
    if (r->data_block.empty()) return;
    WriteBlock(&r->data_block, &r->pending_handle, true /* is _data_block */);
}

void SeekTableBuilder::WriteBlock(SeekBlockBuilder* block,
                                    BlockHandle* handle,
                                    bool is_data_block) {
    WriteBlock(block->Finish(), handle, is_data_block);
    block->Reset();
}

void SeekTableBuilder::WriteBlock(const Slice& raw_contents,
                                    BlockHandle* handle,
                                    bool is_data_block) {
    assert(ok());
    Rep* r = rep_;
    // there are some logic for the compression
    WriteRawBlock(raw_contents, handle, is_data_block);
}

Status SeekTableBuilder::status() const { return rep_->status; }

void SeekTableBuilder::WriteRawBlock(const Slice& raw_contents,
                                        BlockHandle* handle,
                                        bool is_data_block) {
    Rep* r = rep_;

    handle->set_offset(r->offset);
    handle->set_size(raw_contents.size());
    assert(r->status.ok());
    // write to file
    r->status = r->file->Append(raw_contents);
    if (r->status.ok()) {
        // write trailer and checksum
        char trailer[kBlockTrailerSize];
        trailer[0] = CompressionType::kNoCompression;
        char* trailer_without_type = trailer + 1;
        uint32_t crc = crc32c::Value(raw_contents.data(), raw_contents.size());
        crc = crc32c::Extend(crc, trailer, 1);
        EncodeFixed32(trailer_without_type, crc32c::Mask(crc));

        r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
        // if(r->status.ok()) {
            // r->status = InsertBlockInCache(raw_contents)
        // }
        // cache is not a priority
        if (r->status.ok()) {
            r->offset += raw_contents.size() + kBlockTrailerSize;
            size_t align = kDefaultPageSize;
            size_t pad_bytes = 
                (align - ((raw_contents.size() + kBlockTrailerSize) & (align - 1))) & (align - 1);
            r->status = r->file->Pad(pad_bytes);
            if (r->status.ok()) {
                r->offset += pad_bytes;
            }
        }
    }
}

void SeekTableBuilder::Add(const Slice& key, const Slice& value) {
    Rep* r = rep_;

    size_t estimated_size_after = r->data_block.EstimateSizeAfterKV(key, value);

    bool should_flush = (estimated_size_after + kBlockTrailerSize) > r->block_size;
    if (should_flush) {
        assert(!r->data_block.empty());
        Flush();

        if (ok()) {

            r->index_builder->AddIndexEntry(&r->last_key, &key, r->pending_handle);
        }
    }

    r->last_key.assign(key.data(), key.size());
    r->data_block.Add(key, value);

    r->index_builder->OnKeyAdded(key);

    r->props.num_entries++;
    r->props.raw_key_size += key.size();
    r->props.raw_value_size += value.size();
}

} // namespace name rocksdb

