#include "table/block_based/seek_table_builder.h"
#include "table/block_based/seek_block_builder.h"
#include "table/block_based/index_builder.h"
#include "table/block_based/seek_index_builder.h"
#include "table/block_based/pilot_block.h"
#include "table/seek_meta_blocks.h"
#include "util/heap.h"

namespace rocksdb
{

const uint64_t kSeekTableMagicNumber = 0xdbbad01beefe0f44ull;

struct IterComparator {
    IterComparator(const Comparator& _comparator)
        : comparator(_comparator) {}

    // implement greater than
    inline bool operator()(SeekTableIterator* a,
                            SeekTableIterator* b) const {
        return comparator.Compare(a->key(), b->key()) > 0;
    }

    const Comparator& comparator;
};

typedef BinaryHeap<SeekTableIterator*, IterComparator> minHeap;

struct SeekTableBuilder::Rep {

    const Comparator& comparator;
    WritableFileWriter* file;
    uint64_t offset = 0;
    SeekBlockBuilder data_block;
    Status status;

    std::unique_ptr<SeekIndexBuilder> index_builder;

    std::vector<SeekTable*> children;
    std::vector<SeekTableIterator*> children_iter;

    std::unique_ptr<minHeap> iter_heap;
    std::map<SeekTableIterator*, uint8_t> iter_map;

    std::unique_ptr<PilotBlockBuilder> pilot_builder;
    std::vector<uint32_t> pending_data_block_;
    std::vector<uint32_t> pending_index_block_;

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

    Rep(const Comparator& _comparator, WritableFileWriter* f,
        const uint64_t _creation_time, const uint64_t _target_file_size,
        const uint64_t _file_creation_time, const uint64_t _block_size,
        SeekTable** lower_levels, int n)
        : comparator(_comparator),
        file(f),
        data_block(),
        iter_heap(new minHeap(comparator)),
        state(State::kOpened),
        creation_time(_creation_time),
        target_file_size(_target_file_size),
        file_creation_time(_file_creation_time),
        block_size(_block_size) {

    index_builder.reset(new SeekIndexBuilder(&comparator));

    if (lower_levels != nullptr && n > 0) {
        children.resize(n);
        children_iter.resize(n);
        for (int i = 0; i < n; i++) {
            children[i] = lower_levels[i];
            children_iter[i] = children[i]->NewSeekTableIter();
            children_iter[i]->SeekToFirst();
            iter_map[children_iter[i]] = static_cast<uint8_t>(i);
            iter_heap->push(children_iter[i]);
        }

        pilot_builder.reset(new PilotBlockBuilder());
    }
}

    Rep(const Rep&) = delete;
    Rep& operator=(const Rep&) = delete;

    ~Rep() {}
};

void SeekTableBuilder::BuildPilot(const Slice* key) {
    Rep* r = rep_;
    if (r->pilot_builder.get() == nullptr) {
        return;
    }

    assert(!r->children_iter.empty());

    std::vector<uint8_t> levels;
    while (!r->iter_heap->empty()) {
        SeekTableIterator* ptr = r->iter_heap->top();
        if (key == nullptr || r->comparator.Compare(ptr->key(), *key) < 0) {
            ptr->Next();
            if (ptr->Valid()) {
                r->iter_heap->replace_top(ptr);
            } else {
                r->iter_heap->pop();
            }
            uint8_t idx = r->iter_map[ptr];
            levels.push_back(idx);
        } else {
            break;
        }
    }

    if (r->pilot_builder.get()->empty()) {
        r->pilot_builder->AddFirstEntry(levels);
    } else {
        // should use the previous index and data offset
        // since the iterators are now behind key instead of r->last_key
        r->pilot_builder->AddPilotEntry(r->last_key, r->pending_index_block_,
                                        r->pending_data_block_, levels);
    }

    r->pending_index_block_.clear();
    r->pending_data_block_.clear();
    if (!r->iter_heap->empty()) {
        for (size_t i = 0; i < r->children_iter.size(); i++) {
            uint32_t index = 0xFFFFFFFF;
            uint32_t data = 0xFFFFFFFF;
            if (r->children_iter[i]->Valid()) {
                index = r->children_iter[i]->GetIndexBlock();
                data = r->children_iter[i]->GetDataBlock();
            }
            r->pending_index_block_.push_back(index);
            r->pending_data_block_.push_back(data);
        }
    }
}

Status SeekTableBuilder::Finish() {
    Rep* r = rep_;
    assert(r->state != Rep::State::kClosed);
    bool empty_data_block = r->data_block.empty();
    Flush();

    if (ok() && !empty_data_block) {
        r->index_builder->AddIndexEntry(&r->last_key, nullptr, r->pending_handle);
    }

    SeekMetaIndexBuilder meta_index_builder;
    BlockHandle metaindex_block_handle, index_block_handle;

    WriteIndexBlock(&meta_index_builder, &index_block_handle);
    if (rep_->pilot_builder.get() != nullptr) {
        BuildPilot();
        WritePilotBlock(&meta_index_builder);
    }
    // remove it if iterator could check out-of-bound
    meta_index_builder.Add("dummy meta data", index_block_handle);
    WriteRawBlock(meta_index_builder.Finish(), &metaindex_block_handle);
    WriteFooter(metaindex_block_handle, index_block_handle);
    return Status::OK();
}

TableProperties SeekTableBuilder::GetTableProperties() const {
    return TableProperties();
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
    SeekIndexBuilder::IndexBlocks index_blocks;
    Status index_builder_status = rep_->index_builder->Finish(&index_blocks);
    assert(index_builder_status.ok());
    for (const auto& item : index_blocks.meta_blocks) {
        BlockHandle block_handle;
        WriteBlock(item.second, &block_handle, false /* is_data_block */);
        meta_index_builder->Add(item.first, block_handle);
    }
    WriteRawBlock(index_blocks.index_block_contents, index_block_handle);
}

void SeekTableBuilder::WritePilotBlock(
    SeekMetaIndexBuilder* meta_index_builder) {
    Slice pilots = rep_->pilot_builder->Finish();
    BlockHandle block_handle;
    WriteRawBlock(pilots, &block_handle);
    meta_index_builder->Add(kPilotBlock, block_handle);
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

SeekTableBuilder::SeekTableBuilder(
    const Comparator& comparator,
    WritableFileWriter* file,
    SeekTable** lower_levels, int n) {

    rep_ = new Rep(comparator, file, 0, 0, 0, 4096,
                lower_levels, n);
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

    BuildPilot(&key);
    r->last_key.assign(key.data(), key.size());
    r->data_block.Add(key, value);

    r->index_builder->OnKeyAdded(key);

    r->props.num_entries++;
    r->props.raw_key_size += key.size();
    r->props.raw_value_size += value.size();

    // building pilot block
}

} // namespace name rocksdb

