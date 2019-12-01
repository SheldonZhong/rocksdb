#include "table/block_based/seek_table_reader.h"
#include "table/block_based/seek_table_builder.h"

namespace rocksdb
{

struct SeekTable::Rep {
    Rep(const Comparator& _comparator, int _level)
        : comparator(_comparator),
        level(_level) {}
    const Comparator& comparator;
    Status status;
    std::unique_ptr<RandomAccessFileReader> file;

    Footer footer;

    std::unique_ptr<IndexReader> index_reader;
    std::unique_ptr<InternalIterator> pilot_block;

    int level;

};

Status SeekTable::Open(const Comparator& comparator,
                        std::unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        std::unique_ptr<SeekTable>* table_reader,
                        int level) {
    table_reader->reset();

    Status s;
    Footer footer;

    s = ReadFooterFromFile(file.get(), nullptr, file_size, &footer, kSeekTableMagicNumber);
    if (!s.ok()) {
        return s;
    }
    if (footer.version() != 5) {
        return Status::Corruption(
            "Unknown Footer version. Our project should have self defined version of 5."
        );
    }

    // files are opened footer are initialized.
    Rep* rep = new SeekTable::Rep(comparator, level);
    rep->file = std::move(file);
    rep->footer = footer;

    std::unique_ptr<SeekTable> new_table(new SeekTable(rep));

    std::unique_ptr<IndexReader> index_reader;
    s = new_table->CreateIndexReader(&index_reader);

    if (!s.ok()) {
        return s;
    }

    rep->index_reader = std::move(index_reader);

    // read metaindex
    std::unique_ptr<SeekBlock> meta;
    std::unique_ptr<InternalIterator> meta_iter;

    s = new_table->ReadMetaBlock(&meta, &meta_iter);
    if (!s.ok()) {
        return s;
    }

    meta_iter->Seek(kPilotBlock);
    if (meta_iter->Valid() &&
        meta_iter->key().ToString().compare(kPilotBlock) == 0) {
        BlockHandle pilot_handle;

        Slice v = meta_iter->value();
        s = pilot_handle.DecodeFrom(&v);
        if (!s.ok()) {
            return s;
        }

        std::unique_ptr<SeekBlock> pilot_block;
        std::unique_ptr<InternalIterator> pilot_iter;
        s = new_table->ReadPilotBlock(pilot_handle, &pilot_block, &pilot_iter);
        if (!s.ok()) {
            return s;
        }

        rep->pilot_block = std::move(pilot_iter);
    }

    // populate table_properties and some fields

    *table_reader = std::move(new_table);
    return s;
}

// It has a lot of simplification, there might be a memory leak
// ptr allocation is rockdb is still a mist.
Status SeekTable::RetrieveBlock(const BlockHandle& handle, BlockContents* contents) const {
    // slice is on stack, the content in data would be lost after function return
    Slice slice;
    contents->is_raw_block = true;
    Status s;
    size_t n = static_cast<size_t>(handle.size());
    char* buf = new char[n + kBlockTrailerSize];
    s = rep_->file->Read(handle.offset(),
                        n + kBlockTrailerSize,
                        &slice, buf);

    if (!s.ok()) {
        delete[] buf;
        return s;
    }
    if (slice.size() != n + kBlockTrailerSize) {
        delete[] buf;
        return Status::Corruption("block has been truncated.");
    }
    const char* data = slice.data();

    // crc32
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
        delete[] buf;
        return Status::Corruption("block checksum mismatch");
    }

    if (data != buf) {
        delete[] buf;
        contents->data = Slice(data, n);
        // might be leak here
    } else {
        contents->data = Slice(buf, n);
    }

    return s;
}

Status SeekTable::ReadMetaBlock(std::unique_ptr<SeekBlock>* meta_block,
                                std::unique_ptr<InternalIterator>* iter) {
    BlockContents contents;
    Status s = RetrieveBlock(rep_->footer.metaindex_handle(), &contents);

    meta_block->reset(new SeekBlock(std::move(contents)));
    // global one bytewise comparator
    iter->reset(meta_block->get()->NewDataIterator(&rep_->comparator));
    return s;
}

Status SeekTable::ReadPilotBlock(const BlockHandle& handle,
                                std::unique_ptr<SeekBlock>* pilot_block,
                                std::unique_ptr<InternalIterator>* iter) {
    BlockContents contents;
    Status s = RetrieveBlock(handle, &contents);
    pilot_block->reset(new SeekBlock(std::move(contents)));
    iter->reset(pilot_block->get()->NewDataIterator(&rep_->comparator));
    return s;
}

Status SeekTable::CreateIndexReader(std::unique_ptr<IndexReader>* index_reader) {
    return IndexReader::Create(this, index_reader);
}

InternalIterator* SeekTable::NewDataBlockIterator(const BlockHandle& handle,
                                                SeekDataBlockIter* input_iter) const {
    BlockContents contents;
    Status s = RetrieveBlock(handle, &contents);
    if (!s.ok()) {
        return nullptr;
    }
    SeekBlock block(std::move(contents));
    return block.NewDataIterator(&rep_->comparator, input_iter);
}

InternalIterator* SeekTable::NewIterator() const {
    return NewSeekTableIter();
}

SeekTableIterator* SeekTable::NewSeekTableIter() const {
    SeekDataBlockIter* index_iter = NewIndexIterator();
    return new SeekTableIterator(this, rep_->comparator, index_iter);
}

SeekDataBlockIter* SeekTable::NewIndexIterator() const {
    return rep_->index_reader->NewIterator(nullptr);
}

Status SeekTable::IndexReader::ReadIndexBlock(const SeekTable* table,
                                            SeekBlock** index_block) {
    // how to return block from the function? The behavior in rocksdb is hard to understand.
    assert(table != nullptr);
    assert(index_block);
    const Rep* const rep = table->get_rep();
    assert(rep != nullptr);
    BlockContents contents;
    const Status s = table->RetrieveBlock(rep->footer.index_handle(), &contents);
    *index_block = new SeekBlock(std::move(contents));

    return s;
}

Status SeekTable::IndexReader::Create(
                                SeekTable* table,
                                std::unique_ptr<SeekTable::IndexReader>* index_reader) {
    assert(table);
    assert(table->get_rep());
    assert(index_reader);
    SeekBlock* index_block;
    const Status s = ReadIndexBlock(table, &index_block);

    if (!s.ok()) {
        return s;
    }

    index_reader->reset(new IndexReader(table, index_block));

    return Status::OK();
}

SeekDataBlockIter* SeekTable::IndexReader::NewIterator(SeekDataBlockIter* iter) {
    return index_block_.get()->NewDataIterator(&table_->rep_->comparator, iter);
}

void SeekTableIterator::Seek(const Slice& target) {
    SeekImpl(&target);
}

void SeekTableIterator::SeekImpl(const Slice* target) {
    bool seek_index = true;
    if (block_iter_.Valid()) {
        if (target) {
            if (comp_.Compare(*target,
                                block_iter_.key()) > 0 &&
                comp_.Compare(*target,
                                index_iter_->key()) < 0) {
                seek_index = false;
            }
        }
    }

    if (seek_index) {
        if (target) {
            index_iter_->Seek(*target);
        } else {
            index_iter_->SeekToFirst();
        }

        if (!index_iter_->Valid()) {
            return;
        }
    }
    InitDataBlock();

    if (target) {
        block_iter_.Seek(*target);
    } else {
        block_iter_.SeekToFirst();
    }
}

void SeekTableIterator::InitDataBlock() {
    Slice index_value = index_iter_->value();
    IndexValue v;
    v.DecodeFrom(&index_value, false, nullptr);
    table_->NewDataBlockIterator(v.handle, &block_iter_);
    block_iter_points_to_real_block_ = true;
}

void SeekTableIterator::SeekToFirst() {
    SeekImpl(nullptr);
}

void SeekTableIterator::Next() {
    assert(block_iter_points_to_real_block_);
    block_iter_.Next();
    if (!block_iter_.Valid()) {
        index_iter_->Next();
        if (index_iter_->Valid()) {
            InitDataBlock();
            block_iter_.SeekToFirst();
        }
    }
    if (!index_iter_->Valid()) {
        return;
    }
}

bool SeekTableIterator::NextAndGetResult(IterateResult* result) {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
        result->key = key();
        result->may_be_out_of_upper_bound = MayBeOutOfUpperBound();
    }
    return is_valid;
}

bool SeekTableIterator::Valid() const {
    return (block_iter_points_to_real_block_ && block_iter_.Valid());
}

Slice SeekTableIterator::key() const {
    return block_iter_.key();
}

Slice SeekTableIterator::value() const {
    assert(Valid());
    return block_iter_.value();
}

Status SeekTableIterator::status() const {
    if (!index_iter_->status().ok()) {
        return index_iter_->status();
    } else if (block_iter_points_to_real_block_) {
        return block_iter_.status();
    } else {
        return Status::OK();
    }
}

uint32_t SeekTableIterator::GetIndexBlock() const {
    return index_iter_->GetRestartIndex();
}

uint32_t SeekTableIterator::GetDataBlock() const {
    return block_iter_.GetRestartIndex();
}

} // namespace rocksdb
