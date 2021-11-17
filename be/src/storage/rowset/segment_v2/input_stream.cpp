#include "storage/rowset/segment_v2/input_stream.h"

#include "common/status.h"
#include "util/runtime_profile.h"
#include "gutil/strings/fastmem.h"
#include <fcntl.h>
namespace starrocks {

namespace segment_v2 {

Status InputStream::seek(const PagePointer& pp) {
    return Status::OK();
}

/*
Status InputStream::read(const PagePointer& pp, Slice dst_slice) {
    do {
        if (!_buffer.empty()) {
            // read from buffer
            DCHECK(_buffer.size() > dst_slice.get_size()) << ", buffer size:" << _buffer.size() << ", dst size:" << dst_slice.get_size();
            memcpy(dst_slice.mutable_data(), _buffer.current_pos(), dst_slice.get_size());
            _buffer.next(dst_slice.get_size());
            return Status::OK();
        } else {
            if (_cur_pos == _page_ids.size()) {
                return Status::EndOfFile("end of file");
            }
            LOG(INFO) << "cur pos" << _cur_pos;
            CHECK(_cur_pos < _page_ids.size()) << "cur pos:" << _cur_pos << ", page id size:" << _page_ids.size();
            size_t pos = _cur_pos;
            uint64_t file_offset = _page_pointers[pos].offset;
            uint32_t size = _page_pointers[pos].size;
            while (pos < _page_ids.size() && pos + 1 < _page_ids.size() && _page_ids[pos] + 1 == _page_ids[pos + 1]) {
                if (size + _page_pointers[pos + 1].size > 1024 * 1024) {
                    break;
                }
                size += _page_pointers[pos + 1].size;
                pos = pos + 1;
            }
            _buffer.reset();
            LOG(INFO) << "cur pos" << _cur_pos << ", pos:" << pos << ", size:" << size;
            Slice page_slice(_buffer.current_pos(), size);
            _buffer.resize(size);
            RETURN_IF_ERROR(_rblock->read(file_offset, page_slice));
            _cur_pos = pos + 1;
        }
    } while (true);
    return Status::InternalError("can not reach here");
}
*/

Status InputStream::init() {
    DCHECK(_rblock) << "_rblock is null";
    auto st = _rblock->size(&_file_length);
    return st;
}

Status InputStream::read(const PagePointer& pp, Slice dst_slice) {
    // LOG(INFO) << "read page pointer, offset:" << pp.offset << ", size:" << pp.size;
    SCOPED_RAW_TIMER(&_stats->io_read_from_stream_time);
    _stats->io_read_from_stream_count++;
    if (!_buffer.empty() && pp.offset >= _file_offset && pp.offset + pp.size <= _file_offset + _buffer.size()) {
        // the data is in memory
        DCHECK(pp.size == dst_slice.get_size()) << "pp size:" << pp.size << ", dst size:" << dst_slice.get_size();
        DCHECK(_buffer.size() > dst_slice.get_size())
                << ", buffer size:" << _buffer.size() << ", dst size:" << dst_slice.get_size();
        memcpy(dst_slice.mutable_data(), _buffer.data() + pp.offset - _file_offset, dst_slice.get_size());
        // memcpy_fast_sse(dst_slice.mutable_data(), _buffer.data() + pp.offset - _file_offset, dst_slice.get_size());
        //strings::memcpy_inlined(dst_slice.mutable_data(), _buffer.data() + pp.offset - _file_offset, dst_slice.get_size());
        _stats->io_read_buffered_count++;
    } else {
        // read data from file
        // read 1MB data each time
        DCHECK(pp.size <= _file_length) << "page pointer size:" << pp.size << ", file_length:" << _file_length;
        DCHECK(pp.offset < _file_length) << "page pointer offset:" << pp.offset << ", file_length:" << _file_length;
        size_t length = _buffer.empty() ? pp.size : std::min(_buffer.capacity(), _file_length - pp.offset);
        Slice page_slice(_buffer.data(), length);
        {
            SCOPED_RAW_TIMER(&_stats->io_read_directly_time);
            RETURN_IF_ERROR(_rblock->read(pp.offset, page_slice));

            size_t ahead_offset = pp.offset + length;
            size_t ahead_length = std::min(_buffer.capacity(), _file_length - ahead_offset);
            
            if (ahead_length > 0) {
                SCOPED_RAW_TIMER(&_stats->io_read_ahead_time);
                //RETURN_IF_ERROR(_rblock->read_ahead(ahead_offset, ahead_length));
                RETURN_IF_ERROR(_rblock->fadvise(ahead_offset, ahead_length, POSIX_FADV_SEQUENTIAL));
            }
        }
        _file_offset = pp.offset;
        _buffer.resize(length);
        memcpy(dst_slice.mutable_data(), _buffer.data() + pp.offset - _file_offset, dst_slice.get_size());
        // memcpy_fast_sse(dst_slice.mutable_data(), _buffer.data() + pp.offset - _file_offset, dst_slice.get_size());
        // strings::memcpy_inlined(dst_slice.mutable_data(), _buffer.data(), dst_slice.get_size());
        _stats->io_read_directly_count++;
    }
    return Status::OK();
}


} // namespace segment_v2
} // namespace starrocks