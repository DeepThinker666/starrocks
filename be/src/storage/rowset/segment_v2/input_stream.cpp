#include "storage/rowset/segment_v2/input_stream.h"

#include "common/status.h"

namespace starrocks {

namespace segment_v2 {

Status InputStream::init(vectorized::SparseRangeIterator range_iter, OrdinalPageIndexIterator ord_iter) {
    // 获取要读取的page id列表
    while (range_iter.has_more()) {
        vectorized::Range range = range_iter.next();
        LOG(INFO) << "range:" << range.to_string();
        while (ord_iter.valid()) {
            vectorized::Range page_range(ord_iter.first_ordinal(), ord_iter.last_ordinal() + 1);
            LOG(INFO) << "page range:" << page_range.to_string();
            if (range.has_intersection(page_range)) {
                LOG(INFO) << "add page id:" << ord_iter.page_index() << ", pp:" << ord_iter.page().to_string();
                _page_ids.push_back(ord_iter.page_index());
                _page_pointers.push_back(ord_iter.page());
                /*
            PageInfo page_info;
            page_info.id = ord_iter.page_index();
            page_info.index = _page_ids.size() - 1;
            _pp_to_id[ord_iter.page] = page_info;
            */
            } else {
                break;
            }
            ord_iter.next();
        }
        
    }
    _cur_pos = 0;
    LOG(INFO) << "init input stream. cur pos:" << _cur_pos << ", page id size:" << _page_ids.size();
    std::stringstream ss;
    for (int i = 0; i < _page_ids.size(); ++i) {
        ss << "i:" << i << ", page id:" << _page_ids[i] << ",";
    }
    LOG(INFO) << "page id list:" << ss.str();

    // 从page pointer能够拿到连续的后面连续的范围
    _is_range = true;
    return Status::OK();
}

Status InputStream::seek(PagePointer pp) {
    return Status::OK();
}

Status InputStream::read(Slice dst_slice) {
    do {
        if (!_buffer.empty()) {
            // read from buffer
            CHECK(_buffer.size() > dst_slice.get_size()) << ", buffer size:" << _buffer.size() << ", dst size:" << dst_slice.get_size();
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

} // namespace segment_v2
} // namespace starrocks