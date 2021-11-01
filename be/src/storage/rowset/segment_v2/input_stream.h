
#pragma once

#include <memory>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/rowset/segment_v2/ordinal_page_index.h"
#include "storage/rowset/segment_v2/page_pointer.h"
#include "storage/vectorized/range.h"
#include "storage/rowset/segment_v2/buffer.h"
#include "storage/fs/block_manager.h"
#include "storage/vectorized/range.h"

namespace starrocks {

namespace segment_v2 {

// 需要实现大块读取数据
// 需要实现cache封装和透明
// 目前默认顺序访问数据块，不能进行seek
class InputStream {
public:
    InputStream(fs::ReadableBlock* rblock) : _buffer(1024 * 1024), _rblock(rblock) { }

    // void init(PagePointer pp);

    Status init(vectorized::SparseRangeIterator range_iter, OrdinalPageIndexIterator ord_iter);

    Status seek(PagePointer pp);

    // Slice has size to read, and the dest buffer to store data
    Status read(Slice dst_slice);

private:
    std::vector<uint32_t> _page_ids;
    std::vector<PagePointer> _page_pointers;
    bool _is_range = false;
    Buffer _buffer;
    size_t _cur_pos;
    fs::ReadableBlock* _rblock;

    /*
    Struct PageInfo {
        uint32_t id;
        uint32_t index;
    }
    // page pointer to page id
    std::map<PagePointer, PageInfo> _pp_to_id;
    */
};

} // namespace segment_v2
} // namespace starrocks