#pragma once

#include "storage/rowset/segment_v2/buffer.h"

namespace starrocks {

namespace segment_v2 {

class ReadBuffer {
public:
    ReadBuffer(size_t capacity) : _buffer(capacity){};
    ~ReadBuffer() = default;

    int next(size_t size);

    int seek(size_t offset);

    int read(Slice dst);

    uint32_t size() {
        
    }

private:
    Buffer _buffer;
    size_t _file_offset;
};

} // namespace segment_v2

} // namespace starrocks