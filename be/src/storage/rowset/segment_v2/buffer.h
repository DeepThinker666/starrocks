#pragma once

#include "storage/rowset/segment_v2/memory.h"

#include "common/logging.h"
#include "util/debug_util.h"

namespace starrocks {

namespace segment_v2 {

class Buffer {
public:
    explicit Buffer(size_t capacity) : _capacity(capacity) {
        _memory = std::make_unique<uint8_t[]>(capacity);
        reset();
    }
    Buffer() : _capacity(1024 * 1024) {
        _memory = std::make_unique<uint8_t[]>(1024 * 1024);
        reset();
    }

    uint8_t* current_pos() { return _pos; }

    uint8_t* data() {
        return _memory.get();
    }

    size_t capacity() const { return _capacity; } 

    /*
    void next(size_t size) {
        CHECK(size <= _end - _pos);
        _pos = _pos + size;
    }
    */

    void resize(size_t size) {
         _end = _start + size;
    }

    bool empty() { return _start >= _end; }

    void reset() {
        _start = _memory.get();
        _end = _memory.get();
        _pos = _memory.get();
    }

    uint32_t size() {
        return _end - _start;
    }

private:
    // Memory _memory;
    std::unique_ptr<uint8_t[]> _memory;
    size_t _capacity;
    uint8_t* _start;
    uint8_t* _end;
    uint8_t* _pos;
};

} // namespace segment_v2
} // namespace starrocks