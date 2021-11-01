#include "storage/rowset/segment_v2/memory.h"

#include "common/logging.h"
#include "util/debug_util.h"

namespace starrocks {

namespace segment_v2 {

class Buffer {
public:
    explicit Buffer(size_t capacity) : _memory(capacity) { reset(); }
    Buffer() : _memory(1024 * 1024) { reset(); }

    uint8_t* current_pos() { return _pos; }

    void next(size_t size) {
        CHECK(size <= _end - _pos);
        _pos = _pos + size;
    }

    void resize(size_t size) {
         _end = _start + size;
         _memory.resize(size);
    }

    bool empty() { return _pos < _end; }

    void reset() {
        _start = _memory.data();
        _end = _memory.data() + _memory.capacity();
        _pos = _start;
        LOG(INFO) << "reset buffer. size:" << size() << ", _pos:" << hexdump((char*)&_pos, sizeof(uint8_t*)) << ", memory capacity:" << _memory.capacity();
    }

    uint32_t size() {
        return _end - _start;
    }

private:
    Memory _memory;
    uint8_t* _start;
    uint8_t* _end;
    uint8_t* _pos;
};

} // namespace segment_v2
} // namespace starrocks