#include "util/faststring.h"

namespace starrocks {

namespace segment_v2 {

class Memory {
public:
    explicit Memory(size_t capacity) : _buf(capacity) {}

    uint8_t* data() {
        return _buf.data();
    }

    size_t capacity() const { return _buf.capacity(); }

    size_t size() const { return _buf.size(); }

    void resize(size_t size) {
        _buf.resize(size);
    }

    bool empty() { return size() == 0; }

private:
    faststring _buf;
    size_t _pos;
};

} // namespace segment_v2
} // namespace starrocks