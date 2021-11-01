#include "storage/rowset/segment_v2/read_buffer.h"

namespace starrocks {
namespace segment_v2 {

int ReadBuffer::seek(size_t offset) {
    return -1;
}

int ReadBuffer::next(size_t size) {
    return -1;
}

int ReadBuffer::read(Slice dst) {
    return -1;
}

} // namespace segment_v2
} // namespace starrocks