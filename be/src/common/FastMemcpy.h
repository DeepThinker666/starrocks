#pragma once

#include <stddef.h>
#include <stdint.h>

void* memcpy_fast_sse(void * __restrict destination, const void * __restrict source, size_t size);