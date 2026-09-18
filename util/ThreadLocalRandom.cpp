#include "ThreadLocalRandom.h"
thread_local XORSHF32 ThreadLocalRandom::generator(std::hash<std::thread::id>{}(std::this_thread::get_id()),
                                                   0xFFFFFFF1, 0xFFFFFFF5);
