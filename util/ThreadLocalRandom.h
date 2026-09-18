
#ifndef LSMGRAPH_THREADLOCALRANDOM_H
#define LSMGRAPH_THREADLOCALRANDOM_H

#include <cstdint>
#include <thread>

class XORSHF32 {
private:
  uint32_t x_, y_, z_;

public:
  XORSHF32()
          : x_(123)
          , y_(456)
          , z_(789) {}

  // thread local
  XORSHF32(uint32_t x, uint32_t y, uint32_t z)
          : x_(x)
          , y_(y)
          , z_(z) {}

  uint32_t gen() {
    uint32_t t;

    x_ ^= x_ << 13;
    x_ ^= x_ >> 17;
    x_ ^= x_ << 5;

    t  = x_;
    x_ = y_;
    y_ = z_;
    z_ = t ^ x_ ^ y_;

    return z_;
  }
};

class ThreadLocalRandom {
private:
  thread_local static XORSHF32 generator;

public:
  static XORSHF32 &get_generator() {
    return generator;
  }
};

#endif //LSMGRAPH_THREADLOCALRANDOM_H
