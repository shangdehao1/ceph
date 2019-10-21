// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_PROMOTE_THROTTLE
#define CEPH_CACHE_PROMOTE_THROTTLE

#include <sys/time.h>
#include <mutex>

#include "common/ceph_mutex.h"
#include "include/ceph_assert.h"

namespace ceph {
namespace immutable_obj_cache {

class PromoteThrottle {
 public:
  PromoteThrottle(uint64_t limiter) 
   : m_object_limiter(limiter), m_out_object(0), m_in_object(0) {
    m_last_time_tv = get_curr_tv();
  }

  ~PromoteThrottle() {
    ceph_assert(m_out_object == m_in_object);
  }

  bool take_object(uint64_t object) {
    double out_ops = calculate_instant_speed(object, m_last_out_time_tv);
    if ((out_ops + m_in_ops) > m_object_limiter) {
      return false;
    }

    m_out_object += object;
    m_out_ops = out_ops;
    m_last_out_time_tv = get_curr_tv();

    return true;
  }

  void put_object(uint64_t object) {
    m_in_object += object;
    m_in_ops = calculate_instant_speed(object, m_last_in_time_tv);
    m_last_in_time_tv = get_curr_tv();
  }

 private:

  double calculate_instant_speed(uint64_t object,
                                 struct timeval last_time_tv) {
    uint64_t diff;
    double ops;
      
    diff = diff_tv(last_time_tv, get_curr_tv());
    ceph_assert(diff != 0);
    ops = ((double)(object) / (double)diff)*1000000;

    return ops;
  }

  uint64_t diff_tv(struct timeval starttv, struct timeval endtv) {
    struct timeval result;
    uint64_t diff;

    if (endtv.tv_sec < starttv.tv_sec) {
      return 0;
    }

    if (endtv.tv_sec == starttv.tv_sec && endtv.tv_usec < starttv.tv_usec) {
      return 0;
    }

    result.tv_sec = endtv.tv_sec - starttv.tv_sec;
    result.tv_usec = endtv.tv_usec - starttv.tv_usec;

    if (result.tv_usec < 0) {
      result.tv_sec--;
      result.tv_usec += 1000000;
    }

    diff = (uint64_t)(result.tv_sec) * 1000000 + (uint64_t)(result.tv_usec);

    return diff;
  }

  struct timeval get_curr_tv() {
    struct timeval tv;
    struct timezone tz;
    if (gettimeofday(&tv, &tz) != 0) {
      ceph_assert(false);
    }
    return tv;
  }

 private:
  ceph::mutex m_lock = ceph::make_mutex("ceph::cache::PromoteThrottle");
  struct timeval m_last_time_tv;
  uint64_t m_object_limiter;
  uint64_t m_out_object;
  struct timeval m_last_out_time_tv;
  uint64_t m_out_ops;
  uint64_t m_in_object;
  struct timeval m_last_in_time_tv;
  uint64_t m_in_ops;
};

}  // namespace immutable_obj_cache
}  // namespace ceph

#endif  // CEPH_CACHE_PROMOTE_THROTTLE 
