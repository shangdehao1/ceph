#ifndef RWL_SYNC_POINT_H
#define RWL_SYNC_POINT_H

#include "LogEntry.h"

namespace librbd {
namespace cache {
namespace rwl {


template <typename T>
class SyncPoint
{
public:
  SyncPoint(T &rwl, const uint64_t sync_gen_num)
    : rwl(rwl), log_entry(std::make_shared<SyncPointLogEntry>(sync_gen_num)) 
  {
    m_prior_log_entries_persisted = new C_Gather(rwl.m_image_ctx.cct, nullptr);
    m_sync_point_persist = new C_Gather(rwl.m_image_ctx.cct, nullptr);

    /* When set MAX_WRITES_PER_SYNC_POINT, what's factor need to be considered ? */
    m_on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
    m_on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  }

  ~SyncPoint()
  {
    ceph_assert(m_on_sync_point_appending.empty());
    ceph_assert(m_on_sync_point_persisted.empty());
    ceph_assert(!earlier_sync_point);
  }

public:
  T &rwl;

  std::shared_ptr<SyncPointLogEntry> log_entry;

  // link   : rwl::new_sync_point
  // cancel : SyncPointLogOperation::complete
  std::shared_ptr<SyncPoint<T>> earlier_sync_point;

  // link   : rwl.new_sync_point
  // remove : none
  std::shared_ptr<SyncPoint<T>> later_sync_point;

  uint64_t m_final_op_sequence_num = 0;

 /* A sync point can't appear in the log until all the writes bearing
  * it and all the prior sync points have been appended and persisted.
  *
  * Writes bearing this sync gen number and the prior sync point will be
  * sub-ops of this Gather.
  *
  * This sync point will not be appended until all these complete to the point
  * where their persist order is guaranteed. */
  C_Gather *m_prior_log_entries_persisted;

  int m_prior_log_entries_persisted_result = 0;
  int m_prior_log_entries_persisted_complete = false;

 /* The finisher for this will append the sync point to the log.
  * The finisher for m_prior_log_entries_persisted will be a sub-op of this. */
  C_Gather *m_sync_point_persist;

  bool m_append_scheduled = false;
  bool m_appending = false;

 /* Signal these when this sync point is appending to the log, and its order of appearance is guaranteed.
  * One of these is is a sub-operation of the next sync point's m_prior_log_entries_persisted Gather. */
  std::vector<Context*> m_on_sync_point_appending;

 /* Signal these when this sync point is appended and persisted.
  * User aio_flush() calls are added to this. */
  std::vector<Context*> m_on_sync_point_persisted;

  SyncPoint(const SyncPoint&) = delete;
  SyncPoint &operator=(const SyncPoint&) = delete;

  std::ostream &format(std::ostream &os) const
  {
    os << "log_entry=[" << *log_entry << "], "
       << "earlier_sync_point=" << earlier_sync_point << ", "
       << "later_sync_point=" << later_sync_point << ", "
       << "m_final_op_sequence_num=" << m_final_op_sequence_num << ", "
       << "m_prior_log_entries_persisted=" << m_prior_log_entries_persisted << ", "
       << "m_prior_log_entries_persisted_complete=" << m_prior_log_entries_persisted_complete << ", "
       << "m_append_scheduled=" << m_append_scheduled << ", "
       << "m_appending=" << m_appending << ", "
       << "m_on_sync_point_appending=" << m_on_sync_point_appending.size() << ", "
       << "m_on_sync_point_persisted=" << m_on_sync_point_persisted.size() << "";
    return os;
  } 

  friend std::ostream &operator<<(std::ostream &os, const SyncPoint &p) {
    return p.format(os);
  }
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif
