#ifndef RWL_REQUEST_H
#define RWL_REQUEST_H

#include "Extent.h"
#include "Resource.h"
#include "Config.h"

namespace librbd {
namespace cache {

namespace rwl {

/* A request that can be deferred in a BlockGuard to sequence overlapping operations. */
template <typename T>
struct C_GuardedBlockIORequest : public SharedPtrContext
{
private:
  std::atomic<bool> m_cell_released = {false};
  BlockGuardCell* m_cell = nullptr;

public:
  T &rwl;

  C_GuardedBlockIORequest(T &rwl) : rwl(rwl) {}

  ~C_GuardedBlockIORequest() {
    ceph_assert(m_cell_released || !m_cell);
  }

  C_GuardedBlockIORequest(const C_GuardedBlockIORequest&) = delete;
  C_GuardedBlockIORequest &operator=(const C_GuardedBlockIORequest&) = delete;

  auto shared_from_this() { return shared_from(this); }

  virtual const char *get_name() const = 0;

  void set_cell(BlockGuardCell *cell) {
    ceph_assert(cell);
    ceph_assert(!m_cell);
    m_cell = cell;
  }

  BlockGuardCell *get_cell(void) {
    return m_cell;
  }

  void release_cell()
  {
    ceph_assert(m_cell);
    bool initial = false;

    if (m_cell_released.compare_exchange_strong(initial, true)) {
      rwl.release_guarded_request(m_cell); 
    } else {
      ldout(rwl.m_image_ctx.cct, 5) << "cell " << m_cell << " already released for " << this << dendl;
    }
  }
};

} // namespace rwl

struct C_ReadRequest : public Context
{
  CephContext *m_cct;
  Context *m_on_finish;

  Extents m_miss_extents;

  ImageExtentBufs m_read_extents;

  bufferlist m_miss_bl;
  bufferlist* m_out_bl;

  utime_t m_arrived_time;
  PerfCounters *m_perfcounter;

  C_ReadRequest(CephContext *cct, utime_t arrived,
                PerfCounters *perfcounter, bufferlist *out_bl,
                Context *on_finish)
    : m_cct(cct), m_on_finish(on_finish),
      m_out_bl(out_bl), m_arrived_time(arrived),
      m_perfcounter(perfcounter)
  {}

  ~C_ReadRequest() {}

  virtual void finish(int r) override
  {
    int hits = 0;
    int misses = 0;
    int hit_bytes = 0;
    int miss_bytes = 0;

    /* At this point the miss read has completed. We'll iterate through
     * m_read_extents and produce *m_out_bl by assembling pieces of m_miss_bl
     * and the individual hit extent bufs in the read extents that represent hits.*/
    if (r >= 0)
    {
      uint64_t miss_bl_offset = 0;

      for (auto &extent : m_read_extents)
      {
        if (extent.m_bl.length())
        {
          /* This was a hit */
          ceph_assert(extent.second == extent.m_bl.length());
          ++hits;
          hit_bytes += extent.second;
          m_out_bl->claim_append(extent.m_bl);
        }
        else
        {
          /* This was a miss. */
          ++misses;
          miss_bytes += extent.second;
          bufferlist miss_extent_bl;
          miss_extent_bl.substr_of(m_miss_bl, miss_bl_offset, extent.second);
          /* Add this read miss bufferlist to the output bufferlist */
          m_out_bl->claim_append(miss_extent_bl);
          /* Consume these bytes in the read miss bufferlist */
          miss_bl_offset += extent.second;
        }
      }
    }

    utime_t now = ceph_clock_now();

    ceph_assert((int)m_out_bl->length() == hit_bytes + miss_bytes);

    m_on_finish->complete(r);

    m_perfcounter->inc(l_librbd_rwl_rd_bytes, hit_bytes + miss_bytes);
    m_perfcounter->inc(l_librbd_rwl_rd_hit_bytes, hit_bytes);
    m_perfcounter->tinc(l_librbd_rwl_rd_latency, now - m_arrived_time);

    if (!misses) {
      m_perfcounter->inc(l_librbd_rwl_rd_hit_req, 1);
      m_perfcounter->tinc(l_librbd_rwl_rd_hit_latency, now - m_arrived_time);
    } else {
      if (hits) {
        m_perfcounter->inc(l_librbd_rwl_rd_part_hit_req, 1);
      }
    }
  }

  virtual const char *get_name() const {
    return "C_ReadRequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this IO, and the
 * state information about the progress of this IO.
 *
 * This object lives until the IO is persisted in all (live) log replicas.
 *
 * User request may be completed from here before the IO persists.
 */
template <typename T>
struct C_BlockIORequest : public C_GuardedBlockIORequest<T>
{
  using C_GuardedBlockIORequest<T>::rwl;

  int fadvise_flags;
  Extents m_image_extents;
  ExtentsSummary<Extents> m_image_extents_summary;

  bufferlist bl;     /* User data derived from aio_write interface */
  Context* user_req; /* User callback derived from aio_write interface */

  // guarantee user callback just be called one time.
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_finish_called = {false};

  utime_t m_arrived_time;
  utime_t m_allocated_time;    
  utime_t m_dispatched_time;   
  utime_t m_user_req_completed_time;

  /* Detained in blockguard (overlapped with a prior IO) */
  bool m_detained = false;
  /* Deferred because this or a prior IO had to wait for write resources */
  std::atomic<bool> m_deferred = {false};
  /* This IO waited for free persist/replicate lanes */
  bool m_waited_lanes = false;
  /* This IO waited for free log entries */
  bool m_waited_entries = false;
  /* This IO waited for data buffers (pmemobj_reserve() failed) */
  bool m_waited_buffers = false;

  friend std::ostream &operator<<(std::ostream &os, const C_BlockIORequest<T> &req)
  {
    os << "m_image_extents=[" << req.m_image_extents << "], "
       << "m_image_extents_summary=[" << req.m_image_extents_summary << "], "
       << "bl=" << req.bl << ", " << "user_req=" << req.user_req << ", "
       << "m_user_req_completed=" << req.m_user_req_completed << ", "
       << "deferred=" << req.m_deferred << ", " << "detained=" << req.m_detained << ", "
       << "m_waited_lanes=" << req.m_waited_lanes << ", " << "m_waited_entries=" << req.m_waited_entries << ", "
       << "m_waited_buffers=" << req.m_waited_buffers << "";
    return os;
  };

  C_BlockIORequest(T &rwl, const utime_t arrived, Extents&& image_extents,
                   bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_GuardedBlockIORequest<T>(rwl),
      fadvise_flags(fadvise_flags),
      m_image_extents(std::move(image_extents)),
      m_image_extents_summary(m_image_extents),
      bl(std::move(bl)),
      user_req(user_req),
      m_arrived_time(arrived)
  {
    /* Remove zero length image extents from input */
    for (auto it = m_image_extents.begin(); it != m_image_extents.end(); ) {
      if (0 == it->second) {
        it = m_image_extents.erase(it);
        continue;
      }
      ++it;
    }
  }

  virtual ~C_BlockIORequest() {}

  auto shared_from_this() {
    return shared_from(this);
  }

  // there are two ways to execute this method, but it just can be executed one time
  void complete_user_request(int r)
  {
    bool initial = false;

    // if m_uer_req_completed == initial, return true and set m_user_req_completed to be true.
    // if m_usr_req_completed != initial, return false, and set initial to be m_user_req_completed.
    if (m_user_req_completed.compare_exchange_strong(initial, true)) {
      m_user_req_completed_time = ceph_clock_now();
      user_req->complete(r);
    } else {
      if (RWL_VERBOSE_LOGGING) {
        ldout(rwl.m_image_ctx.cct, 20) << this << " user req already completed" << dendl;
      }
    }
  }

  //  complete for user, then release block guard
  void finish(int r)
  {
    // try to execute it.  If it have been executed, return.
    complete_user_request(r); // ##

    bool initial = false;

    if (m_finish_called.compare_exchange_strong(initial, true)) {
      // child-class implements
      finish_req(0);
    } else {
      ceph_assert(0);
    }
  }

  void deferred() {
    bool initial = false;
    if (m_deferred.compare_exchange_strong(initial, true)) {
      deferred_handler();
    }
  }

  virtual void finish_req(int r) = 0;
  virtual bool alloc_resources() = 0;
  virtual void deferred_handler() = 0;
  virtual void dispatch() = 0;

  virtual const char *get_name() const override {
    return "C_BlockIORequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this write.
 *
 * Block guard is not released until the write persists everywhere (this is
 * how we guarantee to each log replica that they will never see overlapping writes).
 */
template <typename T>
struct C_WriteRequest : public C_BlockIORequest<T>
{
  using C_BlockIORequest<T>::rwl;

  WriteRequestResources m_resources;

  unique_ptr<WriteLogOperationSet<T>> m_op_set = nullptr;

  bool m_do_early_flush = false; // ##

  std::atomic<int> m_appended = {0};
  bool m_queued = false;

  friend std::ostream &operator<<(std::ostream &os, const C_WriteRequest<T> &req)
  {
    os << (C_BlockIORequest<T>&)req << " m_resources.allocated=" << req.m_resources.allocated;
    if (req.m_op_set) {
       os << "m_op_set=" << *req.m_op_set;
    }
    return os;
  };

  C_WriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
                 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {}

  ~C_WriteRequest() {}

  template <typename... U>
  static inline std::shared_ptr<C_WriteRequest<T>> create(U&&... arg) {
    return SharedPtrContext::create<C_WriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
    ceph_assert(guard_ctx.m_cell);
    this->m_detained = guard_ctx.m_state.detained; /* overlapped */
    this->m_queued = guard_ctx.m_state.queued; /* queued behind at least one barrier */
    this->set_cell(guard_ctx.m_cell);
  }

  /* Common finish to plain write and compare-and-write (if it writes) */
  virtual void finish_req(int r) {
    /* Completed to caller by here (in finish(), which calls this) */
    utime_t now = ceph_clock_now();

    rwl.release_write_lanes(this);

    /* TODO: Consider doing this in appending state */
    this->release_cell(); // ##

    update_req_stats(now);
  }

  /* Compare and write will override this */
  virtual void update_req_stats(utime_t &now) {
    for (auto &allocation : this->m_resources.buffers) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_log_op_alloc_t, allocation.allocation_lat);
      rwl.m_perfcounter->hinc(l_librbd_rwl_log_op_alloc_t_hist, allocation.allocation_lat.to_nsec(), allocation.allocation_size);
    }

    if (this->m_detained) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
    }
    if (this->m_queued) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_queued, 1);
    }
    if (this->m_deferred) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def, 1);
    }
    if (this->m_waited_lanes) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_lanes, 1);
    }
    if (this->m_waited_entries) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_log, 1);
    }
    if (this->m_waited_buffers) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_buf, 1);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
    utime_t comp_latency = now - this->m_arrived_time;
  
    if (!(this->m_waited_entries || this->m_waited_buffers || this->m_deferred)) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_latency, comp_latency);
      rwl.m_perfcounter->hinc(l_librbd_rwl_nowait_wr_latency_hist, comp_latency.to_nsec(), this->m_image_extents_summary.total_bytes);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_caller_latency, this->m_user_req_completed_time - this->m_arrived_time);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_latency, comp_latency);
    rwl.m_perfcounter->hinc(l_librbd_rwl_wr_latency_hist, comp_latency.to_nsec(), this->m_image_extents_summary.total_bytes);
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_caller_latency, this->m_user_req_completed_time - this->m_arrived_time);
  }

  virtual bool alloc_resources() override;

  /* Plain writes will allocate one buffer per request extent */
  virtual void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied)
  {
    ldout(rwl.m_image_ctx.cct, 20) << dendl;

    for (auto &extent : this->m_image_extents)
    {
      // nice code, avoid copy costs.
      m_resources.buffers.emplace_back();
      struct WriteBufferAllocation& buffer = m_resources.buffers.back();

      buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
      buffer.allocated = false;
      bytes_cached += extent.second;

      if (extent.second > buffer.allocation_size) {
        buffer.allocation_size = extent.second;
      }
    }

    bytes_dirtied = bytes_cached;
  }

  void deferred_handler() override {}

  void dispatch() override;

  /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
  virtual void setup_log_operations()
  {
    ldout(rwl.m_image_ctx.cct, 20) << dendl;

    for (auto &extent : this->m_image_extents)
    {
      auto operation = std::make_shared<WriteLogOperation<T>>(*m_op_set, extent.first, extent.second);
      m_op_set->operations.emplace_back(operation);
    }
  }

  virtual void schedule_append() {

    ldout(rwl.m_image_ctx.cct, 20) << dendl;
    ceph_assert(++m_appended == 1);

    /* This caller is waiting for persist, so we'll use their thread to expedite it */
    if (m_do_early_flush)
    {
      ldout(rwl.m_image_ctx.cct, 20) << "m_do_early_flush = " << m_do_early_flush << " , then directly flush pmem and schedule append."<<dendl;
      rwl.flush_pmem_buffer(this->m_op_set->operations);
      rwl.schedule_append(this->m_op_set->operations);
    }
    else
    {
      ldout(rwl.m_image_ctx.cct, 20) << "m_do_early_flush = " << m_do_early_flush << " , then schedule flush and append."<<dendl;
      /* This is probably not still the caller's thread,
         so do the payload flushing/replicating later. */
      rwl.schedule_flush_and_append(this->m_op_set->operations); // ##
    }
  }

  const char *get_name() const override {
    return "C_WriteRequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this aio_flush.
 *
 * Block guard is released as soon as the new sync point (if required) is created.
 *
 * Subsequent IOs can proceed while this flush waits for prio IOs to complete
 * and any required sync points to be persisted.
 */
template <typename T>
struct C_FlushRequest : public C_BlockIORequest<T>
{
  using C_BlockIORequest<T>::rwl;

  std::atomic<bool> m_log_entry_allocated = {false};

  bool m_internal = false;

  std::shared_ptr<SyncPoint<T>> to_append;

  std::shared_ptr<SyncPointLogOperation<T>> op;

  friend std::ostream &operator<<(std::ostream &os, const C_FlushRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req << " m_log_entry_allocated=" << req.m_log_entry_allocated;
    return os;
  };

  C_FlushRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
                 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {}

  ~C_FlushRequest() {}

  template <typename... U>
  static inline std::shared_ptr<C_FlushRequest<T>> create(U&&... arg) {
    return SharedPtrContext::create<C_FlushRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {
    /* Block guard already released */
    ceph_assert(!this->get_cell());

    /* Completed to caller by here */
    utime_t now = ceph_clock_now();
    rwl.m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - this->m_arrived_time);
  }

  bool alloc_resources() override {
    CephContext *cct = rwl.m_image_ctx.cct;
    ldout(cct, 20) << dendl;
    ceph_assert(!m_log_entry_allocated);
  
    bool allocated_here = false;
  
    Mutex::Locker locker(rwl.m_lock);
  
    if (rwl.m_free_log_entries) {
      rwl.m_free_log_entries--;
      m_log_entry_allocated = true;
      allocated_here = true;
    }
  
    return allocated_here;
  }

  void deferred_handler() override {
    rwl.m_perfcounter->inc(l_librbd_rwl_aio_flush_def, 1);
  }

  void dispatch() override {
    CephContext *cct = rwl.m_image_ctx.cct;
    ldout(cct, 20) << dendl;

    utime_t now = ceph_clock_now();
    ceph_assert(m_log_entry_allocated);
    this->m_dispatched_time = now;

    op = std::make_shared<SyncPointLogOperation<T>>(rwl, to_append, now);

    rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);

    rwl.schedule_append(op);
  }

  const char *get_name() const override {
    return "C_FlushRequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this discard. As in the
 * case of write, the block guard is not released until the discard persists
 * everywhere.
 */
template <typename T>
struct C_DiscardRequest : public C_BlockIORequest<T>
{
  using C_BlockIORequest<T>::rwl;

  std::atomic<bool> m_log_entry_allocated = {false};
  bool m_skip_partial_discard;
  std::shared_ptr<DiscardLogOperation<T>> op;

  friend std::ostream &operator<<(std::ostream &os, const C_DiscardRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req << "m_skip_partial_discard=" << req.m_skip_partial_discard;

    if (req.op) {
      os << "op=[" << *req.op << "]";
    } else {
      os << "op=nullptr";
    }
    return os;
  };

  C_DiscardRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
                   const int skip_partial_discard, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), bufferlist(), 0, user_req),
      m_skip_partial_discard(skip_partial_discard)
  {}

  ~C_DiscardRequest() {}

  template <typename... U>
  static inline std::shared_ptr<C_DiscardRequest<T>> create(U&&... arg) {
    return SharedPtrContext::create<C_DiscardRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {}

  bool alloc_resources() override {
    CephContext *cct = rwl.m_image_ctx.cct;
    ldout(cct, 20) << dendl;
  
    ceph_assert(!m_log_entry_allocated);
  
    bool allocated_here = false;
  
    Mutex::Locker locker(rwl.m_lock);
  
   /* No bytes are allocated for a discard, but we count the discarded bytes
    * as dirty.  This means it's possible to have more bytes dirty than
    * there are bytes cached or allocated. */
    if (rwl.m_free_log_entries) {
      rwl.m_free_log_entries--;
      rwl.m_bytes_dirty += op->log_entry->bytes_dirty();
      m_log_entry_allocated = true;
      allocated_here = true;
    }
  
    return allocated_here;
  }

  void deferred_handler() override {}

  void dispatch() override {
    utime_t now = ceph_clock_now();
    ceph_assert(m_log_entry_allocated);
    this->m_dispatched_time = now;
  
    rwl.m_blocks_to_log_entries.add_log_entry(op->log_entry); // ##
  
    rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
  
    rwl.schedule_append(op);
  }

  const char *get_name() const override {
    return "C_DiscardRequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this compare and write. The
 * block guard is acquired before the read begins to guarantee atomicity of this
 * operation.  If this results in a write, the block guard will be released
 * when the write completes to all replicas.
 */
template <typename T>
struct C_CompAndWriteRequest : public C_WriteRequest<T>
{
  using C_BlockIORequest<T>::rwl;

  bool m_compare_succeeded = false;
  uint64_t *m_mismatch_offset;
  bufferlist m_cmp_bl;
  bufferlist m_read_bl;

  friend std::ostream &operator<<(std::ostream &os, const C_CompAndWriteRequest<T> &req) {
    os << (C_WriteRequest<T>&)req << "m_cmp_bl=" << req.m_cmp_bl << ", "
       << "m_read_bl=" << req.m_read_bl << ", " << "m_compare_succeeded=" << req.m_compare_succeeded << ", "
       << "m_mismatch_offset=" << req.m_mismatch_offset;
    return os;
  };

  C_CompAndWriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
                        bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
                        int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
    m_mismatch_offset(mismatch_offset), m_cmp_bl(std::move(cmp_bl)) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_CompAndWriteRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_CompAndWriteRequest<T>> create(U&&... arg) {
    return SharedPtrContext::create<C_CompAndWriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) override {
    if (m_compare_succeeded) {
      C_WriteRequest<T>::finish_req(r);
    } else {
      utime_t now = ceph_clock_now();
      update_req_stats(now);
    }
  }

  void update_req_stats(utime_t &now) override {
    /* Compare-and-write stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    if (!m_compare_succeeded) {
      rwl.m_perfcounter->inc(l_librbd_rwl_cmp_fails, 1);
    }
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_cmp_latency, comp_latency);
  }

  /*
   * Compare and write doesn't implement alloc_resources(), deferred_handler(),
   * or dispatch(). We use the implementation in C_WriteRequest(), and only if the
   * compare phase succeeds and a write is actually performed.
   */

  const char *get_name() const override {
    return "C_CompAndWriteRequest";
  }
};

/*****
 * This is the custodian of the BlockGuard cell for this write same.
 *
 * A writesame allocates and persists a data buffer like a write, but the
 * data buffer is usually much shorter than the write same.
 */
template <typename T>
struct C_WriteSameRequest : public C_WriteRequest<T>
{
  using C_BlockIORequest<T>::rwl;

  friend std::ostream &operator<<(std::ostream &os, const C_WriteSameRequest<T> &req) {
    os << (C_WriteRequest<T>&)req;
    return os;
  };

  C_WriteSameRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
                     bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_WriteSameRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_WriteSameRequest<T>> create(U&&... arg) {
    return SharedPtrContext::create<C_WriteSameRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  /* Inherit finish_req() from C_WriteRequest */

  void update_req_stats(utime_t &now) override {
    /* Write same stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_ws_latency, comp_latency);
  }

  /* Write sames will allocate one buffer, the size of the repeating pattern */
  void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) override 
  {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;

    ceph_assert(this->m_image_extents.size() == 1);

    bytes_dirtied += this->m_image_extents[0].second;
    auto pattern_length = this->bl.length();
    this->m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    bytes_cached += pattern_length;
    if (pattern_length > buffer.allocation_size) {
      buffer.allocation_size = pattern_length;
    }
  }

  virtual void setup_log_operations()
  {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;

    /* Write same adds a single WS log op to the vector, corresponding to the single buffer item created above */
    ceph_assert(this->m_image_extents.size() == 1);
    auto extent = this->m_image_extents.front();
    /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
    auto operation =
      std::make_shared<WriteSameLogOperation<T>>(*this->m_op_set.get(), extent.first, extent.second, this->bl.length());
    this->m_op_set->operations.emplace_back(operation);
  }

  /*
   * Write same doesn't implement alloc_resources(), deferred_handler(), or
   * dispatch(). We use the implementation in C_WriteRequest().
   */

  void schedule_append() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this->get_name() << " " << this << dendl;
    }
    C_WriteRequest<T>::schedule_append();
  }

  const char *get_name() const override {
    return "C_WriteSameRequest";
  }
};

// =======================================================================================
// =======================================================================================

template <typename T>
void C_WriteRequest<T>::dispatch()
{
  CephContext *cct = rwl.m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GeneralWriteLogEntries log_entries;
  DeferredContexts on_exit;
  utime_t now = ceph_clock_now();

  auto write_req_sp = shared_from_this();

  this->m_dispatched_time = now;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(rwl.m_internal->m_log_pool, struct WriteLogPoolRoot);

  // step 1 : check whether need to insert new sync point.
  // step 2 : break write request down operations
  // step 3 : except for entry_index, assign values to all data member which belong to in-mem WritePmemLogEntry.
  {
    uint64_t buffer_offset = 0;

    // multiple thread to race this function.
    Mutex::Locker locker(rwl.m_lock);

    Context* set_complete = this;
    if (use_finishers) {
      set_complete = new C_OnFinisher(this, &rwl.m_on_persist_finisher);
    }

   /*
    * Create new sync point and persist the previous one.
    *
    * This sequenced write will bear a sync gen number shared with no already completed writes.
    *
    * A group of sequenced writes may be safely flushed concurrently if they all arrived
    * before any of them completed.
    *
    * We'll insert one on an aio_flush() from the application.
    * Here we're inserting one to cap the number of bytes and writes per sync point.
    *
    * When the application is not issuing flushes, we insert sync points to
    * record some observed write concurrency information that enables us to safely issue >1 flush
    * write (for writes observed here to have been in flight simultaneously)
    * at a time in persist-on-write mode.
    */
    if ((!rwl.m_persist_on_flush && rwl.m_current_sync_point->log_entry->m_writes_completed) ||
        (rwl.m_current_sync_point->log_entry->m_writes > MAX_WRITES_PER_SYNC_POINT) ||
        (rwl.m_current_sync_point->log_entry->m_bytes > MAX_BYTES_PER_SYNC_POINT))
    {
      ldout(cct, 20) << "note : flush then insert new sync point due to the following situation :  " << dendl;
      ldout(cct, 20) << "    - check if (rwl.m_current_sycn_point->log_entry->m_writes) > MAX_WRITES_PER_SYNC_POINT  : " << (rwl.m_current_sync_point->log_entry->m_writes > MAX_WRITES_PER_SYNC_POINT) << dendl;
      ldout(cct, 20) << "    - check if (rwl.m_current_sync_point->log_entry->m_bytes) > MAX_BYTES_PER_SYNC_POINT :  " << (rwl.m_current_sync_point->log_entry->m_bytes > MAX_BYTES_PER_SYNC_POINT) << dendl;
      ldout(cct, 20) << "    - check if (rwl.m_current_sync_point->log_entry->m_writes_completed) : " << rwl.m_current_sync_point->log_entry->m_writes_completed << dendl;
      rwl.flush_new_sync_point(nullptr, on_exit);
    }

    // create op set (op set still is empty)
    m_op_set = make_unique<WriteLogOperationSet<T>>(rwl, now,
                                                    rwl.m_current_sync_point, // belog to which sync point
                                                    rwl.m_persist_on_flush,
                                                    this->m_image_extents_summary.block_extent(),
                                                    set_complete);

    ceph_assert(m_resources.allocated);

    // new operation, then put it to op set.
    this->setup_log_operations();

    /* when reading this for sentence, should recall all data member of PmemLogEntry */
    auto allocation = m_resources.buffers.begin();
    for (auto &gen_op : m_op_set->operations)
    {
      auto operation = gen_op->get_write_op();
      log_entries.emplace_back(operation->log_entry);

      rwl.m_perfcounter->inc(l_librbd_rwl_log_ops, 1);
    
      // WriteLogPmemEntry consist of seven data member.

      // data 4 : image_offset_bytes --> construction
      // data 5 : image_bytes --> construction
      // data 3 : entry_id --> alloc_op_log_entries will do this.

      operation->log_entry->ram_entry.has_data = 1; // data 6
      operation->log_entry->ram_entry.write_data = allocation->buffer_oid; // data 7
      ceph_assert(!TOID_IS_NULL(operation->log_entry->ram_entry.write_data));
      operation->log_entry->ram_entry.sync_gen_number = rwl.m_current_sync_gen; // data 1
      if (m_op_set->m_persist_on_flush) {
        operation->log_entry->ram_entry.write_sequence_number = 0;
      } else {
        operation->log_entry->ram_entry.write_sequence_number = ++rwl.m_last_op_sequence_num; // data 2
        operation->log_entry->ram_entry.sequenced = 1;
      }
      operation->log_entry->ram_entry.sync_point = 0;
      operation->log_entry->ram_entry.discard = 0;

      operation->buffer_alloc = &(*allocation);
      operation->log_entry->pmem_buffer = D_RW(operation->log_entry->ram_entry.write_data);

      // ####### break down bufferlist of request, then assgin sub-data to every operation ########
      operation->bl.substr_of(this->bl, buffer_offset, operation->log_entry->write_bytes());

      buffer_offset += operation->log_entry->write_bytes();
      allocation++;
    } // for m_op_sets
  } // scope for lock rwl.m_lock

  m_op_set->m_extent_ops_appending->activate();
  m_op_set->m_extent_ops_persist->activate();

  // step 4 : bufferlist --> pmem_buffer
  for (auto &operation : m_op_set->operations)
  {
    auto write_op = operation->get_write_op();
    ceph_assert(write_op != nullptr);

    bufferlist::iterator i(&write_op->bl);
    rwl.m_perfcounter->inc(l_librbd_rwl_log_op_bytes, write_op->log_entry->write_bytes());
    i.copy((unsigned)write_op->log_entry->write_bytes(), (char*)write_op->log_entry->pmem_buffer);
  }

  // step 5 :  
  /*
   * Adding these entries to the map of (blocks to log entries) makes them
   * readable by this application.
   *
   * They aren't persisted yet, so they may disappear after certain failures.
   *
   * We'll indicate our guaratee of the persistence of these writes with the completion of this request,
   * or the following aio_flush(), according to the configured policy.
   */
  rwl.m_blocks_to_log_entries.add_log_entries(log_entries); // ##

  /*
   * Entries are added to m_log_entries in alloc_op_log_entries() when their
   * order is established.
   *
   * They're added to m_dirty_log_entries when the write completes to all replicas.
   * They must not be flushed before then.
   *
   * we don't prevent the application from reading these before they persist.
   * If we supported coherent shared access, that might be a problem (the write could
   * fail after another initiator had read it).
   *
   * As it is the cost of running reads through the block gurad (and exempting them from
   * the barrier, which doesn't need to apply to them) to prevent reading before the previous
   * write of that data persists doesn't seem justified.
   *
   * We're done with the caller's buffer, and not guaranteeing persistence until the next flush.
   *
   * The block guard for this write_req will not be released until the write is persisted
   * everywhere, but the caller's request can complete now.
   */
  if (rwl.m_persist_on_flush_early_user_comp && m_op_set->m_persist_on_flush) {
    this->complete_user_request(0);
  }

  bool append_deferred = false;

  // determine how to append log entry of current write : defer or directly execute
  {
    Mutex::Locker locker(rwl.m_lock);
   /*
    * In persist-on-write mode, we defer the append of this write until the
    * previous sync point is appending (meaning all the writes before it are
    * persisted and that previous sync point can now appear in the log).
    *
    * Since we insert sync points in persist-on-write mode when writes
    * have already completed to the current sync point, this limits us to
    * one inserted sync point in flight at a time, and gives the next
    * inserted sync point some time to accumulate a few writes if they
    * arrive soon.
    *
    * Without this we can insert an absurd number of sync
    * points, each with one or two writes. That uses a lot of log entries,
    * and limits flushing to very few writes at a time.
    */

    ldout(cct, 20) << "check if earlier sync point is persisted as below : " << dendl;

    // At persist_on_write mode, if earlier sync point don't be completed,
    // we need to defer current write until earlier sync point is completed
    // At SyncPointLogOperation::complete, earlier_sync_point will be assigned to nullptr.
    if (!m_op_set->m_persist_on_flush && m_op_set->sync_point->earlier_sync_point)
    {
      m_do_early_flush = false;

      Context *schedule_append_ctx = new FunctionContext([this, write_req_sp](int r) {
          write_req_sp->schedule_append();
      });
      m_op_set->sync_point->earlier_sync_point->m_on_sync_point_appending.push_back(schedule_append_ctx);

      append_deferred = true;

    } else {
      ldout(cct, 20) << "    - earlier sync point still exist, so defer current write " << dendl;

      /* The prior sync point is done, so we'll schedule append here.
       *
       * If this is persist-on-write mode, true or false.
       * If this is persist-on-flush mode, false
       *
       * If this is persist-on-write, and probably still the caller's thread, we'll use this
       * caller's thread to perform the persist & replication of the payload buffer. */

       m_do_early_flush = !(this->m_detained || this->m_queued || this->m_deferred || m_op_set->m_persist_on_flush);
    }
  }

  if (!append_deferred) {
    ldout(cct, 20) << "    - earlier sync point have been persisted, directly schedule current write append." << dendl;
    this->schedule_append();
  } else {
    ldout(cct, 20) << "    - earlier sync point still exist, so defer current write " << dendl;
  }
  // now, execute deferring contexts.
}

/**
 * Attempts to allocate log resources for a write. Returns true if successful.
 *
 * Resources include 
 *  - 1 lane per extent
 *  - 1 log entry per extent
 *  - 1 PM space for per extent.
 *
 * Lanes are released after the write persists via release_write_lanes()
 */
template <typename T>
bool C_WriteRequest<T>::alloc_resources()
{
  CephContext *cct = rwl.m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  utime_t alloc_start = ceph_clock_now();

  bool alloc_succeeds = true;
  bool no_space = false;
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;

  ceph_assert(!rwl.m_lock.is_locked_by_me());
  ceph_assert(!m_resources.allocated);

  m_resources.buffers.reserve(this->m_image_extents.size());

  // check three kind of resources of rwl.
  {
    Mutex::Locker locker(rwl.m_lock);

    /* This isn't considered a "no space" alloc fail. Lanes are a throttling mechanism.*/
    if (rwl.m_free_lanes < this->m_image_extents.size())
    {
      this->m_waited_lanes = true;
      alloc_succeeds = false;

      ldout(cct, 20)<< "Throttling mechanism : check lanes -- fails " << dendl;
    }

    if (rwl.m_free_log_entries < this->m_image_extents.size())
    {
      this->m_waited_entries = true; // ##
      alloc_succeeds = false;
      no_space = true; // Entries must be retired 

      ldout(cct, 20) << "check alloc log entries -- fails" << dendl;
    }

    /* Don't attempt buffer allocate if we've exceeded the "full" threshold */
    if (rwl.m_bytes_allocated > rwl.m_bytes_allocated_cap)
    {
      if (!this->m_waited_buffers) {
        this->m_waited_buffers = true;
      }
      alloc_succeeds = false;
      no_space = true; // Entries must be retired 

      ldout(cct, 20) << "check alloc payload pmem buffer -- fails" << dendl;
    }
  }

  /* concurrently setup resource buffer, then apply for AEP space */

  if (alloc_succeeds) {
    setup_buffer_resources(bytes_cached, bytes_dirtied);
  }

  // AEP payload spece
  if (alloc_succeeds)
  {
    for (auto &buffer : m_resources.buffers)
    {
      bytes_allocated += buffer.allocation_size;

      utime_t before_reserve = ceph_clock_now();
      ceph_assert(buffer.allocated == false);   

      // reverse AEP space
      buffer.buffer_oid = pmemobj_reserve(rwl.m_internal->m_log_pool,
                                          &buffer.buffer_alloc_action, // action
                                          buffer.allocation_size, 0);

      buffer.allocation_lat = ceph_clock_now() - before_reserve;

      if (TOID_IS_NULL(buffer.buffer_oid)) {
        if (!this->m_waited_buffers) {
          this->m_waited_buffers = true;
        }
        alloc_succeeds = false;
        no_space = true; /* Entries need to be retired */
        ldout(cct, 20) << "reserver pmem payload space fails..." << dendl;
        break;
      } else {
        buffer.allocated = true;
      }

      if (RWL_VERBOSE_LOGGING) {
        ldout(rwl.m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo
                                       << "." << buffer.buffer_oid.oid.off << ", size=" << buffer.allocation_size << dendl;
      }
    } // for
  } 

  /* mannual exclusive to modify rwl data member */

  // AEP log entry and lanes
  if (alloc_succeeds)
  {
    unsigned int num_extents = this->m_image_extents.size();

    Mutex::Locker locker(rwl.m_lock);

    /* We need one free log entry per extent (each is a separate entry), and
     * one free "lane" for remote replication. */
    if ((rwl.m_free_lanes >= num_extents) && (rwl.m_free_log_entries >= num_extents))
    {
      rwl.m_free_lanes -= num_extents;
      rwl.m_free_log_entries -= num_extents;

      // this express that these resource just reserver, but don't use ????
      rwl.m_unpublished_reserves += num_extents;

      rwl.m_bytes_allocated += bytes_allocated;
      rwl.m_bytes_cached += bytes_cached;
      rwl.m_bytes_dirty += bytes_dirtied;

      m_resources.allocated = true;
    } else {
      ldout(cct, 20) << "reserver log entry fails..." << dendl;
      alloc_succeeds = false;
    }
  }

  // if allocation process have any error, just release allocated resources...sdh
  if (!alloc_succeeds)
  {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : m_resources.buffers)
    {
      if (buffer.allocated) {
        pmemobj_cancel(rwl.m_internal->m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }

    m_resources.buffers.clear();

    if (no_space) {
      /* Expedite flushing and/or retiring */
      Mutex::Locker locker(rwl.m_lock);
      rwl.m_alloc_failed_since_retire = true;
      rwl.m_last_alloc_fail = ceph_clock_now();
    }
  }

  this->m_allocated_time = alloc_start;
  return alloc_succeeds;
}

} // namespace cache
} // namespace librbd

#endif


/*********************************************************************************
 *
 *  context    enabled_shared_from_this<SharedPtrContext>
 *     |           |
 *     v           v
 *    SharedPtrContext
 *          |
 *          v
 * C_GuardedBlockIORequest
 *   |
 *   ---> C_ReadRequest
 *   |
 *   ---> C_BlockIORequest
 *          |
 *          ---> C_DiscardRequest
 *          |
 *          ---> C_FlushRequest
 *          |
 *          ---> C_WriteRequest
 *                 |
 *                 ---> C_WriteSameRequest 
 *                 |
 *                 ---> C_CompAndWriteRequest
 *
 *
 *********************************************************************************
 *
 * Context::complete 
 *   |
 *   ---> BlockIORequest::finish
 *           |
 *           ---> BlockIORequest::complete_user_request
 *           |        |
 *           |        ---> user callback
 *           |        
 *           |
 *           ---> child_request::finish_req
 *                  |   
 *                  ---> C_WriteRequest::finish_req
 *                  |        |
 *                  |        ---> rwl.release_lanes
 *                  |        |
 *                  |        ---> C_GuardedBlockIORequest::release_cell
 *                  |                |
 *                  |                ---> rwl.release_guard_request
 *                  |                 
 *                  ---> C_FlushRequest::finish_req           
 *                  |        |            
 *                  |        ---> nothing to do           
 *                  |                     
 *                  ---> C_DiscardRequest::finish_req           
 *                           |   
 *                           ---> nothing to do  
 *
 *
 ***********************************************************************************
 *
 * GuardedRequestFunctionContext 
 *    |
 *    ---> C_WriteRequest::blockguard_acquired
 *            |
 *            ---> C_GuardedBlockIORequest::set_cell
 * 
 * 
 ***********************************************************************************
 *    
 * 
 * 
 * 
 * 
 *                     dispatch 
 *                          step 1 :
 *                              flush_new_sync_point
 *
 *                          step 2 :
 *                             m_op_set = xxx                             
 *                              setup_op_set
 *
 *                           step 3 : 
 *                              data : request ---> op 
 *                                     op ----> pmem
 *           
 *                           ste 4 : how to schedule schedule_append
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 *       
 *********************************************************************************/

