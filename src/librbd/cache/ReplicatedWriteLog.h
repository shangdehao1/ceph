#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/Utils.h"
#include "librbd/BlockGuard.h"
#include <functional>
#include <list>
#include "common/Finisher.h"
#include "include/ceph_assert.h"
#include "librbd/cache/LogMap.h"

#include "rwl/Types.h"
#include "rwl/SyncPoint.h"
#include "rwl/Operation.h"
#include "rwl/BlockGuard.h"

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

template <typename ImageCtxT>
class ReplicatedWriteLog : public ImageCache<ImageCtxT> 
{
public:
  using typename ImageCache<ImageCtxT>::Extent;
  using typename ImageCache<ImageCtxT>::Extents;
  using This = ReplicatedWriteLog<ImageCtxT>;
  using SyncPointT = rwl::SyncPoint<This>;
  using GenericLogOperationT = rwl::GenericLogOperation<This>;
  using GenericLogOperationSharedPtrT = rwl::GenericLogOperationSharedPtr<This>;
  using WriteLogOperationT = rwl::WriteLogOperation<This>;
  using WriteLogOperationSetT = rwl::WriteLogOperationSet<This>;
  using SyncPointLogOperationT = rwl::SyncPointLogOperation<This>;
  using WriteSameLogOperationT = rwl::WriteSameLogOperation<This>;
  using DiscardLogOperationT = rwl::DiscardLogOperation<This>;
  using GenericLogOperationsT = rwl::GenericLogOperations<This>;
  using GenericLogOperationsVectorT = rwl::GenericLogOperationsVector<This>;
  using C_BlockIORequestT = C_BlockIORequest<This>;
  using C_WriteRequestT = C_WriteRequest<This>;
  using C_FlushRequestT = C_FlushRequest<This>;
  using C_DiscardRequestT = C_DiscardRequest<This>;
  using C_WriteSameRequestT = C_WriteSameRequest<This>;
  using C_CompAndWriteRequestT = C_CompAndWriteRequest<This>;

  ReplicatedWriteLog(ImageCtx &image_ctx, ImageCache<ImageCtxT> *lower);
  ~ReplicatedWriteLog();

  ReplicatedWriteLog(const ReplicatedWriteLog&) = delete;
  ReplicatedWriteLog &operator=(const ReplicatedWriteLog&) = delete;

  // client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl, int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl, int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length, bool skip_partial_discard, Context *on_finish);
  void aio_flush(Context *on_finish) override;
  void aio_writesame(uint64_t offset, uint64_t length, ceph::bufferlist&& bl, int fadvise_flags, Context *on_finish) override;
  void aio_compare_and_write(Extents&& image_extents, ceph::bufferlist&& cmp_bl, ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,int fadvise_flags, Context *on_finish) override;

  // internal state methods
  void init(Context *on_finish) override;
  void shut_down(Context *on_finish) override;
  void get_state(bool &clean, bool &empty, bool &present) override;

  void flush(Context *on_finish, bool invalidate = false, bool discard_unflushed_writes = false);
  void flush(Context *on_finish) override;
  
  void invalidate(Context *on_finish, bool discard_unflushed_writes = false);
  void invalidate(Context *on_finish) override;

private:
  friend class rwl::SyncPoint<This>;
  friend class rwl::GenericLogOperation<This>;
  friend class rwl::GeneralWriteLogOperation<This>;
  friend class rwl::WriteLogOperation<This>;
  friend class rwl::WriteLogOperationSet<This>;
  friend class rwl::DiscardLogOperation<This>;
  friend class rwl::WriteSameLogOperation<This>;
  friend class rwl::SyncPointLogOperation<This>;
  friend class rwl::C_GuardedBlockIORequest<This>;
  friend class C_BlockIORequest<This>;
  friend class C_WriteRequest<This>;
  friend class C_FlushRequest<This>;
  friend class C_DiscardRequest<This>;
  friend class C_WriteSameRequest<This>;
  friend class C_CompAndWriteRequest<This>;

  typedef std::list<C_WriteRequest<This> *> C_WriteRequests;
  typedef std::list<C_BlockIORequest<This> *> C_BlockIORequests;

  // ===== BlockGuard =====

  BlockGuardCell* detain_guarded_request_helper(GuardedRequest &req);
  BlockGuardCell* detain_guarded_request_barrier_helper(GuardedRequest &req);
  void detain_guarded_request(GuardedRequest &&req);
  void release_guarded_request(BlockGuardCell *cell);

  // ==== RWL status =======

  std::atomic<bool> m_initialized = {false};
  std::atomic<bool> m_shutting_down = {false};
  std::atomic<bool> m_invalidating = {false};

  ReplicatedWriteLogInternal *m_internal;
  const char* rwl_pool_layout_name;

  ImageCtxT &m_image_ctx;

  std::string m_log_pool_name;
  bool m_log_is_poolset = false;
  uint64_t m_log_pool_config_size;     /* Configured size of RWL */
  uint64_t m_log_pool_actual_size = 0; /* Actual size of RWL pool */
  uint32_t m_total_log_entries = 0;
  uint32_t m_free_log_entries = 0;

  std::atomic<uint64_t> m_bytes_allocated = {0}; /* Total bytes allocated in write buffers */
  uint64_t m_bytes_cached = 0;    /* Total bytes used in write buffers */
  uint64_t m_bytes_dirty = 0;     /* Total bytes yet to flush to RBD */
  uint64_t m_bytes_allocated_cap = 0;

  utime_t m_last_alloc_fail;      /* Entry or buffer allocation fail seen */
  std::atomic<bool> m_alloc_failed_since_retire = {false};

  ImageCache<ImageCtxT> *m_image_writeback;
  WriteLogGuard m_write_log_guard;

  /* When m_first_free_entry == m_first_valid_entry, the log is empty.
   * There is always at least one free entry, which can't be used. */
  uint64_t m_first_free_entry = 0;  /* Entries [from here to m_first_valid_entry-1] are free */
  uint64_t m_first_valid_entry = 0; /* Entries [from here to m_first_free_entry-1] are valid */

  /* Starts at 0 for a new write log. Incremented on every flush. */
  uint64_t m_current_sync_gen = 0;

  std::shared_ptr<SyncPointT> m_current_sync_point = nullptr;

  /* Starts at 0 on each sync gen increase. Incremented before applied to an operation */
  uint64_t m_last_op_sequence_num = 0;

  /* All writes bearing this and all prior sync gen numbers are flushed */
  uint64_t m_flushed_sync_gen = 0;

  bool m_persist_on_write_until_flush = true;

  /* True if it's safe to complete a user request in persist-on-flush mode before the write is persisted. 
   * This is only true if there is a local copy of the write data, or if local write failure always
   * causes local node failure. */
  bool m_persist_on_flush_early_user_comp = false; /* Assume local write failure does not cause node failure */

  /* If false, persist each write before completion */
  bool m_persist_on_flush = false; 
  bool m_flush_seen = false;

  util::AsyncOpTracker m_async_op_tracker;

  /* Debug counters for the places m_async_op_tracker is used */
  std::atomic<int> m_async_flush_ops = {0};
  std::atomic<int> m_async_append_ops = {0};
  std::atomic<int> m_async_complete_ops = {0};
  std::atomic<int> m_async_null_flush_finish = {0};
  std::atomic<int> m_async_process_work = {0};

  /************* Acquire locks in order declared here ********************/

  mutable Mutex m_log_retire_lock;

  /* Hold a read lock on m_entry_reader_lock to add readers to log entry
   * bufs. Hold a write lock to prevent readers from being added (e.g. when
   * removing log entrys from the map). No lock required to remove readers. */
  mutable RWLock m_entry_reader_lock;

  /* Hold m_deferred_dispatch_lock while consuming from m_deferred_ios. */
  mutable Mutex m_deferred_dispatch_lock;

  /* Hold m_log_append_lock while appending or retiring log entries. */
  mutable Mutex m_log_append_lock;

  /* Used for most synchronization */
  mutable Mutex m_lock;

  /* Used in release/detain to make BlockGuard preserve submission order */
  mutable Mutex m_blockguard_lock;

  /* Used in WriteLogEntry::get_pmem_bl() to syncronize between threads making entries readable */
  mutable Mutex m_entry_bl_lock;

  /* Use m_blockguard_lock for the following 3 things */
  WriteLogGuard::BlockOperations m_awaiting_barrier;
  bool m_barrier_in_progress = false;
  BlockGuardCell *m_barrier_cell = nullptr;

  bool m_wake_up_requested = false;
  bool m_wake_up_scheduled = false;
  bool m_wake_up_enabled = true;

  bool m_appending = false;
  bool m_dispatching_deferred_ops = false;

  Contexts m_flush_complete_contexts;

  // finisher
  Finisher m_persist_finisher;
  Finisher m_log_append_finisher;
  Finisher m_on_persist_finisher;

 /* increase : schedule_flush_and_append
  * decrease : flush_then_append_scheduled_ops
  * Write ops needing flush in local log */
 GenericLogOperationsT m_ops_to_flush;

  // increase : schedule_append
  // decrease : append_scheduled_ops
  GenericLogOperationsT m_ops_to_append; /* Write ops needing event append in local log */

  /* map block extent to log entry */
  WriteLogMap m_blocks_to_log_entries;  // reading existing entries from pmem....sdh

  /* New entries are at the back. Oldest at the front */
  GenericLogEntries m_log_entries; // this structure maintain in-memory log entry with the same AEP.

  // This entry is only dirty if its sync gen number is > the flushed sync gen number from the root object. 
  GenericLogEntries m_dirty_log_entries;

  // the following three item : construct_flush_entry_context
  int m_flush_ops_in_flight = 0;
  int m_flush_bytes_in_flight = 0;
  uint64_t m_lowest_flushing_sync_gen = 0;

  /* Writes that have left the block guard, but are waiting for resources */
  C_BlockIORequests m_deferred_ios;

  /* Throttle writes concurrently allocating & replicating */
  unsigned int m_free_lanes = MAX_CONCURRENT_WRITES;
  unsigned int m_unpublished_reserves = 0;

  PerfCounters *m_perfcounter = nullptr;

  /* Initialized from config, then set false during shutdown */
  std::atomic<bool> m_periodic_stats_enabled = {false};
  mutable Mutex m_timer_lock; /* Used only by m_timer */
  SafeTimer m_timer;

  ThreadPool m_thread_pool;
  ContextWQ m_work_queue;

  /* Returned by get_state() */
  std::atomic<bool> m_clean = {false};
  std::atomic<bool> m_empty = {false};
  std::atomic<bool> m_present = {true};

  const Extent whole_volume_extent(void);

  // performace statics tools
  void perf_start(const std::string name);
  void perf_stop();
  void log_perf();
  void periodic_stats();
  void arm_periodic_stats();

  // initialize
  void rwl_init(Context *on_finish, DeferredContexts &later);
  void load_existing_entries(DeferredContexts &later);

  // background thread to execute retire / re-dispatch / flush
  void wake_up();
  void process_work();

  void flush_dirty_entries(Context *on_finish);
  bool can_flush_entry(const std::shared_ptr<GenericLogEntry> log_entry);
  Context *construct_flush_entry_ctx(const std::shared_ptr<GenericLogEntry> log_entry);

  void persist_last_flushed_sync_gen(void);
  bool handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry);
  void sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry);

  void process_writeback_dirty_entries();

  bool can_retire_entry(const std::shared_ptr<GenericLogEntry> log_entry);
  bool retire_entries(const unsigned long int frees_per_tx = MAX_FREE_PER_TRANSACTION);

  // sync point
  void init_flush_new_sync_point(DeferredContexts &later);
  void new_sync_point(DeferredContexts &later);
  C_FlushRequest<ReplicatedWriteLog<ImageCtxT>>* make_flush_req(Context *on_finish);

  void flush_new_sync_point(C_FlushRequestT *flush_req, DeferredContexts &later);
  void flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, DeferredContexts &later);

  void dispatch_deferred_writes(void);

  void release_write_lanes(C_WriteRequestT *write_req);

  void alloc_and_dispatch_io_req(C_BlockIORequestT *write_req);
  void append_scheduled_ops(void);

  void enlist_op_appender(); //  enqueue flush_then_append_scheuled_ops work queue...sdh

  void schedule_append(GenericLogOperationsVectorT &ops); // rwl will take custody of ops, then put them into m_ops_to_append...sdh
  void schedule_append(GenericLogOperationsT &ops);
  void schedule_append(GenericLogOperationSharedPtrT op);

  void flush_then_append_scheduled_ops(void);

  void enlist_op_flusher();

  void schedule_flush_and_append(GenericLogOperationsVectorT &ops);

  /*  Flush the pmem regions for the data blocks of a set of operations */
  template <typename V>
  void flush_pmem_buffer(V& ops); 

  // Allocate the (already reserved) write log entries for a set of operations.
  void alloc_op_log_entries(GenericLogOperationsT &ops);

  /* Flush the persistent write log entries set of ops. The entries must be contiguous in persistent memory. */
  void flush_op_log_entries(GenericLogOperationsVectorT &ops);

  /* Write and persist the (already allocated) write log entries and data buffer allocations for a set of ops. 
   * The data buffer for each of these must already have been persisted to its reserved area. */
  int append_op_log_entries(GenericLogOperationsT &ops);

  /* Complete a set of write ops with the result of append_op_entries.*/
  void complete_op_log_entries(GenericLogOperationsT &&ops, const int r);

  /* Push op log entry completion to a WQ. */
  void schedule_complete_op_log_entries(GenericLogOperationsT &&ops, const int r);

  void internal_flush(Context *on_finish, bool invalidate=false, bool discard_unflushed_writes=false);
};

}  // namespace cache
}  // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
