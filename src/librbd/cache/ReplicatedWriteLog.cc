#include <libpmemobj.h>
#include "ReplicatedWriteLog.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "rwl/SharedPtrContext.h"
#include <map>
#include <vector>

#include "rwl/DeferredContexts.h"
#include "rwl/PmemLayout.h"
#include "rwl/LogEntry.h"
#include "rwl/Request.h"
#include "rwl/Extent.h"
#include "rwl/Config.h"
#include "rwl/Resource.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

class ReplicatedWriteLogInternal 
{
public:
  PMEMobjpool *m_log_pool = nullptr;
};

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(ImageCtx &image_ctx, ImageCache<I> *lower)
  : m_image_ctx(image_ctx),
    m_internal(new ReplicatedWriteLogInternal),
    rwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_rwl)),
    m_log_pool_config_size(DEFAULT_POOL_SIZE),
    m_image_writeback(lower), 
    m_write_log_guard(image_ctx.cct),
    m_log_retire_lock("librbd::cache::ReplicatedWriteLog::m_log_retire_lock", false, true, true),
    m_entry_reader_lock("librbd::cache::ReplicatedWriteLog::m_entry_reader_lock"),
    m_deferred_dispatch_lock("librbd::cache::ReplicatedWriteLog::m_deferred_dispatch_lock", false, true, true),
    m_log_append_lock("librbd::cache::ReplicatedWriteLog::m_log_append_lock", false, true, true),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock", false, true, true),
    m_blockguard_lock("librbd::cache::ReplicatedWriteLog::m_blockguard_lock", false, true, true),
    m_entry_bl_lock("librbd::cache::ReplicatedWriteLog::m_entry_bl_lock", false, true, true),
    m_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_persist_finisher", "pfin_rwl"),
    m_log_append_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_log_append_finisher", "afin_rwl"),
    m_on_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_on_persist_finisher", "opfin_rwl"),
    m_blocks_to_log_entries(image_ctx.cct),
    m_timer_lock("librbd::cache::ReplicatedWriteLog::m_timer_lock", false, true, true),
    m_timer(image_ctx.cct, m_timer_lock, false),
    m_thread_pool(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::thread_pool", "tp_rwl", 4, ""),
    m_work_queue("librbd::cache::ReplicatedWriteLog::work_queue", 60,  &m_thread_pool)
{
  std::cout << std::endl << "==================== RWL construction =================" << std::endl << std::endl;

  ceph_assert(lower);

  m_thread_pool.start();

  if (use_finishers) {
    m_persist_finisher.start(); 
    m_log_append_finisher.start();
    m_on_persist_finisher.start();
  }

  m_timer.init();
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() 
{
  std::cout << std::endl << "==================== RWL destruction =================" << std::endl << std::endl;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.shutdown();

    // all lock should be release
    ldout(m_image_ctx.cct, 15) << "acquiring locks that shouldn't still be held" << dendl;

    Mutex::Locker retire_locker(m_log_retire_lock);
    RWLock::WLocker reader_locker(m_entry_reader_lock);
    Mutex::Locker dispatch_locker(m_deferred_dispatch_lock);
    Mutex::Locker append_locker(m_log_append_lock);
    Mutex::Locker locker(m_lock);
    Mutex::Locker bg_locker(m_blockguard_lock);
    Mutex::Locker bl_locker(m_entry_bl_lock);

    ldout(m_image_ctx.cct, 15) << "gratuitous locking complete" << dendl;

    delete m_image_writeback;

    m_image_writeback = nullptr;

    ceph_assert(m_deferred_ios.size() == 0);
    ceph_assert(m_ops_to_flush.size() == 0);
    ceph_assert(m_ops_to_append.size() == 0);
    ceph_assert(m_flush_ops_in_flight == 0);
    ceph_assert(m_unpublished_reserves == 0);
    ceph_assert(m_bytes_dirty == 0);
    ceph_assert(m_bytes_cached == 0);
    ceph_assert(m_bytes_allocated == 0);

    delete m_internal;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  perf_start(m_image_ctx.id);
  ceph_assert(!m_initialized);

  Context *ctx = new FunctionContext([this, on_finish](int r) 
  {
    if (r >= 0) {
      DeferredContexts later;
      rwl_init(on_finish, later); // ##
      if (m_periodic_stats_enabled) {
        periodic_stats();
      }
    } else {
      // don't init rwl if layer below failed to init 
      m_image_ctx.op_work_queue->queue(on_finish, r);
    }
  });

  // Initialize the cache layer below first 
  m_image_writeback->init(ctx);
}

template <typename I>
void ReplicatedWriteLog<I>::rwl_init(Context *on_finish, DeferredContexts &later) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  ceph_assert(!m_initialized);

  Mutex::Locker locker(m_lock);

  TOID(struct WriteLogPoolRoot) pool_root;

  std::string rwl_path = m_image_ctx.rwl_path;
  ldout(cct,5) << "rwl_path : " << m_image_ctx.rwl_path << dendl;

  std::string log_pool_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".poolset";
  m_log_pool_config_size = max(m_image_ctx.rwl_size, MIN_POOL_SIZE);

  ldout(cct, 5) << "image name : " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;
  ldout(cct, 5) << "rwl_enabled : " << m_image_ctx.rwl_enabled << dendl;
  ldout(cct, 5) << "rwl_size : " << m_image_ctx.rwl_size << dendl;
  ldout(cct, 5) << "log_pool_name : " << log_pool_name << dendl;
  ldout(cct, 5) << "log_poolset_name : " << log_poolset_name << dendl;
  ldout(cct, 5) << "m_log_pool_config_size : " << m_log_pool_config_size << dendl;

  // check poolset file
  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
    m_log_is_poolset = true;
  } else {
    m_log_pool_name = log_pool_name;
    ldout(cct, 5) << "failed to open poolset" << log_poolset_name << ". Opening/creating simple/unreplicated pool" << dendl;
  }

  if (access(m_log_pool_name.c_str(), F_OK) != 0) 
  {
    if ((m_internal->m_log_pool = pmemobj_create(m_log_pool_name.c_str(), 
                                                 rwl_pool_layout_name, 
                                                 m_log_pool_config_size, 
                                                 (S_IWUSR | S_IRUSR))) == NULL) 
    {
      lderr(cct) << "failed to create pool (" << m_log_pool_name << ")" << pmemobj_errormsg() << dendl;
      m_present = false;
      m_clean = true;
      m_empty = true;
      // TODO: filter/replace errnos that are meaningless to the caller 
      on_finish->complete(-errno);
      return;
    }

    /* TODO : case - cache file have been created successfully, but fails at init this file */

    m_present = true;
    m_clean = true;
    m_empty = true;

    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    // payload size : for example, configurable pool size is 1G, but effective size is 1G*0.7
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);

    // 512B payload      -->  MIN_WRITE_ALLOC_SIZE
    // 16B  PMDK need    -->  BLOCK_ALLOC_OVERHEAD_BYTES
    // 64B  log entry    -->  sizeof(WriteLogPmemEntry)
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);

    // log entry number
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    ceph_assert(num_small_writes > 2);

    m_log_pool_actual_size = m_log_pool_config_size;
    m_bytes_allocated_cap = effective_pool_size;

    m_first_free_entry = 0;
    m_first_valid_entry = 0;

    // all data of pool_root need to be updated together at one transaction.
    TX_BEGIN(m_internal->m_log_pool) 
    {
      TX_ADD(pool_root); // 

      D_RW(pool_root)->header.layout_version = RWL_POOL_VERSION;

      D_RW(pool_root)->log_entries = TX_ZALLOC(struct WriteLogPmemEntry, 
                                               sizeof(struct WriteLogPmemEntry) * num_small_writes);
      D_RW(pool_root)->num_log_entries = num_small_writes;

      D_RW(pool_root)->first_free_entry = m_first_free_entry; 
      D_RW(pool_root)->first_valid_entry = m_first_valid_entry;
      D_RW(pool_root)->flushed_sync_gen = m_flushed_sync_gen;

      D_RW(pool_root)->pool_size = m_log_pool_actual_size;
      D_RW(pool_root)->block_size = MIN_WRITE_ALLOC_SIZE;

    } TX_ONCOMMIT {
      m_total_log_entries = D_RO(pool_root)->num_log_entries;
      m_free_log_entries = D_RO(pool_root)->num_log_entries - 1; // leave one free
    } TX_ONABORT {
      m_total_log_entries = 0;
      m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << m_log_pool_name << ")" << dendl;
      on_finish->complete(-pmemobj_tx_errno());
      return;
    } TX_FINALLY {
    } TX_END;
  } 
  else 
  { 
    m_present = true;

    if ((m_internal->m_log_pool = pmemobj_open(m_log_pool_name.c_str(), rwl_pool_layout_name)) == NULL) {
      lderr(cct) << "failed to open pool (" << m_log_pool_name << "): " << pmemobj_errormsg() << dendl;
      on_finish->complete(-errno);
      return;
    }

    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    if (D_RO(pool_root)->header.layout_version != RWL_POOL_VERSION) {
      lderr(cct) << "Pool layout version is " << D_RO(pool_root)->header.layout_version << " expected " << RWL_POOL_VERSION << dendl;
      on_finish->complete(-EINVAL);
      return;
    }

    m_total_log_entries = D_RO(pool_root)->num_log_entries;

    m_flushed_sync_gen = D_RO(pool_root)->flushed_sync_gen;
    m_first_free_entry = D_RO(pool_root)->first_free_entry;
    m_first_valid_entry = D_RO(pool_root)->first_valid_entry;

    m_log_pool_actual_size = D_RO(pool_root)->pool_size;
    if (D_RO(pool_root)->block_size != MIN_WRITE_ALLOC_SIZE) {
      lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      on_finish->complete(-EINVAL);
      return;
    }

    if (m_first_free_entry < m_first_valid_entry) 
    {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
      m_free_log_entries = m_total_log_entries - (m_first_valid_entry - m_first_free_entry) -1;
    } 
    else 
    {
      /* first_valid is <= first_free. If they are == we have zero valid log entries, and n-1 free log entries */
      m_free_log_entries = m_total_log_entries - (m_first_free_entry - m_first_valid_entry) -1;
    }

    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    m_bytes_allocated_cap = effective_pool_size;

    load_existing_entries(later);

    m_clean = m_dirty_log_entries.empty();
    m_empty = m_log_entries.empty();
  
    ldout(cct, 5) << "after load super block, get meta data as below :  " << dendl;
    ldout(cct, 5) << " - pool layout version : " << RWL_POOL_VERSION << dendl;
    ldout(cct, 5) << " - pool total log entries : " << m_total_log_entries <<dendl;
    ldout(cct, 5) << " - pool first valid entry index : " << m_first_valid_entry << dendl;
    ldout(cct, 5) << " - pool flushed sync gen number : " << m_flushed_sync_gen << dendl;
    ldout(cct, 5) << " - pool first free entry index : " << m_first_free_entry << dendl;
    ldout(cct, 5) << " - pool block size : " << MIN_WRITE_ALLOC_SIZE<< dendl;
    ldout(cct, 5) << " - pool size : " << m_log_pool_actual_size << dendl;
    ldout(cct, 5) << "after load existing entries, get log entries information as below : " << dendl;
    ldout(cct, 5) << " - dirty entry size : " << m_dirty_log_entries.size() << dendl;
    ldout(cct, 5) << " - cache entry size : " << m_log_entries.size() << dendl;
  }

  if (m_first_free_entry == m_first_valid_entry) {
    ldout(cct,1) << "write log is empty" << dendl;
    m_empty = true;
  }

  /* Start the sync point following the last one seen in the log. 
   * Flush the last sync point created during the loading of the existing log entries */
  init_flush_new_sync_point(later);

  m_initialized = true;

  m_periodic_stats_enabled = m_image_ctx.rwl_log_periodic_stats;
  arm_periodic_stats();

  m_image_ctx.op_work_queue->queue(on_finish);
}

/*
 * Loads the log entries from an existing log.
 *
 * Creates the in-memory structures to represent the state of the
 * re-opened log.
 *
 * Finds the last appended sync point, and any sync points referred to
 * in log entries, but missing from the log. These missing sync points
 * are created and scheduled for append. Some rudimentary consistency
 * checking is done.
 *
 * Rebuilds the m_blocks_to_log_entries map, to make log entries
 * readable.
 *
 * Places all writes on the dirty entries list, which causes them all
 * to be flushed.
 *
 * TODO: Turn consistency check asserts into open failures.
 *
 * TODO: Writes referring to missing sync points must be discarded if
 * the replication mechanism doesn't guarantee all entries are
 * appended to all replicas in the same order, and that appends in
 * progress during a replica failure will be resolved by the
 * replication mechanism. PMDK pool replication guarantees this, so
 * discarding unsequenced writes referring to a missing sync point is
 * not yet implemented.
 *
 */
template <typename I>
void ReplicatedWriteLog<I>::load_existing_entries(DeferredContexts &later) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  uint64_t entry_index = m_first_valid_entry;

  /* The map below allows us to find sync point log entries by sync gen number, 
   * which is necessary so write entries can be linked to their sync points. */
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;  
  std::shared_ptr<SyncPointLogEntry> highest_existing_sync_point = nullptr;

  /* The map below tracks sync points referred to in writes but not
   * appearing in the sync_point_entries map.  
   * 
   * We'll use this to determine which sync points are missing and need to be created.*/
  std::map<uint64_t, bool> missing_sync_points;

  /* Read the existing log entries. Construct an in-memory log entry
   * object of the appropriate type for each. Add these to the global log entries list.
   *
   * Write entries will not link to their sync points yet. We'll do
   * that in the next pass. Here we'll accumulate a map of sync point
   * gen numbers that are referred to in writes but do not appearing in the log. */
  while (entry_index != m_first_free_entry) 
  {
    WriteLogPmemEntry *pmem_entry = &pmem_log_entries[entry_index]; // ##

    std::shared_ptr<GenericLogEntry> log_entry = nullptr;
    bool writer = pmem_entry->is_writer();

    // check data consistent
    ceph_assert(pmem_entry->entry_index == entry_index);

    ldout(cct, 5) << "currently, missing sync point size : " << missing_sync_points.size() << dendl;
    ldout(cct, 5) << "currently, handling log entry index : " << entry_index << dendl;

    if (pmem_entry->is_sync_point()) 
    { 
      ldout(m_image_ctx.cct, 20) << "Entry [" << entry_index << "] is a sync point. pmem_entry=[" << *pmem_entry << "]" << dendl;

      auto sync_point_entry = std::make_shared<SyncPointLogEntry>(pmem_entry->sync_gen_number);
      log_entry = sync_point_entry;

      ceph_assert(sync_point_entries.find(pmem_entry->sync_gen_number) != sync_point_entries.end()); // dehao
      ceph_assert(sync_point_entries[pmem_entry->sync_gen_number] == nullptr); // dehao

      sync_point_entries[pmem_entry->sync_gen_number] = sync_point_entry;

      ceph_assert(missing_sync_points.find(pmem_entry->sync_gen_number) !=  missing_sync_points.end()); // dehao
      ceph_assert(missing_sync_points.size() == 1); // dehao
      missing_sync_points.erase(pmem_entry->sync_gen_number);
      ceph_assert(missing_sync_points.size() == 0); // dehao

      /* Sync points must appear in order */
      if (highest_existing_sync_point) {
        ceph_assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }

      // if found sync point, it must be the highest. (sync point must appear in order)
      highest_existing_sync_point = sync_point_entry;
      m_current_sync_gen = pmem_entry->sync_gen_number;
    }
    else if (pmem_entry->is_write()) 
    {
      ldout(m_image_ctx.cct, 20) << "Entry [" << entry_index << "] is a write. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto write_entry = std::make_shared<WriteLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      write_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = write_entry;
    } 
    else if (pmem_entry->is_writesame()) 
    {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index << " is a write same. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto ws_entry = std::make_shared<WriteSameLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes, pmem_entry->ws_datalen);
      ws_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = ws_entry;
    }
    else if (pmem_entry->is_discard()) 
    {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index << " is a discard. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto discard_entry = std::make_shared<DiscardLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      log_entry = discard_entry;
    }
    else 
    {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry " << entry_index << ", pmem_entry=[" << *pmem_entry << "]" << dendl;
      ceph_assert(false);
    }

    // check the curresponding sync point entry of write log entry.
    if (writer) 
    {
      ldout(m_image_ctx.cct, 20) << "Entry [" << entry_index << "] writes. pmem_entry=[" << *pmem_entry << "]" << dendl;

      /* Writes must precede the sync points they bear */
      if (highest_existing_sync_point) 
      {
        ceph_assert(highest_existing_sync_point->ram_entry.sync_gen_number + 1 == pmem_entry->sync_gen_number); // dehao
        ceph_assert(highest_existing_sync_point->ram_entry.sync_gen_number == highest_existing_sync_point->pmem_entry->sync_gen_number);
        ceph_assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }

      // TODO : this usage of map ?....sdh
      if (!sync_point_entries[pmem_entry->sync_gen_number]) 
      {
        ldout(m_image_ctx.cct, 20) << "Entry [" << entry_index << "] write. sync point of current write log is missing..." << dendl;
        ceph_assert(sync_point_entries.find(pmem_entry->sync_gen_number) != sync_point_entries.end()); // dehao
        ceph_assert(sync_point_entries[pmem_entry->sync_gen_number] == nullptr); // dehao

	missing_sync_points[pmem_entry->sync_gen_number] = true;
      }
      else 
      {
        ceph_assert(0);
      }
    }

    log_entry->ram_entry = *pmem_entry;
    log_entry->pmem_entry = pmem_entry;
    log_entry->log_entry_index = entry_index;
    log_entry->completed = true;

    // all log entries will be put into this container.
    m_log_entries.push_back(log_entry);

    entry_index = (entry_index + 1) % m_total_log_entries;
  } // while 

  ldout(cct, 5) << "=================================================" << dendl;
  ldout(cct, 5) << "|| summary : missing sync point number is " << missing_sync_points.size() << " ||" << dendl;
  ldout(cct, 5) << "=================================================" << dendl;

  ceph_assert(missing_sync_points.size() == 1 || missing_sync_points.size() == 0); // dehao

  /* Create missing sync points. 
   * These must not be appended until the entry reload is complete and the write map is up to date.
   * Currently this is handled by the deferred contexts object passed to new_sync_point(). 
   * These contexts won't be completed until this function returns.*/
  for (auto &kv : missing_sync_points) 
  {
    ldout(m_image_ctx.cct, 5) << "missing sync point : " << kv.first << "  --> create this missing sync point ."<< dendl;

   /* The unlikely case where the log contains writing entries, but no sync
    * points (e.g. because they were all retired) */
    if (0 == m_current_sync_gen) {
      m_current_sync_gen = kv.first - 1;
    }

    ceph_assert(kv.first == m_current_sync_gen + 1);
    ceph_assert(m_current_sync_point == nullptr);
    init_flush_new_sync_point(later);
    ceph_assert(kv.first == m_current_sync_gen);

    sync_point_entries[kv.first] = m_current_sync_point->log_entry;;
  }

  if(m_log_entries.size() != 0) {
    ldout(cct, 5) << "Iterate over the log entries : connecting write entries to their sync point" << dendl;
  }

  /* Iterate over the log entries again (this time via the global entries list), 
   * connecting write entries to their sync points and updating the sync point stats.
   *
   * Add writes to the write log map. */
  std::shared_ptr<SyncPointLogEntry> previous_sync_point_entry = nullptr;

  for (auto& log_entry : m_log_entries)  
  {
    if (log_entry->ram_entry.is_writer()) 
    {
      auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);
      auto sync_point_entry = sync_point_entries[gen_write_entry->ram_entry.sync_gen_number];

      if (!sync_point_entry) {
        lderr(m_image_ctx.cct) << "Sync point missing for entry=[" << *gen_write_entry << "]" << dendl;
        ceph_assert(false);
      } else { 

        /* TODO: Discard unsequenced writes for sync points that didn't appear in the log (but were added above). 
         * This is optional if the replication mechanism guarantees persistence everywhere in the same order 
         * (which PMDK pool replication does). */
        gen_write_entry->sync_point_entry = sync_point_entry;

        // update sync point entry
        sync_point_entry->m_writes++;
        sync_point_entry->m_bytes += gen_write_entry->ram_entry.write_bytes;
        sync_point_entry->m_writes_completed++;

        m_blocks_to_log_entries.add_log_entry(gen_write_entry);

	/* This entry is only dirty if its sync gen number is > the flushed sync gen number. 
         * Note : the flushed sync gen number derive from the root object. */
        if (gen_write_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) 
        {
          m_dirty_log_entries.push_back(log_entry);
          m_bytes_dirty += gen_write_entry->bytes_dirty();
        } 
        else 
        {
	  gen_write_entry->flushed = true;
	  sync_point_entry->m_writes_flushed++;
	}

        /* This entry is a basic write */
        if (log_entry->ram_entry.is_write()) {
          uint64_t bytes_allocated = MIN_WRITE_ALLOC_SIZE;
          if (gen_write_entry->ram_entry.write_bytes > bytes_allocated) {
            bytes_allocated = gen_write_entry->ram_entry.write_bytes;
          }
          m_bytes_allocated += bytes_allocated;
          m_bytes_cached += gen_write_entry->ram_entry.write_bytes;
        }
      }
    } 
    else if (log_entry->ram_entry.is_sync_point()) 
    {
      auto sync_point_entry = static_pointer_cast<SyncPointLogEntry>(log_entry);

      if (previous_sync_point_entry) 
      {
        // link new sync point with old...sdh
        previous_sync_point_entry->m_next_sync_point_entry = sync_point_entry;

        // ## judge/decide whether this sync point have been flushed...sdh ##
        if (previous_sync_point_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) 
        {
	  sync_point_entry->m_prior_sync_point_flushed = false;
	  ceph_assert(!previous_sync_point_entry->m_prior_sync_point_flushed || 
                      (0 == previous_sync_point_entry->m_writes) ||
                      (previous_sync_point_entry->m_writes > previous_sync_point_entry->m_writes_flushed));
	} else {
	  sync_point_entry->m_prior_sync_point_flushed = true;
	  ceph_assert(previous_sync_point_entry->m_prior_sync_point_flushed);
	  ceph_assert(previous_sync_point_entry->m_writes == previous_sync_point_entry->m_writes_flushed);
	}
	previous_sync_point_entry = sync_point_entry;

      } else {
	// There are no previous sync points, so we'll consider them flushed 
	sync_point_entry->m_prior_sync_point_flushed = true;
      }

      ldout(m_image_ctx.cct, 10) << "Loaded to sync point=[" << *sync_point_entry << dendl;
    }
    else 
    {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry=[" << *log_entry << "]" << dendl;
      ceph_assert(false);
    }
  } // for

  if (0 == m_current_sync_gen) 
  {
   /* If a re-opened log was completely flushed, we'll have found no sync point entries here,
    * and not advanced m_current_sync_gen. Here we ensure it starts past the last flushed sync
    * point recorded in the log. */
    m_current_sync_gen = m_flushed_sync_gen;
  }

}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // execute callback of shutdown
  Context *ctx = new FunctionContext([this, on_finish](int r) 
  {
    {
      Mutex::Locker timer_locker(m_timer_lock);
      m_timer.cancel_all_events();
    }

    ldout(m_image_ctx.cct, 6) << "shutdown complete" << dendl;
    m_image_ctx.op_work_queue->queue(on_finish, r);
  });

  // shutdown lower image-cache
  ctx = new FunctionContext([this, ctx](int r) 
  {
    ldout(m_image_ctx.cct, 20) << "close forend complete, r = " << r << dendl;
    Context *next_ctx = ctx;
    if (r < 0) {
      /* Override on_finish status with this error */
      next_ctx = new FunctionContext([r, ctx](int _r) {
        ctx->complete(r);
      });
    }
    ldout(m_image_ctx.cct, 20) << "shut down lower cache..." << dendl;

    m_image_writeback->shut_down(next_ctx);
  });

   // close forend.
  ctx = new FunctionContext([this, ctx](int r) 
  {
    ldout(m_image_ctx.cct, 20) << "the second wait async op complete, r = " << r << dendl;

    Context *next_ctx = ctx;
    if (r < 0) {
      /* Override next_ctx status with this error */
      next_ctx = new FunctionContext([r, ctx](int _r) {
        ctx->complete(r);
      });
    }

    m_periodic_stats_enabled = false;
    {
      ldout(m_image_ctx.cct, 6) << "stopping timer..." << dendl;

      Mutex::Locker timer_locker(m_timer_lock);
      m_timer.cancel_all_events();
    }

    if (m_perfcounter && m_image_ctx.rwl_log_stats_on_close) {
      log_perf();
    }

    if (use_finishers) 
    {
      ldout(m_image_ctx.cct, 6) << "stopping finishers" << dendl;

      m_persist_finisher.wait_for_empty();
      m_persist_finisher.stop();

      m_log_append_finisher.wait_for_empty();
      m_log_append_finisher.stop();

      m_on_persist_finisher.wait_for_empty();
      m_on_persist_finisher.stop();
    }

    ldout(m_image_ctx.cct, 6) << "stopping thread pool..." << dendl;
    m_thread_pool.stop();
    {
      Mutex::Locker locker(m_lock);
      ceph_assert(m_dirty_log_entries.size() == 0);
      m_clean = true;
      bool empty = true;

      for (auto entry : m_log_entries) 
      {
        if (!entry->ram_entry.is_sync_point()) {
          empty = false; /* ignore sync points for emptiness */
        }

        if (entry->ram_entry.is_write() || entry->ram_entry.is_writesame()) 
        {
          /* WS entry is also a Write entry */
          auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
          m_blocks_to_log_entries.remove_log_entry(write_entry);
          assert(write_entry->referring_map_entries == 0);
          assert(write_entry->reader_count() == 0);
          assert(!write_entry->flushing);
        }
       }
       m_empty = empty;
       m_log_entries.clear();
     }

    if (m_internal->m_log_pool) {
      ldout(m_image_ctx.cct, 20) << "closing pmem pool" << dendl;
      pmemobj_close(m_internal->m_log_pool);
      r = -errno;
    }

    if (m_image_ctx.rwl_remove_on_close) 
    {
      if (m_log_is_poolset) {
        ldout(m_image_ctx.cct, 5) << "Not removing poolset " << m_log_pool_name << dendl;
      } else {
        ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << m_log_pool_name << dendl;
        // delete cache file 
        if (remove(m_log_pool_name.c_str()) != 0) {
          lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << m_log_pool_name << "\": "
                                 << pmemobj_errormsg() << dendl;
        } else {
          m_clean = true;
          m_empty = true;
          m_present = false;
        }
      }
    }

    if (m_perfcounter) {
      perf_stop();
    }

    next_ctx->complete(r);

  }); // context

  // enqueue 
  ctx = new FunctionContext([this, ctx](int r) 
  {
    /* Get off of RWL WQ - thread pool about to be shut down */
    m_image_ctx.op_work_queue->queue(ctx);
  });

  // wait for async io finished.
  ctx = new FunctionContext([this, ctx](int r) 
  {
    ldout(m_image_ctx.cct, 20)<< "flush dirty entries complete r = " << r << dendl;

    Context *next_ctx = ctx;
    if (r < 0) {
      /* Override next_ctx status with this error */
      next_ctx = new FunctionContext([r, ctx](int _r) {
        ctx->complete(r);
      });
    }

    ldout(m_image_ctx.cct, 20) << "retiring entries..." << dendl;

    while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }

    ldout(m_image_ctx.cct, 20) << "retire entries complete..." << dendl;

    ldout(m_image_ctx.cct, 6) << "waiting for internal async operations" << dendl;

    // Second op tracker wait after flush completion for process_work()
    {
      Mutex::Locker locker(m_lock);
      m_wake_up_enabled = false;
    }

    m_async_op_tracker.wait(m_image_ctx, next_ctx);
  });

  // flush write to osd
  ctx = new FunctionContext([this, ctx](int r) 
  {
    ldout(m_image_ctx.cct, 20)<< "m_async_op_tracker wait complete r = " << r << dendl;

    Context *next_ctx = ctx;
    if (r < 0) {
      /* Override next_ctx status with this error */
      next_ctx = new FunctionContext([r, ctx](int _r) {
        ctx->complete(r);
      });
    }

    m_shutting_down = true;

    ldout(m_image_ctx.cct, 20) << "flusing dirty entries....." << dendl;

    flush_dirty_entries(next_ctx);
  });

  // enqueue of work queue
  ctx = new FunctionContext([this, ctx](int r) 
  {
    /* Back to RWL WQ */
    m_work_queue.queue(ctx);
  });

  // wait for IO finished.
  ctx = new FunctionContext([this, ctx](int r) 
  {
    ldout(m_image_ctx.cct, 20)<< "internal_flush complete r = " << r << dendl;

    Context *next_ctx = ctx;
    if (r < 0) {
      /* Override next_ctx status with this error */
      next_ctx = new FunctionContext([r, ctx](int _r) {
        ctx->complete(r);
      });
    }

    ldout(m_image_ctx.cct, 20) << "waiting for in flight operations" << dendl;

    // wait for in progress IOs to complete
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.wait(m_image_ctx, next_ctx);
  });

  // enqueue of work queue.
  ctx = new FunctionContext([this, ctx](int r) 
  {
      m_work_queue.queue(ctx);
  });

  /* Complete all in-flight writes before shutting down */
  internal_flush(ctx, false, false);
}

// ===========================================

/*
 * Flushes all dirty log entries, then flushes the cache below. On completion
 * there will be no dirty entries.
 *
 * Optionally invalidates the cache (RWL invalidate interface comes here with
 * invalidate=true). On completion with invalidate=true there will be no entries
 * in the log. Until the next write, all subsequent reads will complete from the
 * layer below. When invalidatingm the cache below is invalidated instead of
 * flushed.
 *
 * If discard_unflushed_writes is true, invalidate must also be true. Unflushed
 * writes are discarded instead of flushed.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish, bool invalidate, bool discard_unflushed_writes) 
{
  internal_flush(on_finish, invalidate, discard_unflushed_writes);
}

template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) 
{
  flush(on_finish, m_image_ctx.rwl_invalidate_on_flush, false);
};

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish, bool discard_unflushed_writes) 
{
  flush(on_finish, true, discard_unflushed_writes);
};

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) 
{
  invalidate(on_finish, false);
};

// ===========================================

template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents &&image_extents, bufferlist *bl, int fadvise_flags, Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  utime_t now = ceph_clock_now();

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  C_ReadRequest *read_ctx = new C_ReadRequest(cct, now, m_perfcounter, bl, on_finish);

  ceph_assert(m_initialized);

  bl->clear();

  m_perfcounter->inc(l_librbd_rwl_rd_req, 1);

  /*
   * TODO hanle fadvise flags
   *
   * The strategy here is to look up all the WriteLogMapEntries that overlap
   * this read, and iterate through those to separate this read into hits and
   * misses. 
   *
   * A new Extents object is produced here with Extents for each miss region.
   * The miss Extents is then passed on to the read cache below RWL. 
   *
   * we also produce an ImageExtentBufs for all the extents (hit or miss) in this read.
   *
   * When the read from the lower cache layer completes, we iterate
   * through the ImageExtentBufs and insert buffers for each cache hit at the
   * appropriate spot in the bufferlist returned from below for the miss
   * read. 
   * (TODO dehao : when missing read completes, then hit read. why ?)
   *
   * The buffers we insert here refer directly to regions of various
   * write log entry data buffers.
   *
   * Locking: These buffer objects hold a reference on the write log entries
   * they refer to. Log entries can't be retired until there are no references.
   * The GeneralWriteLogEntry references are released by the buffer destructor.
   */
  for (auto &extent : image_extents) 
  {
    uint64_t extent_offset = 0;

    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);

    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));

    for (auto &map_entry : map_entries) 
    {
      Extent entry_image_extent(image_extent(map_entry.block_extent));

      /* If this map entry starts after the current image extent offset ... */
      if (entry_image_extent.first > extent.first + extent_offset) 
      {
        /* add range before map_entry to miss extents */
        uint64_t miss_extent_start = extent.first + extent_offset;
        uint64_t miss_extent_length = entry_image_extent.first - miss_extent_start;
        Extent miss_extent(miss_extent_start, miss_extent_length);
        
        read_ctx->m_miss_extents.push_back(miss_extent); 
        
        ImageExtentBuf miss_extent_buf(miss_extent);
        read_ctx->m_read_extents.push_back(miss_extent_buf);
        extent_offset += miss_extent_length;
      }

      assert(entry_image_extent.first <= extent.first + extent_offset);
      uint64_t entry_offset = 0;
      /* If this map entry starts before the current image extent offset ... */
      if (entry_image_extent.first < extent.first + extent_offset) {
	/* ... compute offset into log entry for this read extent */
	entry_offset = (extent.first + extent_offset) - entry_image_extent.first;
      }
      /* This read hit ends at the end of the extent or the end of the log
	 entry, whichever is less. */
      uint64_t entry_hit_length = min(entry_image_extent.second - entry_offset,
				      extent.second - extent_offset);
      Extent hit_extent(entry_image_extent.first, entry_hit_length);
      assert(map_entry.log_entry->is_writer());

      if (map_entry.log_entry->is_write() || map_entry.log_entry->is_writesame()) {
	/* Offset of the map entry into the log entry's buffer */
	uint64_t map_entry_buffer_offset = entry_image_extent.first - map_entry.log_entry->ram_entry.image_offset_bytes;
	/* Offset into the log entry buffer of this read hit */
	uint64_t read_buffer_offset = map_entry_buffer_offset + entry_offset;
	/* Create buffer object referring to pmem pool for this read hit */
	auto write_entry = static_pointer_cast<WriteLogEntry>(map_entry.log_entry);

	/* Make a bl for this hit extent. This will add references to the write_entry->pmem_bp */
	buffer::list hit_bl;
	if (COPY_PMEM_FOR_READ) {
	  buffer::list entry_bl_copy;
	  write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
	  entry_bl_copy.copy(read_buffer_offset, entry_hit_length, hit_bl);
	} else {
	  hit_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), read_buffer_offset, entry_hit_length);
	}

	assert(hit_bl.length() == entry_hit_length);

	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, hit_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);

      } else if (map_entry.log_entry->is_discard()) {
	auto discard_entry = static_pointer_cast<DiscardLogEntry>(map_entry.log_entry);
	if (RWL_VERBOSE_LOGGING) {
	  ldout(cct, 20) << "read hit on discard entry: log_entry=" << *discard_entry << dendl;
	}
	/* Discards read as zero, so we'll construct a bufferlist of zeros */
	bufferlist zero_bl;
	zero_bl.append_zero(entry_hit_length);
	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, zero_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);
      } else {
	ldout(cct, 02) << "Reading from log entry=" << *map_entry.log_entry
		       << " unimplemented" << dendl;
	assert(false);
      }

      /* Exclude RWL hit range from buffer and extent */
      extent_offset += entry_hit_length;
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << map_entry << dendl;
      }
    } // for

    /* If the last map entry didn't consume the entire image extent ... */
    if (extent.second > extent_offset) 
    {
      /* ... add the rest of this extent to miss extents */
      uint64_t miss_extent_start = extent.first + extent_offset;
      uint64_t miss_extent_length = extent.second - extent_offset;
      Extent miss_extent(miss_extent_start, miss_extent_length);
      read_ctx->m_miss_extents.push_back(miss_extent);
      /* Add miss range to read extents */
      ImageExtentBuf miss_extent_buf(miss_extent);
      read_ctx->m_read_extents.push_back(miss_extent_buf);
      extent_offset += miss_extent_length;
    }
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "miss_extents=" << read_ctx->m_miss_extents << ", "
                   << "miss_bl=" << read_ctx->m_miss_bl << dendl;
  }

  /* All of this read comes from RWL */
  if (read_ctx->m_miss_extents.empty()) {
    read_ctx->complete(0);
  } else {
    /* Pass the read misses on to the layer below RWL */
    m_image_writeback->aio_read(std::move(read_ctx->m_miss_extents), &read_ctx->m_miss_bl, fadvise_flags, read_ctx);
  }
}

/**
 * We make a best-effort attempt to also defer until all in-progress writes complete, 
 * but we may not know about all of the writes the application considers in-progress yet,
 * due to uncertainty in the IO submission workq (multiple WQ threads
 * may allow out-of-order submission). */
template <typename I>
void ReplicatedWriteLog<I>::aio_flush(Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* May be called even if initialization fails */
  /* aio_flush completes when all previously completed writes are flushed to persistent cache. */
  /* This flush operation will not wait for writes deferred for overlap in the block guard.*/

  // Deadlock if completed here 
  if (!m_initialized) {
    ldout(cct, 05) << "never initialized" << dendl;
    m_image_ctx.op_work_queue->queue(on_finish);
    return;
  }

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  auto flush_req = make_flush_req(on_finish);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, flush_req](GuardedRequestFunctionContext &guard_ctx) 
    {
      ldout(m_image_ctx.cct, 5) << dendl;
      ceph_assert(guard_ctx.m_cell);

      flush_req->m_detained = guard_ctx.m_state.detained;

      /* We don't call flush_req->set_cell(), because the block guard will be released here */

      if (flush_req->m_detained) {}

      {
        DeferredContexts post_unlock; /* Do these when the lock below is released */
        Mutex::Locker locker(m_lock);

        if (!m_flush_seen) {
          ldout(m_image_ctx.cct, 15) << "flush seen" << dendl;
          m_flush_seen = true;
          if (!m_persist_on_flush && m_persist_on_write_until_flush) {
            m_persist_on_flush = true;
            ldout(m_image_ctx.cct, 5) << "now persisting on flush" << dendl;
          }
        }

        /* Create a new sync point if there have been writes since the last one.
         * We do not flush the caches below the RWL here.*/
        flush_new_sync_point_if_needed(flush_req, post_unlock); // ##
      }

      release_guarded_request(guard_ctx.m_cell);
    });

  detain_guarded_request(GuardedRequest(flush_req->m_image_extents_summary.block_extent(), guarded_ctx, true));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents, bufferlist&& bl, int fadvise_flags, Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  if (image_extents.size() != 1) {
    ldout(cct, 20) << "image_extents size is " << image_extents.size() << dendl;
  }

  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_wr_req, 1);

  ceph_assert(m_initialized);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  auto *write_req =
    C_WriteRequestT::create(*this, now, std::move(image_extents), std::move(bl), fadvise_flags, on_finish).get();

  m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->m_image_extents_summary.total_bytes);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req, cct](GuardedRequestFunctionContext &guard_ctx) {
      ldout(cct, 20) << dendl;
      write_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(write_req);
    });

  detain_guarded_request(GuardedRequest(write_req->m_image_extents_summary.block_extent(), guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length, bool skip_partial_discard, Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  utime_t now = ceph_clock_now();
  Extents discard_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_discard, 1);

  ceph_assert(m_initialized);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (length == 0) {
    on_finish->complete(0);
    return;
  }

  auto discard_req_sp =
    C_DiscardRequestT::create(*this, now, std::move(discard_extents), skip_partial_discard, on_finish);
  auto* discard_req = discard_req_sp.get();

  // TODO: Add discard stats
  {
    Mutex::Locker locker(m_lock);

    discard_req->op = std::make_shared<DiscardLogOperationT>(*this, m_current_sync_point, offset, length, now);
    discard_req->op->log_entry->ram_entry.sync_gen_number = m_current_sync_gen;

    if (m_persist_on_flush) {
      discard_req->op->log_entry->ram_entry.write_sequence_number = 0;
    } else {
      discard_req->op->log_entry->ram_entry.write_sequence_number = ++m_last_op_sequence_num;
      discard_req->op->log_entry->ram_entry.sequenced = 1;
    }
  }

  discard_req->op->on_write_persist = new FunctionContext([this, discard_req_sp](int r) 
  {
      ceph_assert(discard_req_sp->get_cell());

      discard_req_sp->complete_user_request(r);

      discard_req_sp->release_cell();
  });

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, discard_req](GuardedRequestFunctionContext &guard_ctx) 
  {
      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 20) << dendl;

      ceph_assert(guard_ctx.m_cell);

      discard_req->m_detained = guard_ctx.m_state.detained;
      discard_req->set_cell(guard_ctx.m_cell);

      // TODO: more missing discard stats
      if (discard_req->m_detained) {}

      alloc_and_dispatch_io_req(discard_req);
  });

  detain_guarded_request(GuardedRequest(discard_req->m_image_extents_summary.block_extent(), guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length, bufferlist&& bl, int fadvise_flags, Context *on_finish) 
{
  ldout(m_image_ctx.cct, 5) << dendl;
  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_ws, 1);
  ceph_assert(m_initialized);

  Extents ws_extents = {{offset, length}}; // don't use bufferlist length...sdh
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if ((0 == length) || bl.length() == 0) {
    on_finish->complete(0);
    return;
  }

  /* Length must be integer multiple of pattern length */
  if (length % bl.length()) {
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(m_image_ctx.cct, 06) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
                             << "offset=" << offset << ", " << "length=" << length << ", "
                             << "data_len=" << bl.length() << ", " << "on_finish=" << on_finish << dendl;

  /* 
   * A write same request is also a write request. The key difference is the
   * write same data buffer is shorter than the extent of the request. The full
   * extent will be used in the block guard, and appear in
   * m_blocks_to_log_entries_map. The data buffer allocated for the WS is only
   * as long as the length of the bl here, which is the pattern that's repeated
   * in the image for the entire length of this WS. Read hits and flushing of
   * write sames are different than normal writes. */
  auto *ws_req =
    C_WriteSameRequestT::create(*this, now, std::move(ws_extents), std::move(bl), fadvise_flags, on_finish).get();
  m_perfcounter->inc(l_librbd_rwl_ws_bytes, ws_req->m_image_extents_summary.total_bytes);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, ws_req](GuardedRequestFunctionContext &guard_ctx) {
      ws_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(ws_req);
   });

  detain_guarded_request(GuardedRequest(ws_req->m_image_extents_summary.block_extent(), guarded_ctx));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents, bufferlist&& cmp_bl, bufferlist&& bl,
                                                  uint64_t *mismatch_offset, int fadvise_flags, Context *on_finish) 
{
  ldout(m_image_ctx.cct, 5) << dendl;

  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_cmp, 1);

  ceph_assert(m_initialized);

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }

  /* A compare and write request is also a write request. We only allocate
   * resources and dispatch this write request if the compare phase succeeds. */
  auto *cw_req =
    C_CompAndWriteRequestT::create(*this, now, std::move(image_extents), std::move(cmp_bl), std::move(bl),
				   mismatch_offset, fadvise_flags, on_finish).get();

  m_perfcounter->inc(l_librbd_rwl_cmp_bytes, cw_req->m_image_extents_summary.total_bytes);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, cw_req](GuardedRequestFunctionContext &guard_ctx) 
  {
      cw_req->blockguard_acquired(guard_ctx);

      auto read_complete_ctx = new FunctionContext(
	[this, cw_req](int r) {
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
				       << "cw_req=" << cw_req << dendl;
	  }

	  /* Compare read_bl to cmp_bl to determine if this will produce a write */
	  if (cw_req->m_cmp_bl.contents_equal(cw_req->m_read_bl)) {
	    /* Compare phase succeeds. Begin write */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 05) << __func__ << " cw_req=" << cw_req << " compare matched" << dendl;
	    }
	    cw_req->m_compare_succeeded = true;
	    *cw_req->m_mismatch_offset = 0;
	    /* Continue with this request as a write. Blockguard release and
	     * user request completion handled as if this were a plain
	     * write. */
	    alloc_and_dispatch_io_req(cw_req);
	  } else {
	    /* Compare phase fails. Comp-and write ends now. */
	    if (RWL_VERBOSE_LOGGING) {
	      ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " compare failed" << dendl;
	    }
	    /* Bufferlist doesn't tell us where they differed, so we'll have to determine that here */
	    assert(cw_req->m_read_bl.length() == cw_req->m_cmp_bl.length());
	    uint64_t bl_index = 0;
	    for (bl_index = 0; bl_index < cw_req->m_cmp_bl.length(); bl_index++) {
	      if (cw_req->m_cmp_bl[bl_index] != cw_req->m_read_bl[bl_index]) {
		if (RWL_VERBOSE_LOGGING) {
		  ldout(m_image_ctx.cct, 15) << __func__ << " cw_req=" << cw_req << " mismatch at " << bl_index << dendl;
		}
		break;
	      }
	    }
	    cw_req->m_compare_succeeded = false;
	    *cw_req->m_mismatch_offset = bl_index;
	    cw_req->complete_user_request(-EILSEQ);
	    cw_req->release_cell();
	    cw_req->complete(0);
	  }
	});

      /* Read phase of comp-and-write must read through RWL */
      Extents image_extents_copy = cw_req->m_image_extents;
      aio_read(std::move(image_extents_copy), &cw_req->m_read_bl, cw_req->fadvise_flags, read_complete_ctx);
    });

  detain_guarded_request(GuardedRequest(cw_req->m_image_extents_summary.block_extent(), guarded_ctx));
}

// ===========================================

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_helper(GuardedRequest &req)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  BlockGuardCell *cell;
  ceph_assert(m_blockguard_lock.is_locked_by_me());

  int r = m_write_log_guard.detain(req.block_extent, &req, &cell);
  ceph_assert(r >= 0);
 
  // there exist overlap with req. So, directly put guard_request into op container.
  if (r > 0) {
    return nullptr;
  }

  // detain current request because there don't exist overlap
  return cell;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_barrier_helper(GuardedRequest &req)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  BlockGuardCell* cell = nullptr;

  ceph_assert(m_blockguard_lock.is_locked_by_me());

  if (m_barrier_in_progress) 
  {
    req.guard_ctx->m_state.queued = true;
    m_awaiting_barrier.push_back(req);
  } 
  else 
  {
    bool barrier = req.guard_ctx->m_state.barrier;
    if (barrier) {
      m_barrier_in_progress = true;
      req.guard_ctx->m_state.current_barrier = true;
    }

    cell = detain_guarded_request_helper(req);
    /* Only non-null if the barrier acquires the guard now */
    if (barrier) {
      m_barrier_cell = cell;
    }
  }

  return cell;
}

template <typename I>
void ReplicatedWriteLog<I>::detain_guarded_request(GuardedRequest &&req)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  BlockGuardCell *cell = nullptr;
  {
    Mutex::Locker locker(m_blockguard_lock);
    cell = detain_guarded_request_barrier_helper(req);
  }

  if (cell) {
    req.guard_ctx->m_cell = cell;
    req.guard_ctx->complete(0);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *released_cell)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  WriteLogGuard::BlockOperations block_reqs;

  {
    Mutex::Locker locker(m_blockguard_lock);

    m_write_log_guard.release(released_cell, &block_reqs);

    for (auto &req : block_reqs) 
    {
      req.guard_ctx->m_state.detained = true;
      BlockGuardCell *detained_cell = detain_guarded_request_helper(req);
      if (detained_cell) 
      {
        if (req.guard_ctx->m_state.current_barrier) 
        {
          /* The current barrier is acquiring the block guard, so now we know its cell */
          m_barrier_cell = detained_cell;
          /* detained_cell could be == released_cell here */
        }
        req.guard_ctx->m_cell = detained_cell;
        m_work_queue.queue(req.guard_ctx);
      }
    }

    // if is is barrier, need to do more deeper handle to rwl's data member....
    if (m_barrier_in_progress && (released_cell == m_barrier_cell)) 
    {
      /* The released cell is the current barrier request */
      m_barrier_in_progress = false;
      m_barrier_cell = nullptr;
      /* Move waiting requests into the blockguard. Stop if there's another barrier */
      while (!m_barrier_in_progress && !m_awaiting_barrier.empty()) 
      {
        auto &req = m_awaiting_barrier.front();
        BlockGuardCell *detained_cell = detain_guarded_request_barrier_helper(req);
        if (detained_cell) {
          req.guard_ctx->m_cell = detained_cell;
          m_work_queue.queue(req.guard_ctx);
        }
        m_awaiting_barrier.pop_front();
      }
    }

  }
}

// ===========================================

template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* multiple thread to execute this method */

  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_time = ceph_clock_now();
      auto write_entry = operation->get_write_log_entry();
      pmemobj_flush(m_internal->m_log_pool, write_entry->pmem_buffer, write_entry->write_bytes());
    }
  }

  // drain once for all 
  pmemobj_drain(m_internal->m_log_pool);

  utime_t now = ceph_clock_now();

  for (auto &operation : ops) {
    if (operation->is_write() || operation->is_writesame()) {
      operation->m_buf_persist_comp_time = now;
    } else {
      if (RWL_VERBOSE_LOGGING) {
        ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  }

}

template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GenericLogOperationsT ops;

  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */

  do {
    ops.clear();
    {
      Mutex::Locker locker(m_lock);

      /* if another thread is appending, directly return */
      if (!appending && m_appending) {
        ceph_assert(appending == false);
        ceph_assert(m_appending == true);
        return;
      }

      if (m_ops_to_append.size()) {
        appending = true;
        m_appending = true;

        auto last_in_batch = m_ops_to_append.begin();
        unsigned int ops_to_append = m_ops_to_append.size();

        if (ops_to_append > ops_appended_together) {
          ops_to_append = ops_appended_together;
        }

        std::advance(last_in_batch, ops_to_append);
        ops.splice(ops.end(), m_ops_to_append, m_ops_to_append.begin(), last_in_batch);
        // Always check again before leaving 
        ops_remain = true;
      }
      else 
      {
	ops_remain = false;
	if (appending) {
	  appending = false;
	  m_appending = false;
	}
      } //else
    } // scope

    if (ops.size()) 
    {
      Mutex::Locker locker(m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    }

    int num_ops = ops.size();

    /* New entries may be flushable. Completion will wake up flusher. */
    if (num_ops) {
      complete_op_log_entries(std::move(ops), append_result);
    }

  } while (ops_remain);
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_appender()
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_async_append_ops++;
  m_async_op_tracker.start_op();

  Context *append_ctx = new FunctionContext([this](int r) 
  {
    append_scheduled_ops();
    m_async_append_ops--;
    m_async_op_tracker.finish_op();
  });

  if (use_finishers) { 
    m_log_append_finisher.queue(append_ctx);
  } else {
    m_work_queue.queue(append_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // multi-thread to call this methods.
  // Althouht multi-thread to execute this method, but all these ops operated by multi-thread
  // belog to the same sync point. 
  // They'll all get their log entries appended,
  // and have their on_write_persist contexts completed once they and all prior log entries are persisted everywhere.

  bool need_finisher;

  GenericLogOperationsVectorT appending;
  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));

  {
    Mutex::Locker locker(m_lock);
    need_finisher = m_ops_to_append.empty() && !m_appending;
    m_ops_to_append.splice(m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    enlist_op_appender();
  }

  for (auto &op : appending) {
    op->appending();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsVectorT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GenericLogOperationsT to_append(ops.begin(), ops.end());
  schedule_append(to_append);
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationSharedPtrT op)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GenericLogOperationsT to_append { op };
  schedule_append(to_append);
}

template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* multiple thread from WQ or finisher to execute this method */

  GenericLogOperationsT ops;
  bool ops_remain = false;

  do {
    {
      ops.clear();
      Mutex::Locker locker(m_lock);

      if (m_ops_to_flush.size()) 
      {
        auto last_in_batch = m_ops_to_flush.begin();

        unsigned int ops_to_flush = m_ops_to_flush.size();
        if (ops_to_flush > ops_flushed_together) {
          ops_to_flush = ops_flushed_together;
        }

        std::advance(last_in_batch, ops_to_flush);
        ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);

        ops_remain = !m_ops_to_flush.empty();

      } else {
        ops_remain = false;
      }
    } // scope

    // launch new thread to concurrently execute current method.
    if (ops_remain) {
      enlist_op_flusher();
    }

    /* Ops subsequently scheduled for flush may finish before these, which is fine.
     * We're unconcerned with completion order until we get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      schedule_append(ops);
    }

  } while (ops_remain);

  append_scheduled_ops();
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* multiple thread to enter this method */
  /* schedule_flush_and_append call this method if condition is fulfilled */
  /* flush_then_append_scheduled_ops also call this method */

  m_async_flush_ops++;
  m_async_op_tracker.start_op();

  Context *flush_ctx = new FunctionContext([this](int r) {
    flush_then_append_scheduled_ops();
    m_async_flush_ops--;
    m_async_op_tracker.finish_op();
  });

  if (use_finishers) {
    m_persist_finisher.queue(flush_ctx);
  } else {
    m_work_queue.queue(flush_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVectorT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* multiple thread to enter this method*/

  GenericLogOperationsT to_flush(ops.begin(), ops.end());

  bool need_finisher;
  {
    Mutex::Locker locker(m_lock);
    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperationsT &ops)
{
  /* Allocate the (already reserved) write log entries for a set of operations. */

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  ceph_assert(m_log_append_lock.is_locked_by_me());

  Mutex::Locker locker(m_lock);
    
  for (auto &operation : ops) {
    uint32_t entry_index = m_first_free_entry;
    m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;

    auto &log_entry = operation->get_log_entry();

    log_entry->log_entry_index = entry_index;

    log_entry->ram_entry.entry_index = entry_index;  // ###
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;

    m_log_entries.push_back(log_entry);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVectorT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  if (ops.empty()) {
    return;
  }

  if (ops.size() > 1) {
    ceph_assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  pmemobj_flush(m_internal->m_log_pool, 
                ops.front()->get_log_entry()->pmem_entry,  // begin address on AEP
                ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))); // flush bytes 
}

/* Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.*/
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperationsT &ops)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  int ret = 0;
  GenericLogOperationsVectorT entries_to_flush;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

  ceph_assert(m_log_append_lock.is_locked_by_me());

  if (ops.empty()) { 
    return 0;
  }

  entries_to_flush.reserve(ops_appended_together);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();

  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
	  operation->get_log_entry()->log_entry_index) 
      {
        // flush AEP
        flush_op_log_entries(entries_to_flush); 
        entries_to_flush.clear();
        now = ceph_clock_now();
      }
    }

    operation->m_log_append_time = now;

    // ################ persist log entry to AEP ###################
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;

    entries_to_flush.push_back(operation); // ##
  }

  flush_op_log_entries(entries_to_flush); // call pmemobj_flush

  /* Drain once for all */
  pmemobj_drain(m_internal->m_log_pool);

  /* Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to. */
  utime_t tx_start = ceph_clock_now();

  // append_op_log_entries
  TX_BEGIN(m_internal->m_log_pool) 
  {
    D_RW(pool_root)->first_free_entry = m_first_free_entry;

    for (auto &operation : ops) 
    {
      if (operation->is_write() || operation->is_writesame()) 
      {
        auto write_op = (std::shared_ptr<WriteLogOperationT>&) operation;

        pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1); // ##

      } else {
	if (RWL_VERBOSE_LOGGING) {
	  ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
	}
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size() << " log entries (" << m_log_pool_name << ")" << dendl;
    assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(l_librbd_rwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());

  for (auto &operation : ops) {
    operation->m_log_append_comp_time = tx_end;
  }

  return ret;
}

template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GenericLogEntries dirty_entries;
  int published_reserves = 0;

  for (auto &op : ops) 
  {
    utime_t now = ceph_clock_now();
    auto log_entry = op->get_log_entry();
   
    log_entry->completed = true;

    if (op->is_writing_op()) 
    {
      op->get_gen_write_op()->sync_point->log_entry->m_writes_completed++;
      dirty_entries.push_back(log_entry);
    }

    if (op->is_write() || op->is_writesame()) {
      published_reserves++;
    }

    if (op->is_discard()) {
    }

    // ###########################
    op->complete(result); // #####
    // ###########################

    if (op->is_write()) {
      m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_buf_t, op->m_buf_persist_time - op->m_dispatch_time);
    }

    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_app_t, op->m_log_append_time - op->m_dispatch_time);
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_cmp_t, now - op->m_dispatch_time);
    m_perfcounter->hinc(l_librbd_rwl_log_op_dis_to_cmp_t_hist, utime_t(now - op->m_dispatch_time).to_nsec(), log_entry->ram_entry.write_bytes);
    if (op->is_write()) {
      utime_t buf_lat = op->m_buf_persist_comp_time - op->m_buf_persist_time;
      m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_bufc_t, buf_lat);
      m_perfcounter->hinc(l_librbd_rwl_log_op_buf_to_bufc_t_hist, buf_lat.to_nsec(), log_entry->ram_entry.write_bytes);
      m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_app_t, op->m_log_append_time - op->m_buf_persist_time);
    }
    utime_t app_lat = op->m_log_append_comp_time - op->m_log_append_time;
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_appc_t, app_lat);
    m_perfcounter->hinc(l_librbd_rwl_log_op_app_to_appc_t_hist, app_lat.to_nsec(), log_entry->ram_entry.write_bytes);
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_cmp_t, now - op->m_log_append_time);
  }

  {
    Mutex::Locker locker(m_lock);
    m_unpublished_reserves -= published_reserves;

    m_dirty_log_entries.splice(m_dirty_log_entries.end(), dirty_entries);

    /* New entries may be flushable */
    wake_up();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_complete_op_log_entries(GenericLogOperationsT &&ops, const int result)
{
  // disable
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_async_complete_ops++;
  m_async_op_tracker.start_op();

  Context *complete_ctx = new FunctionContext([this, ops=move(ops), result](int r) {
    auto captured_ops = std::move(ops);
    complete_op_log_entries(std::move(captured_ops), result);
    m_async_complete_ops--;
    m_async_op_tracker.finish_op();
  });

  if (use_finishers) {
    m_on_persist_finisher.queue(complete_ctx);
  } else {
    m_work_queue.queue(complete_ctx);
  }
}

// ===========================================

template <typename I>
void ReplicatedWriteLog<I>::dispatch_deferred_writes(void)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  /* req still on front of deferred list */
  C_BlockIORequestT* front_req = nullptr;

  /* req that was allocated, and is now off the list */
  C_BlockIORequestT* allocated_req = nullptr;

  /* front_req allocate succeeded */
  bool allocated = false;

  bool cleared_dispatching_flag = false;

  /* if we can't become the dispatcher, we'll exit */
  {
    Mutex::Locker locker(m_lock);
    if (m_dispatching_deferred_ops || !m_deferred_ios.size()) {
      return;
    }
    m_dispatching_deferred_ops = true;
  }

  /* There are ops to dispatch, and this should be the only thread dispatching them */
  {
    Mutex::Locker deferred_dispatch(m_deferred_dispatch_lock);

    do {
      {
        Mutex::Locker locker(m_lock);
        ceph_assert(m_dispatching_deferred_ops);

        if (allocated) 
        {
          /* On the 2..n-1 th time we get m_lock, front_req->alloc_resources() will
           * have succeeded, and we'll need to pop it off the deferred ops list here.*/
          ceph_assert(front_req);
          ceph_assert(!allocated_req);
          
          m_deferred_ios.pop_front(); // ##
          allocated_req = front_req;  
          front_req = nullptr;
          allocated = false;
	}

	ceph_assert(!allocated);

	if (!allocated && front_req) 
        {
          /* front_req->alloc_resources() failed on the last iteration. we'll stop dispatching. */
          front_req = nullptr;
          ceph_assert(!cleared_dispatching_flag);
          m_dispatching_deferred_ops = false;
          cleared_dispatching_flag = true;
        } 
        else 
        {
          ceph_assert(!front_req);

          if (m_deferred_ios.size()) {
            front_req = m_deferred_ios.front(); // ### 
          } else {
            ceph_assert(!cleared_dispatching_flag);
            m_dispatching_deferred_ops = false;
            cleared_dispatching_flag = true;
          }
        } // else
      } // scope

      /* Try allocating for front_req before we decide what to do with allocated_req (if any) */
      if (front_req) 
      {
        ceph_assert(!cleared_dispatching_flag);
        allocated = front_req->alloc_resources(); // ##
      }

      if (allocated_req && front_req && allocated) 
      {
        /* Push dispatch of the first allocated req to a wq */
        m_work_queue.queue(new FunctionContext([this, allocated_req](int r) {
          allocated_req->dispatch(); // ####
        }), 0);
        allocated_req = nullptr;
      }

      ceph_assert(!(allocated_req && front_req && allocated));

      /* Continue while we're still considering the front of the deferred ops list */
    } while (front_req);

    ceph_assert(!allocated);
  }

  ceph_assert(cleared_dispatching_flag);

  /* If any deferred requests were allocated, the last one will still be in allocated_req */
  if (allocated_req) {
    allocated_req->dispatch();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_write_lanes(C_WriteRequestT *write_req)
{
  /* Returns the lanes used by this write, and attempts to dispatch the next deferred write */

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(write_req->m_resources.allocated);

    m_free_lanes += write_req->m_image_extents.size();
    write_req->m_resources.allocated = false;
  }

  dispatch_deferred_writes();
}

template <typename I>
void ReplicatedWriteLog<I>::alloc_and_dispatch_io_req(C_BlockIORequestT *req)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool dispatch_here = false;
  {
    {
      Mutex::Locker locker(m_lock);
      dispatch_here = m_deferred_ios.empty();
    }

    // order ensure : once any request have been blocked, later all request will be blocked.
    if (dispatch_here) {
      dispatch_here = req->alloc_resources();
    }

    if (dispatch_here) {
      req->dispatch();
    } else {
      req->deferred();
      {
        Mutex::Locker locker(m_lock);
        m_deferred_ios.push_back(req);
      }

      dispatch_deferred_writes();
    }
  }
}

// =================================

template <typename I>
const typename ImageCache<I>::Extent ReplicatedWriteLog<I>::whole_volume_extent(void) 
{
  return typename ImageCache<I>::Extent({0, ~0});
}

template <typename I>
void ReplicatedWriteLog<I>::perf_start(std::string name) 
{
  PerfCountersBuilder plb(m_image_ctx.cct, name, l_librbd_rwl_first, l_librbd_rwl_last);

  // Latency axis configuration for op histograms, values are in nanoseconds
  PerfHistogramCommon::axis_config_d op_hist_x_axis_config{
    "Latency (nsec)",
    PerfHistogramCommon::SCALE_LOG2, ///< Latency in logarithmic scale
    0,                               ///< Start at 0
    5000,                            ///< Quantization unit is 5usec
    16,                              ///< Ranges into the mS
  };

  // Op size axis configuration for op histogram y axis, values are in bytes
  PerfHistogramCommon::axis_config_d op_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    512,                             ///< Quantization unit is 512 bytes
    16,                              ///< Writes up to >32k
  };

  // Num items configuration for op histogram y axis, values are in items
  PerfHistogramCommon::axis_config_d op_hist_y_axis_count_config{
    "Number of items",
    PerfHistogramCommon::SCALE_LINEAR, ///< Request size in linear scale
    0,                                 ///< Start at 0
    1,                                 ///< Quantization unit is 512 bytes
    32,                                ///< Writes up to >32k
  };

  plb.add_u64_counter(l_librbd_rwl_rd_req, "rd", "Reads");
  plb.add_u64_counter(l_librbd_rwl_rd_bytes, "rd_bytes", "Data size in reads");
  plb.add_time_avg(l_librbd_rwl_rd_latency, "rd_latency", "Latency of reads");

  plb.add_u64_counter(l_librbd_rwl_rd_hit_req, "hit_rd", "Reads completely hitting RWL");
  plb.add_u64_counter(l_librbd_rwl_rd_hit_bytes, "rd_hit_bytes", "Bytes read from RWL");
  plb.add_time_avg(l_librbd_rwl_rd_hit_latency, "hit_rd_latency", "Latency of read hits");

  plb.add_u64_counter(l_librbd_rwl_rd_part_hit_req, "part_hit_rd", "reads partially hitting RWL");

  plb.add_u64_counter(l_librbd_rwl_wr_req, "wr", "Writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def, "wr_def", "Writes deferred for resources");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_lanes, "wr_def_lanes", "Writes deferred for lanes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_log, "wr_def_log", "Writes deferred for log entries");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_buf, "wr_def_buf", "Writes deferred for buffers");
  plb.add_u64_counter(l_librbd_rwl_wr_req_overlap, "wr_overlap", "Writes overlapping with prior in-progress writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_queued, "wr_q_barrier", "Writes queued for prior barriers (aio_flush)");
  plb.add_u64_counter(l_librbd_rwl_wr_bytes, "wr_bytes", "Data size in writes");

  plb.add_u64_counter(l_librbd_rwl_log_ops, "log_ops", "Log appends");
  plb.add_u64_avg(l_librbd_rwl_log_op_bytes, "log_op_bytes", "Average log append bytes");

  plb.add_time_avg(l_librbd_rwl_req_arr_to_all_t, "req_arr_to_all_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_req_arr_to_dis_t, "req_arr_to_dis_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_req_all_to_dis_t, "req_all_to_dis_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_wr_latency, "wr_latency", "Latency of writes (persistent completion)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_wr_latency_hist, "wr_latency_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_wr_caller_latency, "caller_wr_latency", "Latency of write completion to caller");
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_all_t, "req_arr_to_all_nw_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_dis_t, "req_arr_to_dis_nw_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_all_to_dis_t, "req_all_to_dis_nw_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_latency, "wr_latency_nw", "Latency of writes (persistent completion) not deferred for free space");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_nowait_wr_latency_hist, "wr_latency_nw_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written for writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_caller_latency, "caller_wr_latency_nw", "Latency of write completion to callerfor writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_log_op_alloc_t, "op_alloc_t", "Average buffer pmemobj_reserve() time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_alloc_t_hist, "op_alloc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of buffer pmemobj_reserve() time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_buf_t, "op_dis_to_buf_t", "Average dispatch to buffer persist time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_app_t, "op_dis_to_app_t", "Average dispatch to log append time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_cmp_t, "op_dis_to_cmp_t", "Average dispatch to persist completion time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_dis_to_cmp_t_hist, "op_dis_to_cmp_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of op dispatch to persist complete time (nanoseconds) vs. bytes written");

  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_app_t, "op_buf_to_app_t", "Average buffer persist to log append time (write data persist/replicate + wait for append time)");
  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_bufc_t, "op_buf_to_bufc_t", "Average buffer persist time (write data persist/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_buf_to_bufc_t_hist, "op_buf_to_bufc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write buffer persist time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_cmp_t, "op_app_to_cmp_t", "Average log append to persist complete time (log entry append/replicate + wait for complete time)");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_appc_t, "op_app_to_appc_t", "Average log append to persist complete time (log entry append/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_app_to_appc_t_hist, "op_app_to_appc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of log append persist time (nanoseconds) (vs. op bytes)");

  plb.add_u64_counter(l_librbd_rwl_discard, "discard", "Discards");
  plb.add_u64_counter(l_librbd_rwl_discard_bytes, "discard_bytes", "Bytes discarded");
  plb.add_time_avg(l_librbd_rwl_discard_latency, "discard_lat", "Discard latency");

  plb.add_u64_counter(l_librbd_rwl_aio_flush, "aio_flush", "AIO flush (flush to RWL)");
  plb.add_u64_counter(l_librbd_rwl_aio_flush_def, "aio_flush_def", "AIO flushes deferred for resources");
  plb.add_time_avg(l_librbd_rwl_aio_flush_latency, "aio_flush_lat", "AIO flush latency");

  plb.add_u64_counter(l_librbd_rwl_ws,"ws", "Write Sames");
  plb.add_u64_counter(l_librbd_rwl_ws_bytes, "ws_bytes", "Write Same bytes to image");
  plb.add_time_avg(l_librbd_rwl_ws_latency, "ws_lat", "Write Same latency");

  plb.add_u64_counter(l_librbd_rwl_cmp, "cmp", "Compare and Write requests");
  plb.add_u64_counter(l_librbd_rwl_cmp_bytes, "cmp_bytes", "Compare and Write bytes compared/written");
  plb.add_time_avg(l_librbd_rwl_cmp_latency, "cmp_lat", "Compare and Write latecy");
  plb.add_u64_counter(l_librbd_rwl_cmp_fails, "cmp_fails", "Compare and Write compare fails");

  plb.add_u64_counter(l_librbd_rwl_flush, "flush", "Flush (flush RWL)");
  plb.add_u64_counter(l_librbd_rwl_invalidate_cache, "invalidate", "Invalidate RWL");
  plb.add_u64_counter(l_librbd_rwl_invalidate_discard_cache, "discard", "Discard and invalidate RWL");

  plb.add_time_avg(l_librbd_rwl_append_tx_t, "append_tx_lat", "Log append transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_append_tx_t_hist, "append_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log append transaction time (nanoseconds) vs. entries appended");
  plb.add_time_avg(l_librbd_rwl_retire_tx_t, "retire_tx_lat", "Log retire transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_retire_tx_t_hist, "retire_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log retire transaction time (nanoseconds) vs. entries retired");

  m_perfcounter = plb.create_perf_counters();
  m_image_ctx.cct->get_perfcounters_collection()->add(m_perfcounter);
}

template <typename I>
void ReplicatedWriteLog<I>::perf_stop() 
{
  ceph_assert(m_perfcounter);
  m_image_ctx.cct->get_perfcounters_collection()->remove(m_perfcounter);
  delete m_perfcounter;
}

template <typename I>
void ReplicatedWriteLog<I>::log_perf() 
{
  bufferlist bl;
  Formatter *f = Formatter::create("json-pretty");
  bl.append("Perf dump follows\n--- Begin perf dump ---\n");
  bl.append("{\n");
  stringstream ss;
  utime_t now = ceph_clock_now();
  ss << "\"test_time\": \"" << now << "\",";
  ss << "\"image\": \"" << m_image_ctx.name << "\",";
  bl.append(ss);
  bl.append("\"stats\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted(f, 0);
  f->flush(bl);
  bl.append(",\n\"histograms\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted_histograms(f, 0);
  f->flush(bl);
  delete f;
  bl.append("}\n--- End perf dump ---\n");
  bl.append('\0');
  ldout(m_image_ctx.cct, 1) << bl.c_str() << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::periodic_stats() 
{
  Mutex::Locker locker(m_lock);
  ldout(m_image_ctx.cct, 1) << "STATS: "
			    << "m_free_log_entries=" << m_free_log_entries << ", "
			    << "m_ops_to_flush=" << m_ops_to_flush.size() << ", "
			    << "m_ops_to_append=" << m_ops_to_append.size() << ", "
			    << "m_deferred_ios=" << m_deferred_ios.size() << ", "
			    << "m_log_entries=" << m_log_entries.size() << ", "
			    << "m_dirty_log_entries=" << m_dirty_log_entries.size() << ", "
			    << "m_bytes_allocated=" << m_bytes_allocated << ", "
			    << "m_bytes_cached=" << m_bytes_cached << ", "
			    << "m_bytes_dirty=" << m_bytes_dirty << ", "
			    << "bytes available=" << m_bytes_allocated_cap - m_bytes_allocated << ", "
			    << "m_current_sync_gen=" << m_current_sync_gen << ", "
			    << "m_flushed_sync_gen=" << m_flushed_sync_gen << ", "
			    << "m_flush_ops_in_flight=" << m_flush_ops_in_flight << ", "
			    << "m_flush_bytes_in_flight=" << m_flush_bytes_in_flight << ", "
			    << "m_async_flush_ops=" << m_async_flush_ops << ", "
			    << "m_async_append_ops=" << m_async_append_ops << ", "
			    << "m_async_complete_ops=" << m_async_complete_ops << ", "
			    << "m_async_null_flush_finish=" << m_async_null_flush_finish << ", "
			    << "m_async_process_work=" << m_async_process_work << ", "
			    << "m_async_op_tracker=[" << m_async_op_tracker << "]"
			    << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::arm_periodic_stats() 
{
  if (m_periodic_stats_enabled) {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.add_event_after(LOG_STATS_INTERVAL_SECONDS, new FunctionContext(
      [this](int r) {
	periodic_stats();
	arm_periodic_stats();
      }));
  }
}

template <typename I>
void ReplicatedWriteLog<I>::get_state(bool &clean, bool &empty, bool &present) 
{
  /* State of this cache to be recorded in image metadata */
  clean = m_clean;     /* true if there's nothing to flush */
  empty = m_empty;     /* true if there's nothing to invalidate */
  present = m_present; /* true if there's no storage to release */
}

// ==================================

template <typename I>
void ReplicatedWriteLog<I>::wake_up() 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  ceph_assert(m_lock.is_locked());

  ldout(cct, 20) << "======= wakeup summary ========= " << dendl;
  ldout(cct, 20) << " m_wake_up_enabled : " << m_wake_up_enabled << dendl;
  ldout(cct, 20) << " m_wake_up_requested : " << m_wake_up_requested << dendl;
  ldout(cct, 20) << " m_wake_up_scheduled : " << m_wake_up_scheduled << dendl;
  ldout(cct, 20) << "================================" << dendl;

  // wake_up is disabled during shutdown after flushing completes 
  if (!m_wake_up_enabled) {
    ldout(cct, 20) << "startup process_work fails, m_wak_up_enabled : " << m_wake_up_enabled << dendl;
    return;
  }

  if (m_wake_up_requested && m_wake_up_scheduled) {
    ldout(cct, 20) << "startup process_work fails, m_wak_up_request : " << m_wake_up_requested << " " << m_wake_up_scheduled << dendl;
    return;
  }

  // wake-up can be requested while it's already scheduled 
  m_wake_up_requested = true;

  // wake-up cannot be scheduled if it's already scheduled 
  if (m_wake_up_scheduled) {
    ceph_assert(m_wake_up_requested && m_wake_up_scheduled);
    return;
  }

  m_wake_up_scheduled = true;

  m_async_process_work++;
  m_async_op_tracker.start_op();
  m_work_queue.queue(new FunctionContext([this](int r) 
  {
    process_work();
    m_async_process_work--;
    m_async_op_tracker.finish_op();
  }), 0);
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  int max_iterations = 4;
  bool wake_up_requested = false;

  // there are three strategies to decide how to retire log entris.
  // --1-- low_water --2--  high_water --3-- aggressive_water --4--
  uint64_t aggressive_high_water_bytes = m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = m_bytes_allocated_cap * RETIRE_HIGH_WATER; // 0.5
  uint64_t low_water_bytes = m_bytes_allocated_cap * RETIRE_LOW_WATER;   // 0.4

  do {

    {
      Mutex::Locker locker(m_lock);
      m_wake_up_requested = false;
    }

    ldout(cct, 20) << "================ process_work summary ============ " << dendl;
    ldout(cct, 20) << "|| ==> retire entry " << dendl;
    ldout(cct, 20) << "|| rwl.m_alloc_failed_since_retire : " << m_alloc_failed_since_retire << dendl;
    ldout(cct, 20) << "|| rwl.m_shutting_down : " << m_shutting_down << dendl;
    ldout(cct, 20) << "|| rwl.m_invalidating : " << m_invalidating << dendl;
    ldout(cct, 20) << "|| rwl.m_bytes_allocated : " << m_bytes_allocated << dendl;
    ldout(cct, 20) << "|| rwl.m_log_entries size : " << m_log_entries.size() << dendl;
    ldout(cct, 20) << "|| config.high water bytes : " << high_water_bytes << dendl;
    ldout(cct, 20) << "|| config.low_water_bytes : " << low_water_bytes << dendl;
    ldout(cct, 20) << "--------------------------------------------------" << dendl;
    ldout(cct, 20) << "||  ==> dirty entry " << dendl;
    ldout(cct, 20) << "||  m_dirty_log_enries size : " << m_dirty_log_entries.size() << dendl;
    ldout(cct, 20) << "--------------------------------------------------" << dendl;
    ldout(cct, 20) << "||  ==> defer entry " << dendl;
    ldout(cct, 20) << "||  m_deferred_ios size : " << m_deferred_ios.size() << dendl;
    ldout(cct, 20) << "================================================== " << dendl;

    if (m_alloc_failed_since_retire || m_shutting_down || m_invalidating || m_bytes_allocated > high_water_bytes) 
    {
      int retired = 0;
      utime_t started = ceph_clock_now();

      // once in retiring status, check how to lanuch next-round retire...sdh
      while (m_alloc_failed_since_retire || m_shutting_down || m_invalidating ||
            (m_bytes_allocated > high_water_bytes) ||
            ((m_bytes_allocated > low_water_bytes) &&
              (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) 
      {
        if (!retire_entries((m_shutting_down ||  m_invalidating || (m_bytes_allocated > aggressive_high_water_bytes))
                                ? MAX_ALLOC_PER_TRANSACTION : MAX_FREE_PER_TRANSACTION)) {
          break;
        }

        retired++;

        dispatch_deferred_writes();
        process_writeback_dirty_entries();
      } // while 

    } else {
      ldout(cct, 20) << "don't need to retire log entry..." << dendl;
    }

    dispatch_deferred_writes();
    process_writeback_dirty_entries();

    {
      Mutex::Locker locker(m_lock);
      wake_up_requested = m_wake_up_requested;
    }

  } while (wake_up_requested && --max_iterations > 0);

  {
    Mutex::Locker locker(m_lock);
    m_wake_up_scheduled = false;

    /* Reschedule if it's still requested */
    if (m_wake_up_requested) {
      wake_up();
    }
  }

  ldout(cct, 20) << "process_work over..." << dendl;
}

template <typename I>
bool ReplicatedWriteLog<I>::can_retire_entry(std::shared_ptr<GenericLogEntry> log_entry) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(m_lock.is_locked_by_me());

  if (!log_entry->completed) {
    ldout(cct, 20) << "because this log entry still don't be completed, it can't be retired." << dendl;
    return false;
  }

  if (log_entry->is_write() || log_entry->is_writesame()) {

    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);
    if(!write_entry->flushed) {
      ldout(cct, 20) << "because this log entry still don't be flushed, it can't be retired." << dendl;
    }
    return (write_entry->flushed && 0 == write_entry->reader_count());
  } else {
    return true;
  }
}

/* Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. 
 * Returns true if anything was retired.
 * shutdown / process_work / internal_flush call it....sdh */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  GenericLogEntriesVector retiring_entries;

  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  Mutex::Locker retire_locker(m_log_retire_lock);

  // step 1 : get these oldest entry from m_log_entries, and remove them from m_log_entries and m_blocks_to_log_entires.
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(m_entry_reader_lock); // ##
    Mutex::Locker locker(m_lock);

    initial_first_valid_entry = m_first_valid_entry;
    first_valid_entry = m_first_valid_entry;

    auto entry = m_log_entries.front(); 
    while (!m_log_entries.empty() && (retiring_entries.size() < frees_per_tx) && can_retire_entry(entry))
    {
      ceph_assert(entry->completed); 
      ceph_assert(entry->log_entry_index == first_valid_entry);

      first_valid_entry = (first_valid_entry + 1) % m_total_log_entries;

      m_log_entries.pop_front();

      retiring_entries.push_back(entry);

      /* Remove entry from map so there will be no more readers */
      if (entry->is_write() || entry->is_writesame()) {
        auto write_entry = static_pointer_cast<WriteLogEntry>(entry);

        // remove entry from map
        m_blocks_to_log_entries.remove_log_entry(write_entry);

        ceph_assert(!write_entry->flushing);
        ceph_assert(write_entry->flushed);
        ceph_assert(!write_entry->reader_count());
        ceph_assert(!write_entry->referring_map_entries);
      }
      entry = m_log_entries.front();
    } // while 
  } // scope

  if (retiring_entries.size()) 
  {
    ldout(cct, 20) << "retiring entry size : " << retiring_entries.size() << dendl;
    
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;

    {
      uint64_t flushed_sync_gen;
      Mutex::Locker append_locker(m_log_append_lock); // ###

      {
        Mutex::Locker locker(m_lock);
        flushed_sync_gen = m_flushed_sync_gen;
      }

      tx_start = ceph_clock_now();

      // Here, why need to use transaction to retire ? 
      //
      // case 1 : crash between point_1 and point_2
      //   - when re-open rwl, pool_root maybe be in-consistent.
      //      For example : flushed_sync_gen < first_valid_entry
      // caes 2 : crash between point_2 and point_3
      //   - when re-open rwl, will lead to AEP space leaks.
      // 
      TX_BEGIN(m_internal->m_log_pool) // retire_entries 
      {
        // under normal circumstances, the member of pool root should meet below :
        // first_valid_entry <= flushed_sync_gen < first_free_entry.
        // 
        // Here, we consider this extreme case : 
        // - In executing this method, fluser maybe still are in flushing. 
        //    So, m_flushed_sync_gen maybe have been modified.
        // - If we don't update flush_ed_sync_gen in time, will lead to 
        //    first_valid_entry > flushed_sync_gen.
        //
        // hence, is is very necessary to do a try to update flush_sync_gen. 

        // point 1
	if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) 
        {
          ldout(cct, 20) << "transaction to update meta data at superblock : flushed_sync_gen : " << D_RW(pool_root)->flushed_sync_gen << " ---> " << flushed_sync_gen << dendl;
	  D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
	}

        // point 2
        ldout(cct, 20) << "transaction to update meta data at superblock : first_valid_entry : " << D_RW(pool_root)->first_valid_entry << " ---> " << first_valid_entry << dendl;
	D_RW(pool_root)->first_valid_entry = first_valid_entry; // ####

        // point 3
        ldout(cct, 20) << "transaction to free payload buffer of AEP  " << dendl;
        for (auto &entry: retiring_entries) {
	  if (entry->is_write() || entry->is_writesame()) {
	    TX_FREE(entry->ram_entry.write_data);
	  } 
	}
      } TX_ONCOMMIT {
      } TX_ONABORT {
	lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << m_log_pool_name << ")" << dendl;
	ceph_assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
      //assert(last_retired_entry_index == (first_valid_entry - 1) % m_total_log_entries);
    }

    /* Update runtime copy of first_valid, and free entries counts */
    {
      Mutex::Locker locker(m_lock);
      ceph_assert(m_first_valid_entry == initial_first_valid_entry);

      m_first_valid_entry = first_valid_entry;
      m_free_log_entries += retiring_entries.size();

      for (auto &entry: retiring_entries) 
      {
        if (entry->is_write() || entry->is_writesame()) 
        {
          ceph_assert(m_bytes_cached >= entry->write_bytes());
	  m_bytes_cached -= entry->write_bytes();

          uint64_t entry_allocation_size = entry->write_bytes();
          if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
            entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
          }
          ceph_assert(m_bytes_allocated >= entry_allocation_size);
          m_bytes_allocated -= entry_allocation_size;
	} else	if (entry->ram_entry.is_discard()) {
          /* Discards don't record any allocated or cached bytes,
           * but do record dirty bytes until they're flushed. */
	}
      }

      m_alloc_failed_since_retire = false;

      ldout(cct, 20) << "touch wake_up mechanism..." << dendl;
      wake_up();
    }
  }
  else 
  {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }

  return true;
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_entries() 
{
  // there are many ways to execute this methods, but just have two kinds of cases.
  //
  // - case 1 : m_flush_ops_in_flight != 0. 
  //    - post_unlock is empty. Namely, can_flush_entry() always return false.
  //
  // - case 2 : m_flush_ops_in_flight == 0.
  //    - get all log entries bearing with the same sync gen number. 
  //    - then concurrently flush.

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool all_clean = false;
  int flushed = 0;

  {
    DeferredContexts post_unlock;

    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);

    std::shared_ptr<GenericLogEntry> dehao_testing_entry; // dehao
    bool dehao_testing_first_time = true;
    uint64_t dehao_testing_entry_index;
    uint32_t dehao_index = 1;

    while (flushed < IN_FLIGHT_FLUSH_WRITE_LIMIT) 
    {
      Mutex::Locker locker(m_lock);

      /* Check if we should take flush complete actions */
      if (m_dirty_log_entries.empty()) {
        ldout(cct, 20) << "Nothing new to flush" << dendl;
        all_clean = !m_flush_ops_in_flight;
        break;
      }

      auto candidate = m_dirty_log_entries.front();
      ceph_assert(candidate->is_write() == true);
      bool flushable = can_flush_entry(candidate);
      if (flushable) 
      {
        dehao_testing_entry = candidate; // dehao 
        post_unlock.add(construct_flush_entry_ctx(candidate));

        if(dehao_testing_first_time) {
          dehao_testing_first_time = false;
          dehao_testing_entry_index = candidate->log_entry_index;
        } else {
          ceph_assert(candidate->log_entry_index > dehao_testing_entry_index);
          dehao_testing_entry_index = candidate->log_entry_index;
        }

        ldout(cct, 20) << "Flushable entry "<< dehao_index++ << " : [seq number, sync_gen_num] --> [" << candidate->ram_entry.write_sequence_number << "," << candidate->ram_entry.sync_gen_number << "]" << dendl;

        ceph_assert(candidate->log_entry_index == candidate->ram_entry.entry_index);
        flushed++;
      } else {
        ldout(cct, 20) << "this dirty entry can't be flushed yet : [seq number, sync_gen_num] : [" << candidate->ram_entry.write_sequence_number << " " << candidate->ram_entry.sync_gen_number << " ]" << dendl;
      }

      /* Remove if we're flushing it, or it's not a writer */
      if (flushable || !candidate->ram_entry.is_writer()) {
	m_dirty_log_entries.pop_front();
      } else {
        ldout(cct, 20) << "this dirty entry cann't be flushed yet : [seq number, sync_gen_num] : [" << candidate->ram_entry.write_sequence_number << " " << candidate->ram_entry.sync_gen_number << " ]" << dendl;
        break;
      }
    } // while

    ldout(cct, 20) << "================ flush summary ==================" << dendl;
    ldout(cct, 20) << "|| post_unlock -- flush context size : " << post_unlock.contexts.size() << "  ||" << dendl;
    ldout(cct, 20) << "|| m_dirty_log_entries size : " << m_dirty_log_entries.size() << dendl;
    if(m_dirty_log_entries.size() != 0) {
      ldout(cct, 20) << "|| Next-round begining entry : [seq number, sync_gen_num] --> [ " << m_dirty_log_entries.front()->ram_entry.write_sequence_number << " " << m_dirty_log_entries.front()->ram_entry.sync_gen_number << " ] ||" << dendl;
    }
    ldout(cct, 20) << "==================================================" << dendl;

    if(post_unlock.contexts.size() <= IN_FLIGHT_FLUSH_WRITE_LIMIT &&
       !m_dirty_log_entries.empty() && dehao_testing_entry != nullptr) 
    {
      ceph_assert(dehao_testing_entry->is_write() == true);
      ceph_assert(dehao_testing_entry->ram_entry.sync_gen_number + 1 == m_dirty_log_entries.front()->ram_entry.sync_gen_number ||
                  dehao_testing_entry->ram_entry.sync_gen_number == m_dirty_log_entries.front()->ram_entry.sync_gen_number);
    }
  }

  /* All flushing complete, drain outside lock */
  if (all_clean) 
  {
    Contexts flush_contexts;
    {
      Mutex::Locker locker(m_lock);
      flush_contexts.swap(m_flush_complete_contexts);
    }

    finish_contexts(m_image_ctx.cct, flush_contexts, 0);
  }

}

template <typename I>
bool ReplicatedWriteLog<I>::can_flush_entry(std::shared_ptr<GenericLogEntry> log_entry) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(log_entry->ram_entry.is_writer());
  ceph_assert(m_lock.is_locked_by_me());

  // invalidating express disabling all cached data
  if (m_invalidating) {
    return true;
  }

  /* For OWB we can flush entries with the same sync gen number (write between
   * aio_flush() calls) concurrently. Here we'll consider an entry flushable if
   * its sync gen number is <= the lowest sync gen number carried by all the
   * entries currently flushing.
   *
   * If the entry considered here bears a sync gen number lower than a
   * previously flushed entry, the application had to have submitted the write
   * bearing the higher gen number before the write with the lower gen number
   * completed. So, flushing these concurrently is OK.
   *
   * If the entry considered here bears a sync gen number higher than a
   * currently flushing entry, the write with the lower gen number may have
   * completed to the application before the write with the higher sync gen
   * number was submitted, and the application may rely on that completion
   * order for volume consistency. In this case the entry will not be
   * considered flushable until all the entries bearing lower sync gen numbers
   * finish flushing.
   */
  if (m_flush_ops_in_flight && 
     (log_entry->ram_entry.sync_gen_number > m_lowest_flushing_sync_gen)) 
  {
    ldout(cct, 20) <<"current entry can't be flushed as below reason : " << dendl;
    ldout(cct, 20) <<" - m_flush_ops_in_flight : " << m_flush_ops_in_flight << dendl;
    ldout(cct, 20) <<" - current_entry_sync_gen > m_lowest_flushing_sync_gen : " << log_entry->ram_entry.sync_gen_number << " > " << m_lowest_flushing_sync_gen << dendl;
    return false;
  }

  auto gen_write_entry = log_entry->get_gen_write_log_entry();

  // Sync point for this unsequenced writing entry is not persisted
  if (gen_write_entry && !gen_write_entry->ram_entry.sequenced &&
     (gen_write_entry->sync_point_entry && !gen_write_entry->sync_point_entry->completed)) 
  {
    ldout(cct, 20) <<"current entry can't be flushed as below reason : " <<dendl;
    ldout(cct, 20) <<" - cuurent entry don't be sequenced, and its sync point don't be completed " <<dendl;
    return false;
  }

  return (log_entry->completed &&
         (m_flush_ops_in_flight <= IN_FLIGHT_FLUSH_WRITE_LIMIT) &&
         (m_flush_bytes_in_flight <= IN_FLIGHT_FLUSH_BYTES_LIMIT));
}

static const bool COPY_PMEM_FOR_FLUSH = true;

template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(std::shared_ptr<GenericLogEntry> log_entry) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool invalidating = m_invalidating; // snapshot so we behave consistently
  bufferlist entry_bl;

  ceph_assert(log_entry->is_writer());

  if (!(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame())) {
    ldout(cct, 02) << "Flushing from log entry = " << *log_entry << " unimplemented" << dendl;
  }

  ceph_assert(log_entry->is_write() || log_entry->is_discard() || log_entry->is_writesame());
  ceph_assert(m_entry_reader_lock.is_locked());
  ceph_assert(m_lock.is_locked_by_me());

  if (!m_flush_ops_in_flight || (log_entry->ram_entry.sync_gen_number < m_lowest_flushing_sync_gen)) {
    m_lowest_flushing_sync_gen = log_entry->ram_entry.sync_gen_number;
  }

  auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);

  m_flush_ops_in_flight += 1;

  /* For write same this is the bytes affected bt the flush op, not the bytes transferred */
  m_flush_bytes_in_flight += gen_write_entry->ram_entry.write_bytes;

  gen_write_entry->flushing = true;

  /* Construct bl for pmem buffer now while we hold m_entry_reader_lock */
  if (invalidating || log_entry->is_discard()) {
    /* If we're invalidating the RWL, we don't actually flush, so don't create
     * the buffer.  If we're flushing a discard, we also don't need the buffer.*/
  } else {
    // check log type
    ceph_assert(log_entry->is_write() || log_entry->is_writesame());  // ##

    auto write_entry = static_pointer_cast<WriteLogEntry>(log_entry);

    // true
    if (COPY_PMEM_FOR_FLUSH) {
      /* Pass a copy of the pmem buffer to ImageWriteback (which may hang on to the bl evcen after flush()). */
      buffer::list entry_bl_copy;
      write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
      entry_bl_copy.copy(0, write_entry->write_bytes(), entry_bl);
    } else {
      /* Pass a bl that refers to the pmem buffers to ImageWriteback */
      entry_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), 0, write_entry->write_bytes());
    }
  }

  // Flush write completion action 
  Context *ctx = new FunctionContext([this, gen_write_entry, invalidating](int r) 
  {
    {
      Mutex::Locker locker(m_lock);
      gen_write_entry->flushing = false;
      
      if (r < 0) {
        lderr(m_image_ctx.cct) << "failed to flush log entry" << cpp_strerror(r) << dendl;
        m_dirty_log_entries.push_front(gen_write_entry); // ## 
      } else {
        ceph_assert(!gen_write_entry->flushed);
        gen_write_entry->flushed = true; // ##
        ceph_assert(m_bytes_dirty >= gen_write_entry->bytes_dirty());
      
        m_bytes_dirty -= gen_write_entry->bytes_dirty(); // ##
      
        // ####### update its sync point #######
        sync_point_writer_flushed(gen_write_entry->sync_point_entry);
        ldout(m_image_ctx.cct, 20) << "flushed: " << gen_write_entry << " invalidating=" << invalidating << dendl;
      }
      
      m_flush_ops_in_flight -= 1;
      m_flush_bytes_in_flight -= gen_write_entry->ram_entry.write_bytes;

      if(m_flush_ops_in_flight == 0) {
        ceph_assert(m_flush_bytes_in_flight == 0);
        ldout(m_image_ctx.cct, 20) << "one-round flush finish, So, prepare to begin next-round flush " << dendl;
      }

      wake_up();
    }
  });

  // Flush through lower cache before completing
  ctx = new FunctionContext([this, ctx](int r) 
  {
    if (r < 0) {
      lderr(m_image_ctx.cct) << "failed to flush log entry" << cpp_strerror(r) << dendl;
      ctx->complete(r);
    } else {
      ldout(m_image_ctx.cct, 20) << "startup m_writeback_cache::aio_flush..." << dendl;
      m_image_writeback->aio_flush(ctx);
    }
  });

  // When invalidating we just do the flush bookkeeping 
  if (invalidating) {
    return(ctx);
  } else {
    if (log_entry->is_write()) 
    {
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the m_image_writeback call */
      return new FunctionContext([this, gen_write_entry, entry_bl = move(entry_bl), ctx](int r) 
        {
          auto captured_entry_bl = std::move(entry_bl);

          m_image_ctx.op_work_queue->queue(new FunctionContext(
            [this, gen_write_entry, entry_bl = move(captured_entry_bl), ctx](int r) 
            {
              ldout(m_image_ctx.cct, 20) << "startup aio_image_writeback::aio_write..." << dendl;
              auto captured_entry_bl = std::move(entry_bl);
              m_image_writeback->aio_write({{gen_write_entry->ram_entry.image_offset_bytes, gen_write_entry->ram_entry.write_bytes}},
                                           std::move(captured_entry_bl), 0, ctx);
            }));
        });
    }
    else if (log_entry->is_writesame()) 
    {
      auto ws_entry = static_pointer_cast<WriteSameLogEntry>(log_entry);
      /* entry_bl is moved through the layers of lambdas here, and ultimately into the m_image_writeback call */
      return new FunctionContext([this, ws_entry, entry_bl=move(entry_bl), ctx](int r) 
        {
          auto captured_entry_bl = std::move(entry_bl);
          m_image_ctx.op_work_queue->queue(new FunctionContext(
            [this, ws_entry, entry_bl=move(captured_entry_bl), ctx](int r) 
            {
              auto captured_entry_bl = std::move(entry_bl);
              m_image_writeback->aio_writesame(ws_entry->ram_entry.image_offset_bytes,
                                               ws_entry->ram_entry.write_bytes,
                                               std::move(captured_entry_bl), 0, ctx);
            }));
        });
    } 
    else if (log_entry->is_discard()) 
    {
      return new FunctionContext([this, log_entry, ctx](int r) 
      {
	  m_image_ctx.op_work_queue->queue(new FunctionContext(
	    [this, log_entry, ctx](int r) 
            {
              auto discard_entry = static_pointer_cast<DiscardLogEntry>(log_entry);
              /* Invalidate from caches below. We always set skip_partial false
               * here, because we need all the caches below to behave the same
               * way in terms of discard granularity and alignment so they
               * remain consistent. */
              m_image_writeback->aio_discard(discard_entry->ram_entry.image_offset_bytes,
                                             discard_entry->ram_entry.write_bytes,
                                             false, ctx);
            }));
      });
    }
    else 
    {
      lderr(cct) << "Flushing from log entry=" << *log_entry << " unimplemented" << dendl;
      ceph_assert(false);
      return nullptr;
    }
  }

}

template <typename I>
void ReplicatedWriteLog<I>::sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(m_lock.is_locked_by_me());
  ceph_assert(log_entry);

  // tell the corresponding sync_point that its writes have been flushed.
  log_entry->m_writes_flushed++;

  /* If this entry might be completely flushed, look closer.
   * if all writes bearing with current sync point have been flush, we need to handle this sync points.*/
  if ((log_entry->m_writes_flushed == log_entry->m_writes) && log_entry->completed) {
    handle_flushed_sync_point(log_entry);
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  ceph_assert(m_lock.is_locked_by_me());
  ceph_assert(log_entry);

  if ((log_entry->m_writes_flushed == log_entry->m_writes) &&
       log_entry->completed &&  
       log_entry->m_prior_sync_point_flushed &&
       log_entry->m_next_sync_point_entry)
  { 
    // all writes bearing with current sync point have been flushed...sdh

    // updata next sync points member
    log_entry->m_next_sync_point_entry->m_prior_sync_point_flushed = true; // ##

    /* Don't move the flushed sync gen num backwards. */
    if (m_flushed_sync_gen < log_entry->ram_entry.sync_gen_number) {
      m_flushed_sync_gen = log_entry->ram_entry.sync_gen_number;
    }

    m_async_op_tracker.start_op();
    m_work_queue.queue(new FunctionContext([this, log_entry](int r) 
    {
      bool handled_by_next;
      {
        Mutex::Locker locker(m_lock);
        // execute this method again.
        handled_by_next = handle_flushed_sync_point(log_entry->m_next_sync_point_entry); // ##
      }

      // when this sync point is the last, need to persist flushed_index.
      if (!handled_by_next) {
         // update and persist the last flushed sync point in log Root.
        persist_last_flushed_sync_gen(); // ##
      }

      m_async_op_tracker.finish_op();
    }));

    return true;
  } // if

  return false;
}

template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen(void)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

  uint64_t flushed_sync_gen;

  Mutex::Locker append_locker(m_log_append_lock);
  {
    Mutex::Locker locker(m_lock);
    flushed_sync_gen = m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) 
  {
    TX_BEGIN(m_internal->m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen; // ##
    } TX_ONCOMMIT {
    } TX_ONABORT {
      ceph_assert(false);
    } TX_FINALLY {
    } TX_END;

  }
}

// ==============================

template <typename I>
void ReplicatedWriteLog<I>::internal_flush(Context *on_finish, bool invalidate, bool discard_unflushed_writes) 
{
  // if want to discard any unflushed data, this request must derive from invalidate interface 
  if (discard_unflushed_writes) {
    ceph_assert(invalidate);
  }

  // May be called even if initialization fails 
  if (!m_initialized) {
    ldout(m_image_ctx.cct, 05) << "never initialized" << dendl;
    m_image_ctx.op_work_queue->queue(on_finish); // dead lock if completed here...scott
    return;
  }

  /* Flush/invalidate must pass through block guard to ensure all layers of
   * cache are consistently flush/invalidated. 
   *
   * This ensures no in-flight write leaves some layers with valid regions, 
   * which may later produce inconsistent read results. */
  GuardedRequestFunctionContext *guarded_ctx = new GuardedRequestFunctionContext(
      [this, on_finish, invalidate, discard_unflushed_writes](GuardedRequestFunctionContext &guard_ctx) 
  {
        DeferredContexts on_exit;
        ceph_assert(guard_ctx.m_cell);

        // release_block_request
        Context *ctx = new FunctionContext(
          [this, cell=guard_ctx.m_cell, invalidate, discard_unflushed_writes, on_finish](int r) 
          {
            Mutex::Locker locker(m_lock);
            m_invalidating = false;

            if (invalidate) {
              ceph_assert(m_log_entries.size() == 0);
            }

            ceph_assert(m_dirty_log_entries.size() == 0);
            m_image_ctx.op_work_queue->queue(on_finish, r);
            release_guarded_request(cell);
          });

        // execute the lower cache interface.
        ctx = new FunctionContext(
          [this, ctx, invalidate, discard_unflushed_writes](int r) 
          {
            Context *next_ctx = ctx;

            // Override on_finish status with this error 
            if (r < 0) {
              next_ctx = new FunctionContext([r, ctx](int _r) {
                ctx->complete(r);
              });
            }

            if (invalidate) 
            {
              {
                Mutex::Locker locker(m_lock);
                ceph_assert(m_dirty_log_entries.size() == 0);

                if (discard_unflushed_writes) {
                  ceph_assert(m_invalidating);
                } else {
                /* If discard_unflushed_writes was false, we should only now be
                 * setting m_invalidating. All writes are now flushed.  with
                 * m_invalidating set, retire_entries() will proceed without
                 * the normal limits that keep it from interfering with
                 * appending new writes (we hold the block guard, so that can't be happening).*/
                  ceph_assert(!m_invalidating);
                  m_invalidating = true;
                }
              } // scope 

              // Discards all RWL entries
              while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) {} // ######

	      // Invalidate from caches below
              m_image_writeback->invalidate(next_ctx); // ######
            } else {
              {
                Mutex::Locker locker(m_lock);
                ceph_assert(m_dirty_log_entries.size() == 0); // ############
                ceph_assert(!m_invalidating);
	      }

              m_image_writeback->flush(next_ctx); // ####### 
            }
          });

        // wrapper flush_dirty_entries
	ctx = new FunctionContext(
          [this, ctx, discard_unflushed_writes](int r) 
          {
            /* 
             * If discard_unflushed_writes was true, m_invalidating should be set now.
             *
             * With m_invalidating set, flush discards everything in the dirty
             * entries list without writing them to OSDs. It also waits for
             * in-flight flushes to complete, and keeps the flushing stats consistent.
             *
             * If discard_unflushed_writes was false, this is a normal flush. 
             */
             {
                Mutex::Locker locker(m_lock);
                ceph_assert(m_invalidating == discard_unflushed_writes);
              }

              // if rwl have any dirty data, recursively call flush_dirty_entries until it become clean.
              flush_dirty_entries(ctx);
          });

        Mutex::Locker locker(m_lock);

        // i think that invalidate express disbale all cached data.
        if (discard_unflushed_writes) {
          m_invalidating = true;
        }

        /* Even if we're throwing everything away, but we want the last entry to
         * be a sync point so we can cleanly resume.
         *
         * Also, the blockguard only guarantees the replication of this op
         * can't overlap with prior ops. It doesn't guarantee those are all
         * completed and eligible for flush & retire, which we require here.*/
        auto flush_req = make_flush_req(ctx);

        flush_new_sync_point_if_needed(flush_req, on_exit);
      });

  BlockExtent invalidate_block_extent(block_extent(whole_volume_extent()));

  // when this block request begin to execute, this have two kinds of meanings as below : 
  //  - all write prior this flush have been appended. Now, the whole image don't have any writes in-flight.
  //  - when this flush is in-flight, all write of this image will be deferred
  detain_guarded_request(GuardedRequest(invalidate_block_extent, guarded_ctx, true));
}

/*
 * RWL internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 *
 * Librbd internal flushes will arrive at flush(invalidate=false, discard=false), 
 * and traverse the block guard to ensure in-flight writes are flushed.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_dirty_entries(Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool all_clean = false;
  {
    Mutex::Locker locker(m_lock);
    all_clean = (0 == m_flush_ops_in_flight && m_dirty_log_entries.empty());
  }

  // Complete without holding m_lock 
  if (all_clean) {
    on_finish->complete(0);
  } else {
    Mutex::Locker locker(m_lock);
    // process_writeback_dirty_entries will execute this context container...
    m_flush_complete_contexts.push_back(new FunctionContext([this, on_finish](int r) {
      flush_dirty_entries(on_finish);
    }));

    // call wake_up with m_lock
    wake_up();
  }
}

// ==============================

template <typename I>
C_FlushRequest<ReplicatedWriteLog<I>>* ReplicatedWriteLog<I>::make_flush_req(Context *on_finish) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  utime_t flush_begins = ceph_clock_now();
  bufferlist bl;

  // whole image extents
  auto *flush_req = C_FlushRequestT::create(*this, flush_begins, 
                                            Extents({whole_volume_extent()}), 
                                            std::move(bl), 0, on_finish).get();
  return flush_req;
}

/* make a new sync point and flush the previous during initialization, when there may or may
 * not be a previous sync point */
template <typename I>
void ReplicatedWriteLog<I>::init_flush_new_sync_point(DeferredContexts &later) 
{
  // because this new sync point don't take owner of any previous log, will directy be dispatched once activity.
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(m_lock.is_locked_by_me());

  // don't use this method after init
  ceph_assert(!m_initialized);

  // First sync point since start
  if (!m_current_sync_point) {
    new_sync_point(later);
  } else {
    flush_new_sync_point(nullptr, later);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point_if_needed(C_FlushRequestT *flush_req, 
                                                           DeferredContexts &later) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(m_lock.is_locked_by_me());

  // If there have been writes since the last sync point 
  if (m_current_sync_point->log_entry->m_writes) {
    flush_new_sync_point(flush_req, later);
  } else {
    if (m_current_sync_point->earlier_sync_point) 
    { 
      ldout(m_image_ctx.cct, 5) << "There have been no writes to the current sync point, " << 
                                   "Because pervious sync point hasn't completed, " <<
                                   "complete current flush after the earlier sync point.no alloc or dispatch needed " << dendl;

      m_current_sync_point->earlier_sync_point->m_on_sync_point_persisted.push_back(flush_req);
      ceph_assert(m_current_sync_point->earlier_sync_point->m_append_scheduled);
    } 
    else 
    {
      ldout(m_image_ctx.cct, 5) << "There have been no writes to the current sync point, " << 
                                   "the previous sync point has already completed and been appended, " << 
                                   "the current sync point has no writes, so this flush has nothing to wait for. "<<
                                   "this flush completes now." << dendl;
      later.add(flush_req);
    }
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point(C_FlushRequestT *flush_req, 
                                                 DeferredContexts &later) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(m_lock.is_locked_by_me());

  if (!flush_req) 
  {
    m_async_null_flush_finish++;
    m_async_op_tracker.start_op();

    Context *flush_ctx = new FunctionContext([this](int r) {
      m_async_null_flush_finish--;
      m_async_op_tracker.finish_op();
    });

    flush_req = make_flush_req(flush_ctx);
    flush_req->m_internal = true;
  }

  new_sync_point(later);

  std::shared_ptr<SyncPointT> to_append = m_current_sync_point->earlier_sync_point;
  ceph_assert(to_append);
  flush_req->to_append = to_append;
  to_append->m_append_scheduled = true;

  /* All prior sync points that are still in this list must already be scheduled for append */
  std::shared_ptr<SyncPointT> previous = to_append->earlier_sync_point;
  int temp = 0;
  while (previous) {
    temp++; 
    ceph_assert(previous->m_append_scheduled);
    previous = previous->earlier_sync_point;
  }
  ceph_assert(temp == 0); // sdh

 /* Defer current C_FlushRequest executing.
  * When the m_sync_point_persist Gather completes, this sync point can be appended.  
  * The only sub for this Gather is the finisher Context for m_prior_log_entries_persisted, 
  * which records the result of the Gather in the sync point, and completes. */
  to_append->m_sync_point_persist->set_finisher(new FunctionContext([this, flush_req](int r) {
     alloc_and_dispatch_io_req(flush_req);
   }));

  /* The m_sync_point_persist Gather has all the subs it will ever have, and
   * now has its finisher. If the sub is already complete, activation will
   * complete the Gather. The finisher will acquire m_lock, so we'll activate
   * this when we release m_lock.*/
  later.add(new FunctionContext([this, to_append](int r) {
    to_append->m_sync_point_persist->activate();
  }));

  /* The flush request completes when the sync point persists */
  to_append->m_on_sync_point_persisted.push_back(flush_req);
}

template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(DeferredContexts &later) 
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  std::shared_ptr<SyncPointT> old_sync_point = m_current_sync_point;
  std::shared_ptr<SyncPointT> new_sync_point;

  ceph_assert(m_lock.is_locked_by_me());

  /* The first time this is called, if this is a newly created log, 
   * this makes the first sync gen number we'll use 1. 
   *
   * On the first call for a re-opened log m_current_sync_gen will be the highest
   * gen number from all the sync point entries found in the re-opened
   * log, and this advances to the next sync gen number. */
  ++m_current_sync_gen;

  new_sync_point = std::make_shared<SyncPointT>(*this, m_current_sync_gen);

  // the only one change to modify m_current_sync_point
  m_current_sync_point = new_sync_point;

  /* If this log has been re-opened, old_sync_point will initially be nullptr, 
   * but m_current_sync_gen may not be zero. 
   *
   * create relationship between new sync point and old sync point. */
  if (old_sync_point) 
  {
    new_sync_point->earlier_sync_point = old_sync_point;
    old_sync_point->later_sync_point = new_sync_point;

    old_sync_point->log_entry->m_next_sync_point_entry = new_sync_point->log_entry;

    old_sync_point->m_final_op_sequence_num = m_last_op_sequence_num;
    new_sync_point->log_entry->m_prior_sync_point_flushed = false;

    /* Append of new sync point deferred until old sync point is appending */

    /* if old sync point is in appending status, express that log position have been guaranteed 
     * so, don't need to add stub to old sync point....sdh */
    if (!old_sync_point->m_appending) {
      ldout(cct, 20) << "because pervious sync point still isn't in appending, defer current sync point." << dendl;
      old_sync_point->m_on_sync_point_appending.push_back(
        new_sync_point->m_prior_log_entries_persisted->new_sub());
    }

    /* This sync point will acquire no more sub-ops. Activation needs to acquire m_lock, so defer to later*/
    later.add(new FunctionContext([this, old_sync_point](int r) {
      old_sync_point->m_prior_log_entries_persisted->activate(); // ##
    }));
  }

  // create relationship inside sync point.
  Context *sync_point_persist_ready = new_sync_point->m_sync_point_persist->new_sub();

  new_sync_point->m_prior_log_entries_persisted->set_finisher(
    new FunctionContext([this, new_sync_point, sync_point_persist_ready](int r) 
  {
    new_sync_point->m_prior_log_entries_persisted_result = r;
    new_sync_point->m_prior_log_entries_persisted_complete = true;
    sync_point_persist_ready->complete(r);
  }));

  if (old_sync_point) {
    ldout(cct,6) << "new sync point = [" << *m_current_sync_point << "], prior = [" << *old_sync_point << "]" << dendl;
  } else {
    ldout(cct,6) << "first sync point = [" << *m_current_sync_point << "]" << dendl;
  }
}


} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
