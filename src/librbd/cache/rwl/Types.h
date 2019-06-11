#ifndef RWL_TYPES_H
#define RWL_TYPES_H


enum {
  l_librbd_rwl_first = 26500,

  // All read requests
  l_librbd_rwl_rd_req,           // read requests
  l_librbd_rwl_rd_bytes,         // bytes read
  l_librbd_rwl_rd_latency,       // average req completion latency

  // Read requests completed from RWL (no misses)
  l_librbd_rwl_rd_hit_req,       // read requests
  l_librbd_rwl_rd_hit_bytes,     // bytes read
  l_librbd_rwl_rd_hit_latency,   // average req completion latency

  // Reed requests with hit and miss extents
  l_librbd_rwl_rd_part_hit_req,  // read ops

  // All write requests
  l_librbd_rwl_wr_req,             // write requests
  l_librbd_rwl_wr_req_def,         // write requests deferred for resources
  l_librbd_rwl_wr_req_def_lanes,   // write requests deferred for lanes
  l_librbd_rwl_wr_req_def_log,     // write requests deferred for log entries
  l_librbd_rwl_wr_req_def_buf,     // write requests deferred for buffer space
  l_librbd_rwl_wr_req_overlap,     // write requests detained for overlap
  l_librbd_rwl_wr_req_queued,      // write requests queued for prior barrier
  l_librbd_rwl_wr_bytes,           // bytes written

  // Write log operations (1 .. n per request that appends to the log)
  l_librbd_rwl_log_ops,            // log append ops
  l_librbd_rwl_log_op_bytes,       // average bytes written per log op

  /*

   Req and op average latencies to the beginning of and over various phases:

   +------------------------------+------+-------------------------------+
   | Phase                        | Name | Description                   |
   +------------------------------+------+-------------------------------+
   | Arrive at RWL                | arr  |Arrives as a request           |
   +------------------------------+------+-------------------------------+
   | Allocate resources           | all  |time spent in block guard for  |
   |                              |      |overlap sequencing occurs      |
   |                              |      |before this point              |
   +------------------------------+------+-------------------------------+
   | Dispatch                     | dis  |Op lifetime begins here. time  |
   |                              |      |spent in allocation waiting for|
   |                              |      |resources occurs before this   |
   |                              |      |point                          |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist and   | buf  |time spent queued for          |
   |replicate                     |      |replication occurs before here |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist       | bufc |bufc - buf is just the persist |
   |complete                      |      |time                           |
   +------------------------------+------+-------------------------------+
   | Log append                   | app  |time spent queued for append   |
   |                              |      |occurs before here             |
   +------------------------------+------+-------------------------------+
   | Append complete              | appc |appc - app is just the time    |
   |                              |      |spent in the append operation  |
   +------------------------------+------+-------------------------------+
   | Complete                     | cmp  |write persisted, replicated,   |
   |                              |      |and globally visible           |
   +------------------------------+------+-------------------------------+

  */

  /* Request times */
  l_librbd_rwl_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_wr_caller_latency,  // average req completion (to caller) latency

  /* Request times for requests that never waited for space*/
  l_librbd_rwl_nowait_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_nowait_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_nowait_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_nowait_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_nowait_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_nowait_wr_caller_latency,  // average req completion (to caller) latency

  /* Log operation times */
  l_librbd_rwl_log_op_alloc_t,      // elapsed time of pmemobj_reserve()
  l_librbd_rwl_log_op_alloc_t_hist, // Histogram of elapsed time of pmemobj_reserve()

  l_librbd_rwl_log_op_dis_to_buf_t, // dispatch to buffer persist elapsed time
  l_librbd_rwl_log_op_dis_to_app_t, // dispatch to log append elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t, // dispatch to persist completion elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t_hist, // Histogram of dispatch to persist completion elapsed time

  l_librbd_rwl_log_op_buf_to_app_t, // data buf persist + append wait time
  l_librbd_rwl_log_op_buf_to_bufc_t,// data buf persist / replicate elapsed time
  l_librbd_rwl_log_op_buf_to_bufc_t_hist,// data buf persist time vs bytes histogram
  l_librbd_rwl_log_op_app_to_cmp_t, // log entry append + completion wait time
  l_librbd_rwl_log_op_app_to_appc_t, // log entry append / replicate elapsed time
  l_librbd_rwl_log_op_app_to_appc_t_hist, // log entry append time (vs. op bytes) histogram

  l_librbd_rwl_discard,
  l_librbd_rwl_discard_bytes,
  l_librbd_rwl_discard_latency,

  l_librbd_rwl_aio_flush,
  l_librbd_rwl_aio_flush_def,
  l_librbd_rwl_aio_flush_latency,
  l_librbd_rwl_ws,
  l_librbd_rwl_ws_bytes, // Bytes modified by write same, probably much larger than WS payload bytes
  l_librbd_rwl_ws_latency,

  l_librbd_rwl_cmp,
  l_librbd_rwl_cmp_bytes,
  l_librbd_rwl_cmp_latency,
  l_librbd_rwl_cmp_fails,

  l_librbd_rwl_flush,
  l_librbd_rwl_invalidate_cache,
  l_librbd_rwl_invalidate_discard_cache,

  l_librbd_rwl_append_tx_t,
  l_librbd_rwl_retire_tx_t,
  l_librbd_rwl_append_tx_t_hist,
  l_librbd_rwl_retire_tx_t_hist,

  l_librbd_rwl_last,
};


namespace librbd {

struct ImageCtx;

namespace cache {

template <typename ImageCtxT = librbd::ImageCtx>
class ReplicatedWriteLog;

struct WriteBufferAllocation;

namespace rwl
{
typedef std::list<Context*> Contexts;
typedef std::vector<Context*> ContextsV;

static const uint32_t MIN_WRITE_ALLOC_SIZE = 512;

/* Enables use of dedicated finishers for some RWL work */
static const bool use_finishers = false;

static const int IN_FLIGHT_FLUSH_WRITE_LIMIT = 64;
static const int IN_FLIGHT_FLUSH_BYTES_LIMIT = (1 * 1024 * 1024);

/* Limit work between sync points */
static const uint64_t MAX_WRITES_PER_SYNC_POINT = 256;
static const uint64_t MAX_BYTES_PER_SYNC_POINT = (1024 * 1024 * 8);

static const double LOG_STATS_INTERVAL_SECONDS = 5;

/**** Write log entries ****/

static const unsigned long int MAX_ALLOC_PER_TRANSACTION = 8;
static const unsigned long int MAX_FREE_PER_TRANSACTION = 1;
static const unsigned int MAX_CONCURRENT_WRITES = 256;
static const uint64_t DEFAULT_POOL_SIZE = 1u<<30;


static const uint64_t MIN_POOL_SIZE = DEFAULT_POOL_SIZE;
static const double USABLE_SIZE = (7.0 / 10);
static const uint64_t BLOCK_ALLOC_OVERHEAD_BYTES = 16;
static const uint8_t RWL_POOL_VERSION = 1;
static const uint64_t MAX_LOG_ENTRIES = (1024 * 1024);
//static const uint64_t MAX_LOG_ENTRIES = (1024 * 128);
static const double AGGRESSIVE_RETIRE_HIGH_WATER = 0.75;
static const double RETIRE_HIGH_WATER = 0.50;
static const double RETIRE_LOW_WATER = 0.40;
static const int RETIRE_BATCH_TIME_LIMIT_MS = 250;

class SyncPointLogEntry;
class GeneralWriteLogEntry;
class WriteLogEntry;
class WriteSameLogEntry;
class DiscardLogEntry;
class GenericLogEntry;

typedef std::list<std::shared_ptr<GeneralWriteLogEntry>> GeneralWriteLogEntries;
typedef std::list<std::shared_ptr<WriteLogEntry>> WriteLogEntries;
typedef std::list<std::shared_ptr<GenericLogEntry>> GenericLogEntries;
typedef std::vector<std::shared_ptr<GenericLogEntry>> GenericLogEntriesVector;




template <typename T>
class WriteLogOperationSet;

template <typename T> 
class GenericLogOperation;

template <typename T>
class WriteLogOperation;

template <typename T>
class GeneralWriteLogOperation;

template <typename T>
class SyncPointLogOperation;

template <typename T>
class DiscardLogOperation;

template <typename T>
class WriteSameLogOperation;

template <typename T>
class SyncPointLogOperation;


template <typename T>
using GenericLogOperationSharedPtr = std::shared_ptr<GenericLogOperation<T>>;

template <typename T>
using GenericLogOperations = std::list<GenericLogOperationSharedPtr<T>>;

template <typename T>
using GenericLogOperationsVector = std::vector<GenericLogOperationSharedPtr<T>>;

template <typename T>
using WriteLogOperationSharedPtr = std::shared_ptr<WriteLogOperation<T>>;

template <typename T>
using WriteLogOperations = std::list<WriteLogOperationSharedPtr<T>>;

template <typename T>
using WriteSameLogOperationSharedPtr = std::shared_ptr<WriteSameLogOperation<T>>;

// =================
struct GuardedRequest;


typedef librbd::BlockGuard<GuardedRequest> WriteLogGuard;
typedef LogMapEntry<GeneralWriteLogEntry> WriteLogMapEntry;
typedef LogMapEntries<GeneralWriteLogEntry> WriteLogMapEntries;
typedef LogMap<GeneralWriteLogEntry, GeneralWriteLogEntries> WriteLogMap;

template <typename T>
struct C_GuardedBlockIORequest;

class DeferredContexts;


/*
template <typename T>
struct C_BlockIORequest;

template <typename T>
struct C_WriteRequest;

template <typename T>
struct C_FlushRequest;

template <typename T>
struct C_DiscardRequest;

template <typename T>
struct C_WriteSameRequest;

template <typename T>
struct C_CompAndWriteRequest;

// Prototype pmem-based, client-side, replicated write log
class ReplicatedWriteLogInternal;
*/




} // namespace rwl
} // namespace cache
} // namespace librbd


namespace librbd {
namespace cache {

template <typename T>
struct C_BlockIORequest;

template <typename T>
struct C_WriteRequest;

template <typename T>
struct C_FlushRequest;

template <typename T>
struct C_DiscardRequest;

template <typename T>
struct C_WriteSameRequest;

template <typename T>
struct C_CompAndWriteRequest;

// Prototype pmem-based, client-side, replicated write log
class ReplicatedWriteLogInternal;



} // cache
} // librbd







#endif
