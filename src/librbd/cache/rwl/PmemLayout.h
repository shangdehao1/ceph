#ifndef RWL_PMEM_LAYOUT_H
#define RWL_PMEM_LAYOUT_H

#include <libpmemobj.h>
#include "librbd/cache/ReplicatedWriteLog.h"

namespace librbd {
namespace cache {
namespace rwl {

POBJ_LAYOUT_BEGIN(rbd_rwl);
POBJ_LAYOUT_ROOT(rbd_rwl, struct WriteLogPoolRoot);
POBJ_LAYOUT_TOID(rbd_rwl, uint8_t);
POBJ_LAYOUT_TOID(rbd_rwl, struct WriteLogPmemEntry);
POBJ_LAYOUT_END(rbd_rwl);

struct WriteLogPmemEntry
{
  uint64_t sync_gen_number = 0;
  uint64_t write_sequence_number = 0;

  uint64_t image_offset_bytes;
  uint64_t write_bytes;

  struct {
    uint8_t entry_valid :1; /* if 0, this entry is free */
    uint8_t sync_point :1;  /* No data. No write sequence number. Marks sync point for this sync gen number */
    uint8_t sequenced :1;   /* write sequence number is valid */
    uint8_t has_data :1;    /* write_data field is valid (else ignore), for example C_WriteRequest */
    uint8_t discard :1;     /* has_data will be 0 if this is a discard */
    uint8_t writesame :1;   /* ws_datalen indicates length of data at write_bytes */
  };

  // WriteBufferAllocation.buffer_oid
  TOID(uint8_t) write_data; // ## refer to 512 bytes pmem space.

  uint32_t ws_datalen = 0;  /* Length of data buffer (writesame only) */
  uint32_t entry_index = 0; /* For debug consistency check. Can be removed if we need the space */

  WriteLogPmemEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : image_offset_bytes(image_offset_bytes), write_bytes(write_bytes),
      entry_valid(0), sync_point(0), sequenced(0), has_data(0), discard(0), writesame(0)
  {}

  const BlockExtent block_extent();

  bool is_sync_point() { return sync_point; }
  bool is_discard() { return discard; }
  bool is_writesame() { return writesame; }

  // Log entry is a basic write.
  bool is_write() {
    return !is_sync_point() && !is_discard() && !is_writesame();
  }

  // Log entry is any type that writes data 
  bool is_writer() {
    return is_write() || is_discard() || is_writesame();
  }

  const uint64_t get_offset_bytes() { return image_offset_bytes; }
  const uint64_t get_write_bytes() { return write_bytes; }

  friend std::ostream &operator<<(std::ostream &os, const WriteLogPmemEntry &entry) {
    os << "entry_valid=" << (bool)entry.entry_valid << ", "
       << "sync_point=" << (bool)entry.sync_point << ", "
       << "sequenced=" << (bool)entry.sequenced << ", "
       << "has_data=" << (bool)entry.has_data << ", "
       << "discard=" << (bool)entry.discard << ", "
       << "writesame=" << (bool)entry.writesame << ", "
       << "sync_gen_number=" << entry.sync_gen_number << ", "
       << "write_sequence_number=" << entry.write_sequence_number << ", "
       << "image_offset_bytes=" << entry.image_offset_bytes << ", "
       << "write_bytes=" << entry.write_bytes << ", "
       << "ws_datalen=" << entry.ws_datalen << ", "
       << "entry_index=" << entry.entry_index;
    return os;
  };
};

static_assert(sizeof(WriteLogPmemEntry) == 64);
static_assert(sizeof(TOID(uint8_t)) == 16);

struct WriteLogPoolRoot
{
  union {
    struct {
      uint8_t layout_version;    /* Version of this structure (RWL_POOL_VERSION) */
    };
    uint64_t _u64;
  } header;

  TOID(struct WriteLogPmemEntry) log_entries;   /* contiguous array of log entries */
  uint32_t num_log_entries;     /* entry number */

  uint64_t flushed_sync_gen;    /* All writing entries with this or a lower sync gen number are flushed. */
  uint32_t first_free_entry;    /* Entry following the newest valid entry */
  uint32_t first_valid_entry;   /* Index of the oldest valid entry in the log */

  uint64_t pool_size;           /* pool size in AEP */
  uint32_t block_size;          /* block size */
};


} // namespace rwl
} // namespace cache
} // namespace rwl

#endif
