#ifndef RWL_LOG_ENTRY_H
#define RWL_LOG_ENTRY_H

#include <libpmemobj.h>
#include "../ReplicatedWriteLog.h"
#include "common/perf_counters.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "SharedPtrContext.h"
#include <map>
#include <vector>

#include "PmemLayout.h"

namespace librbd {
namespace cache {
namespace rwl {

class GenericLogEntry
{
public:
  WriteLogPmemEntry ram_entry; // copy from pmem to memory
  WriteLogPmemEntry *pmem_entry = nullptr; // pointer of log entry of AEP

  // load_existing_entries / alloc_op_log_entries
  // derive from m_first_free_entry
  uint32_t log_entry_index = 0;

  // complete_op_log_entries / load_existing_entries
  bool completed = false;

  GenericLogEntry(const uint64_t image_offset_bytes = 0, const uint64_t write_bytes = 0)
    : ram_entry(image_offset_bytes, write_bytes) {};

  virtual ~GenericLogEntry() {};

  GenericLogEntry(const GenericLogEntry&) = delete;
  GenericLogEntry &operator=(const GenericLogEntry&) = delete;

  virtual unsigned int write_bytes() = 0;

  // check log entry type
  bool is_sync_point() { return ram_entry.is_sync_point(); }
  bool is_discard() { return ram_entry.is_discard(); }
  bool is_writesame() { return ram_entry.is_writesame(); }
  bool is_write() { return ram_entry.is_write(); }
  bool is_writer() { return ram_entry.is_writer(); }

  virtual const GenericLogEntry* get_log_entry() = 0;

  virtual const SyncPointLogEntry* get_sync_point_log_entry() { return nullptr; }
  virtual const GeneralWriteLogEntry* get_gen_write_log_entry() { return nullptr; }
  virtual const WriteLogEntry* get_write_log_entry() { return nullptr; }
  virtual const WriteSameLogEntry* get_write_same_log_entry() { return nullptr; }
  virtual const DiscardLogEntry* get_discard_log_entry() { return nullptr; }

  virtual std::ostream &format(std::ostream &os) const
  {
    os << "ram_entry=[" << ram_entry << "], " << "pmem_entry=" << (void*)pmem_entry << ", "
       << "log_entry_index=" << log_entry_index << ", " << "completed=" << completed;
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const GenericLogEntry &entry)
  {
    return entry.format(os);
  }
};

class SyncPointLogEntry : public GenericLogEntry
{
public:
  /* Writing entries using this sync gen number
   * add : load_existing_entries / WriteLogOperation construction */
  std::atomic<unsigned int> m_writes = {0};

  /* Total bytes for all writing entries using this sync gen number */
  std::atomic<uint64_t> m_bytes = {0};

  /* Writing entries using this sync gen number that have completed
   * add : load_existing_entries / complete_op_log_entries.*/
  std::atomic<unsigned int> m_writes_completed = {0};

  /* Writing entries using this sync gen number that have completed flushing to the writeback interface
   * add : load_existing_entries / sync_point_writer_flushed */
  std::atomic<unsigned int> m_writes_flushed = {0};

  /* All writing entries using all prior sync gen numbers have been flushed
   * set true : load_existing_entries / handle_flushed_sync_point */
  std::atomic<bool> m_prior_sync_point_flushed = {true};

  /* assign value : load_existing_entries / new_sync_point */
  std::shared_ptr<SyncPointLogEntry> m_next_sync_point_entry = nullptr;

  SyncPointLogEntry(const uint64_t sync_gen_number) {
    ram_entry.sync_gen_number = sync_gen_number;
    ram_entry.sync_point = 1; // express this log entry is sync point...
  };

  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;

  // this log entry have 0 byte data
  virtual inline unsigned int write_bytes() { return 0; }

  const GenericLogEntry* get_log_entry() override { return get_sync_point_log_entry(); }
  const SyncPointLogEntry* get_sync_point_log_entry() override { return this; }

  std::ostream &format(std::ostream &os) const {
    os << "(Sync Point) ";
    GenericLogEntry::format(os);
    os << ", " << "m_writes=" << m_writes << ", " << "m_bytes=" << m_bytes << ", "
       << "m_writes_completed=" << m_writes_completed << ", " << "m_writes_flushed=" << m_writes_flushed << ", "
       << "m_prior_sync_point_flushed=" << m_prior_sync_point_flushed << ", " << "m_next_sync_point_entry=" << m_next_sync_point_entry;
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const SyncPointLogEntry &entry) {
    return entry.format(os);
  }
};

class GeneralWriteLogEntry : public GenericLogEntry
{
private:
  friend class WriteLogEntry;
  friend class DiscardLogEntry;

public:
  uint32_t referring_map_entries = 0;

  // set true  : construct_flush_entry_ctx
  // set false : construct_flush_entry_ctx
  bool flushing = false;

  // set true  : construct_flush_entry_ctx / load_existring_etnries
  // set false : new_sync_point
  bool flushed = false; /* or invalidated */

  // this entry belog to which sync_point_entry
  std::shared_ptr<SyncPointLogEntry> sync_point_entry;

  // ## need sync_point_entry at construction ##
  GeneralWriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                       const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) {}

  GeneralWriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(nullptr) {}

  GeneralWriteLogEntry(const GeneralWriteLogEntry&) = delete;
  GeneralWriteLogEntry &operator=(const GeneralWriteLogEntry&) = delete;

  /* The valid bytes in this ops data buffer. Discard and WS override. */
  virtual inline unsigned int write_bytes() {
    return ram_entry.write_bytes;
  };

  /* The bytes in the image this op makes dirty. Discard and WS override. */
  virtual inline unsigned int bytes_dirty() {
    return write_bytes();
  };

  const BlockExtent block_extent() { return ram_entry.block_extent(); }
  const GenericLogEntry* get_log_entry() override { return get_gen_write_log_entry(); }
  const GeneralWriteLogEntry* get_gen_write_log_entry() override { return this; }

  // operate referring_map_entries
  uint32_t get_map_ref() { return(referring_map_entries); }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }

  std::ostream &format(std::ostream &os) const {
    GenericLogEntry::format(os);
    os << ", " << "sync_point_entry=[";
    if (sync_point_entry) {
      os << *sync_point_entry;
    } else {
      os << "nullptr";
    }
    os << "], " << "referring_map_entries=" << referring_map_entries << ", "
       << "flushing=" << flushing << ", " << "flushed=" << flushed;
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const GeneralWriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteLogEntry : public GeneralWriteLogEntry
{
protected:
  buffer::ptr pmem_bp;  // its built-in space is in AEP....sdh
  buffer::list pmem_bl; // its built-in space is in AEP....sdh

  std::atomic<int> bl_refs = {0}; /* The refs held on pmem_bp by pmem_bl */

  void init_pmem_bp() {
    ceph_assert(!pmem_bp.get_raw());
    pmem_bp = buffer::ptr(buffer::create_static(this->write_bytes(), (char*)pmem_buffer));
  }

  /* Write same will override */
  virtual void init_bl(buffer::ptr &bp, buffer::list &bl) {
    bl.append(bp);
  }

  // ##
  void init_pmem_bl() {
    pmem_bl.clear();

    init_pmem_bp(); // ##
    ceph_assert(pmem_bp.get_raw());

    int before_bl = pmem_bp.raw_nref();
    this->init_bl(pmem_bp, pmem_bl); // ##
    int after_bl = pmem_bp.raw_nref();

    bl_refs = after_bl - before_bl;
  }

public:
  uint8_t *pmem_buffer = nullptr; // ## AEP's pointer

  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {}

  WriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {}

  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;

  const BlockExtent block_extent();

  unsigned int reader_count() {
    if (pmem_bp.get_raw()) {
      return (pmem_bp.raw_nref() - bl_refs - 1);
    } else {
      return 0;
    }
  }
  /* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
  buffer::list &get_pmem_bl(Mutex &entry_bl_lock) {
    if (0 == bl_refs) {
      Mutex::Locker locker(entry_bl_lock);
      if (0 == bl_refs) {
        init_pmem_bl(); // ##
      }
      ceph_assert(0 != bl_refs);
    }
    return pmem_bl;
  };

  /* Constructs a new bl containing copies of pmem_bp */
  void copy_pmem_bl(Mutex &entry_bl_lock, bufferlist *out_bl) {
    this->get_pmem_bl(entry_bl_lock);
    /* pmem_bp is now initialized */
    buffer::ptr cloned_bp(pmem_bp.clone());
    out_bl->clear();
    this->init_bl(cloned_bp, *out_bl);
  }

  virtual const GenericLogEntry* get_log_entry() override { return get_write_log_entry(); }
  const WriteLogEntry* get_write_log_entry() override { return this; }

  std::ostream &format(std::ostream &os) const {
    os << "(Write) ";
    GeneralWriteLogEntry::format(os);
    os << ", " << "pmem_buffer=" << (void*)pmem_buffer << ", ";
    os << "pmem_bp=" << pmem_bp << ", ";
    os << "pmem_bl=" << pmem_bl << ", ";
    os << "bl_refs=" << bl_refs;
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const WriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteSameLogEntry : public WriteLogEntry
{
protected:

  void init_bl(buffer::ptr &bp, buffer::list &bl) override {
    for (uint64_t i = 0; i < ram_entry.write_bytes / ram_entry.ws_datalen; i++) {
      bl.append(bp);
    }

    int trailing_partial = ram_entry.write_bytes % ram_entry.ws_datalen;
    if (trailing_partial) {
      bl.append(bp, 0, trailing_partial);
    }
  };

public:
  WriteSameLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                    const uint64_t image_offset_bytes, const uint64_t write_bytes, const uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes)
  {
      ram_entry.writesame = 1;
      ram_entry.ws_datalen = data_length;
  };

  WriteSameLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes, const uint32_t data_length)
    : WriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
      ram_entry.writesame = 1;
      ram_entry.ws_datalen = data_length;
  };

  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;

  /* The valid bytes in this ops data buffer. */
  virtual inline unsigned int write_bytes() override {
    return ram_entry.ws_datalen;
  };

  /* The bytes in the image this op makes dirty. */
  virtual inline unsigned int bytes_dirty() {
    return ram_entry.write_bytes;
  };

  const BlockExtent block_extent();

  const GenericLogEntry* get_log_entry() override { return get_write_same_log_entry(); }
  const WriteSameLogEntry* get_write_same_log_entry() override { return this; }

  std::ostream &format(std::ostream &os) const {
    os << "(WriteSame) ";
    WriteLogEntry::format(os);
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const WriteSameLogEntry &entry) {
    return entry.format(os);
  }
};

class DiscardLogEntry : public GeneralWriteLogEntry
{
public:
  DiscardLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
                  const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };

  DiscardLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };

  DiscardLogEntry(const DiscardLogEntry&) = delete;
  DiscardLogEntry &operator=(const DiscardLogEntry&) = delete;

  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. */
    return 0;
  };

  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };

  const BlockExtent block_extent();
  const GenericLogEntry* get_log_entry() { return get_discard_log_entry(); }
  const DiscardLogEntry* get_discard_log_entry() { return this; }

  std::ostream &format(std::ostream &os) const {
    os << "(Discard) ";
    GeneralWriteLogEntry::format(os);
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const DiscardLogEntry &entry) {
    return entry.format(os);
  }
};


} // namespace rwl
} // namespace cache
} // namespace rwl

#endif
