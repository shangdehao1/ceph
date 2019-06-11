#ifndef RWL_OPERATION_H
#define RWL_OPERATION_H

namespace librbd {
namespace cache {
namespace rwl {

template <typename T>
class GenericLogOperation
{
public:
  T &rwl;

  utime_t m_dispatch_time;         // When op created
  utime_t m_buf_persist_time;      // When buffer persist begins
  utime_t m_buf_persist_comp_time; // When buffer persist completes
  utime_t m_log_append_time;       // When log append begins
  utime_t m_log_append_comp_time;  // When log append completes

  GenericLogOperation(T &rwl, const utime_t dispatch_time)
    : rwl(rwl), m_dispatch_time(dispatch_time) {}

  virtual ~GenericLogOperation() {};

  GenericLogOperation(const GenericLogOperation&) = delete;
  GenericLogOperation &operator=(const GenericLogOperation&) = delete;

  virtual std::ostream &format(std::ostream &os) const {
    os << "m_dispatch_time=[" << m_dispatch_time << "], "
       << "m_buf_persist_time=[" << m_buf_persist_time << "], "
       << "m_buf_persist_comp_time=[" << m_buf_persist_comp_time << "], "
       << "m_log_append_time=[" << m_log_append_time << "], "
       << "m_log_append_comp_time=[" << m_log_append_comp_time << "], ";
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const GenericLogOperation &op) {
    return op.format(os);
  }

  virtual const std::shared_ptr<GenericLogEntry> get_log_entry() = 0;
  virtual const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return nullptr; }
  virtual const std::shared_ptr<GeneralWriteLogEntry> get_gen_write_log_entry() { return nullptr; }
  virtual const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return nullptr; }
  virtual const std::shared_ptr<DiscardLogEntry> get_discard_log_entry() { return nullptr; }
  virtual const std::shared_ptr<WriteSameLogEntry> get_write_same_log_entry() { return nullptr; }

  // when rwl.schedule_append put these ops into rwl.m_ops_to_append,
  // we say that current ops is in appending status.
  virtual void appending() = 0;

  virtual void complete(int r) = 0;

  virtual bool is_write() { return false; }
  virtual bool is_sync_point() { return false; }
  virtual bool is_discard() { return false; }
  virtual bool is_writesame() { return false; }
  virtual bool is_writing_op() { return false; }

  virtual GeneralWriteLogOperation<T> *get_gen_write_op() { return nullptr; };
  virtual WriteLogOperation<T> *get_write_op() { return nullptr; };
};

template <typename T>
class SyncPointLogOperation : public GenericLogOperation<T>
{
public:
  using GenericLogOperation<T>::rwl;

  std::shared_ptr<SyncPoint<T>> sync_point;

  SyncPointLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point, const utime_t dispatch_time)
    : GenericLogOperation<T>(rwl, dispatch_time),
      sync_point(sync_point)
  {}

  ~SyncPointLogOperation() {}

  SyncPointLogOperation(const SyncPointLogOperation&) = delete;
  SyncPointLogOperation &operator=(const SyncPointLogOperation&) = delete;

  std::ostream &format(std::ostream &os) const {
    os << "(Sync Point) ";
    GenericLogOperation<T>::format(os);
    os << ", " << "sync_point=[" << *sync_point << "]";
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const SyncPointLogOperation<T> &op) {
    return op.format(os);
  }

  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_sync_point_log_entry(); }
  const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return sync_point->log_entry; }
  bool is_sync_point() { return true; }

  /* execute all contexts derived from sync_point->m_on_sync_appending */
  void appending() 
  {
    std::vector<Context*> appending_contexts;
    ceph_assert(sync_point);

    {
      Mutex::Locker locker(rwl.m_lock);
      if (!sync_point->m_appending) {
        sync_point->m_appending = true;
      }
      appending_contexts.swap(sync_point->m_on_sync_point_appending);
    }

    for (auto &ctx : appending_contexts) {
      ctx->complete(0);
    }
  }

  /* execute all contexts derived from sync_point->m_on_sync_persist */
  void complete(int result)
  {
    std::vector<Context*> persisted_contexts;
    ceph_assert(sync_point);
  
    {
      Mutex::Locker locker(rwl.m_lock);
  
      ceph_assert(sync_point->later_sync_point);
      ceph_assert(sync_point->later_sync_point->earlier_sync_point == sync_point);
  
      sync_point->later_sync_point->earlier_sync_point = nullptr;
    }
  
    /* Do append now in case completion occurred before the normal append callback executed, and to handle
     * on_append work that was queued after the sync point entered the appending state. */
    appending();
  
    {
      Mutex::Locker locker(rwl.m_lock);
  
      /* The flush request that scheduled this op will be one of these contexts */
      persisted_contexts.swap(sync_point->m_on_sync_point_persisted);
  
      rwl.handle_flushed_sync_point(sync_point->log_entry);
    }
  
    for (auto &ctx : persisted_contexts) {
      ctx->complete(result);
    }
  }

};

template <typename T>
class GeneralWriteLogOperation : public GenericLogOperation<T>
{
private:
  friend class WriteLogOperation<T>;
  friend class DiscardLogOperation<T>;
  Mutex m_lock;

public:
  using GenericLogOperation<T>::rwl;

  /* derive from construcation */
  std::shared_ptr<SyncPoint<T>> sync_point;

  /* Completion for things waiting on this write's position in the log to be guaranteed */
  Context *on_write_append = nullptr;

  /* Completion for things waiting on this write to persist */
  Context *on_write_persist = nullptr;

  GeneralWriteLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point, const utime_t dispatch_time)
   : GenericLogOperation<T>(rwl, dispatch_time),
     m_lock("librbd::cache::rwl::GeneralWriteLogOperation::m_lock"),
     sync_point(sync_point) {}

  ~GeneralWriteLogOperation() {}

  GeneralWriteLogOperation(const GeneralWriteLogOperation&) = delete;
  GeneralWriteLogOperation &operator=(const GeneralWriteLogOperation&) = delete;

  std::ostream &format(std::ostream &os) const {
    GenericLogOperation<T>::format(os);
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const GeneralWriteLogOperation<T> &op) {
    return op.format(os);
  }

  GeneralWriteLogOperation<T> *get_gen_write_op() { return this; };

  bool is_writing_op() { return true; }

  void appending()
  {
    Context *on_append = nullptr;
    {
      Mutex::Locker locker(m_lock);
      on_append = on_write_append;
      on_write_append = nullptr;
    }
  
    if (on_append)
    {
      on_append->complete(0);
    }
  }

  void complete(int result)
  {
    appending();
  
    Context *on_persist = nullptr;
    {
      Mutex::Locker locker(m_lock);
      on_persist = on_write_persist;
      on_write_persist = nullptr;
    }
  
    if (on_persist) {
      on_persist->complete(result);
    }
  }

};

template <typename T>
class WriteLogOperation : public GeneralWriteLogOperation<T>
{
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;

  std::shared_ptr<WriteLogEntry> log_entry;

  bufferlist bl;
  WriteBufferAllocation *buffer_alloc = nullptr;

  WriteLogOperation(WriteLogOperationSet<T> &set,
                    const uint64_t image_offset_bytes,
                    const uint64_t write_bytes)
    : GeneralWriteLogOperation<T>(set.rwl, set.sync_point, set.m_dispatch_time),
      log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes))
   {
     on_write_append = set.m_extent_ops_appending->new_sub();
     on_write_persist = set.m_extent_ops_persist->new_sub();
   
     log_entry->sync_point_entry->m_writes++;
     log_entry->sync_point_entry->m_bytes += write_bytes;
   }

  ~WriteLogOperation() {}

  WriteLogOperation(const WriteLogOperation&) = delete;
  WriteLogOperation &operator=(const WriteLogOperation&) = delete;

  std::ostream &format(std::ostream &os) const
  {
    os << "(Write) ";
    GeneralWriteLogOperation<T>::format(os);
    os << ", ";
    if (log_entry) {
      os << "log_entry=[" << *log_entry << "], ";
    } else {
      os << "log_entry=nullptr, ";
    }
  
    os << "bl=[" << bl << "]," << "buffer_alloc=" << buffer_alloc;
  
    return os;
  };


  friend std::ostream &operator<<(std::ostream &os, const WriteLogOperation<T> &op) {
    return op.format(os);
  }

  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_write_log_entry(); }
  const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return log_entry; }
  WriteLogOperation<T> *get_write_op() override { return this; }
  bool is_write() { return true; }
};

template <typename T>
class WriteSameLogOperation : public WriteLogOperation<T>
{
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;
  using WriteLogOperation<T>::log_entry;
  using WriteLogOperation<T>::bl;
  using WriteLogOperation<T>::buffer_alloc;

  WriteSameLogOperation(WriteLogOperationSet<T> &set,
                       const uint64_t image_offset_bytes,
                       const uint64_t write_bytes, const uint32_t data_len)
   : WriteLogOperation<T>(set, image_offset_bytes, write_bytes)
  {
    auto ws_entry = std::make_shared<WriteSameLogEntry>(set.sync_point->log_entry,
                                                        image_offset_bytes, write_bytes, data_len);
    log_entry = static_pointer_cast<WriteLogEntry>(ws_entry);
  }

  ~WriteSameLogOperation() {}

  WriteSameLogOperation(const WriteSameLogOperation&) = delete;
  WriteSameLogOperation &operator=(const WriteSameLogOperation&) = delete;

  std::ostream &format(std::ostream &os) const {
    os << "(Write Same) ";
    WriteLogOperation<T>::format(os);
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const WriteSameLogOperation<T> &op) {
    return op.format(os);
  }

  const std::shared_ptr<GenericLogEntry> get_log_entry() {
    return get_write_same_log_entry();
  }
  const std::shared_ptr<WriteSameLogEntry> get_write_same_log_entry() {
    return static_pointer_cast<WriteSameLogEntry>(log_entry);
  }
  bool is_write() { return false; }
  bool is_writesame() { return true; }
};

template <typename T>
class DiscardLogOperation : public GeneralWriteLogOperation<T>
{
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;

  std::shared_ptr<DiscardLogEntry> log_entry;

  DiscardLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point,
                      const uint64_t image_offset_bytes, const uint64_t write_bytes,
                      const utime_t dispatch_time)
    : GeneralWriteLogOperation<T>(rwl, sync_point, dispatch_time),
      log_entry(std::make_shared<DiscardLogEntry>(sync_point->log_entry, image_offset_bytes, write_bytes))
  {
    on_write_append = sync_point->m_prior_log_entries_persisted->new_sub();
    on_write_persist = nullptr;
  
    log_entry->sync_point_entry->m_writes++;
    log_entry->sync_point_entry->m_bytes += write_bytes;
  }

  ~DiscardLogOperation() {}

  DiscardLogOperation(const DiscardLogOperation&) = delete;
  DiscardLogOperation &operator=(const DiscardLogOperation&) = delete;

  std::ostream &format(std::ostream &os) const
  {
    os << "(Discard) ";
    GeneralWriteLogOperation<T>::format(os);
    os << ", ";
    if (log_entry) {
      os << "log_entry=[" << *log_entry << "], ";
    } else {
      os << "log_entry=nullptr, ";
    }
    return os;
  };

  friend std::ostream &operator<<(std::ostream &os, const DiscardLogOperation<T> &op) {
    return op.format(os);
  }

  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_discard_log_entry(); }
  const std::shared_ptr<DiscardLogEntry> get_discard_log_entry() { return log_entry; }
  bool is_discard() { return true; }
};

template <typename T>
class WriteLogOperationSet
{
public:
  T &rwl;

  /* in blocks */
  BlockExtent m_extent;

  /* C_WriteRequest */
  Context* m_on_finish;

  bool m_persist_on_flush;
  BlockGuardCell *m_cell;

  /* Once all ops become appending status, execute this finisher. */
  C_Gather *m_extent_ops_appending;

  /* this stub derive from SyncPoint::m_prior_log_entries_persisted.
   * it will be executed in m_extent_ops_appending. */
  Context *m_on_ops_appending;

  /* execute this finisher when two requirements below is meet.
   *  - al ops become complete status
   *  - m_extent_ops_appending have been executed */
  C_Gather *m_extent_ops_persist;

  /* currently, it is nullptr */
  Context *m_on_ops_persist;

  // just one stub for sync_point::m_prior_log_entries_persisted
  // finially, detailed at new_sync_point

  // SyncPointLogOperation / GenericWriteLogOpeartion
  GenericLogOperationsVector<T> operations;

  utime_t m_dispatch_time; // When set created

  std::shared_ptr<SyncPoint<T>> sync_point;

  WriteLogOperationSet(T &rwl, const utime_t dispatched,
                       std::shared_ptr<SyncPoint<T>> sync_point,
                       const bool persist_on_flush, BlockExtent extent, Context *on_finish)
    : rwl(rwl),
      m_extent(extent),
      m_on_finish(on_finish),
      m_persist_on_flush(persist_on_flush),
      m_dispatch_time(dispatched),
      sync_point(sync_point)
  {
    m_on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
    m_on_ops_persist = nullptr;
  
    m_extent_ops_persist = new C_Gather(rwl.m_image_ctx.cct,
      new FunctionContext( [this](int r)
    {
      if (m_on_ops_persist) {
        m_on_ops_persist->complete(r);
      }
  
      m_on_finish->complete(r);
    }));
  
    auto appending_persist_sub = m_extent_ops_persist->new_sub();
  
    m_extent_ops_appending = new C_Gather(rwl.m_image_ctx.cct,
      new FunctionContext( [this, appending_persist_sub](int r)
    {
      m_on_ops_appending->complete(r);
      appending_persist_sub->complete(r);
    }));
  }

  ~WriteLogOperationSet() {}

  WriteLogOperationSet(const WriteLogOperationSet&) = delete;
  WriteLogOperationSet &operator=(const WriteLogOperationSet&) = delete;

  friend std::ostream &operator<<(std::ostream &os, const WriteLogOperationSet<T> &s) {
    os << "m_extent=[" << s.m_extent.block_start << "," << s.m_extent.block_end << "] "
       << "m_on_finish=" << s.m_on_finish << ", " << "m_cell=" << (void*)s.m_cell << ", "
       << "m_extent_ops_appending=[" << s.m_extent_ops_appending << ", "
       << "m_extent_ops_persist=[" << s.m_extent_ops_persist << "]";
    return os;
  };
};


// =================================================================== 
// =================================================================== 

/***** back up *****************
template <typename T>
SyncPointLogOperation<T>::SyncPointLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point,
                                                const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time),
    sync_point(sync_point)
{}

template <typename T>
SyncPointLogOperation<T>::~SyncPointLogOperation() {}

template <typename T>
void SyncPointLogOperation<T>::appending()
{
  std::vector<Context*> appending_contexts;
  ceph_assert(sync_point);

  {
    Mutex::Locker locker(rwl.m_lock);
    if (!sync_point->m_appending) {
      sync_point->m_appending = true;
    }
    appending_contexts.swap(sync_point->m_on_sync_point_appending);
  }

  for (auto &ctx : appending_contexts) {
    ctx->complete(0);
  }
}

template <typename T>
void SyncPointLogOperation<T>::complete(int result)
{
  std::vector<Context*> persisted_contexts;
  ceph_assert(sync_point);

  {
    Mutex::Locker locker(rwl.m_lock);

    ceph_assert(sync_point->later_sync_point);
    ceph_assert(sync_point->later_sync_point->earlier_sync_point == sync_point);

    sync_point->later_sync_point->earlier_sync_point = nullptr;
  }

  // Do append now in case completion occurred before the normal append callback executed, and to handle
  // on_append work that was queued after the sync point entered the appending state. 
  appending();

  {
    Mutex::Locker locker(rwl.m_lock);

    // The flush request that scheduled this op will be one of these contexts 
    persisted_contexts.swap(sync_point->m_on_sync_point_persisted);

    rwl.handle_flushed_sync_point(sync_point->log_entry);
  }

  for (auto &ctx : persisted_contexts) {
    ctx->complete(result);
  }
}


// ===============================


template <typename T>
GeneralWriteLogOperation<T>::GeneralWriteLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point,
                                                      const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time),
    m_lock("librbd::cache::rwl::GeneralWriteLogOperation::m_lock"),
    sync_point(sync_point) {}

template <typename T>
GeneralWriteLogOperation<T>::~GeneralWriteLogOperation() {}

// Called when the write log operation is appending and its log position is guaranteed
// Completion for things waiting on this write's position in the log to be guaranteed 
template <typename T>
void GeneralWriteLogOperation<T>::appending()
{
  Context *on_append = nullptr;
  {
    Mutex::Locker locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }

  if (on_append)
  {
    on_append->complete(0);
  }
}

// Called when the write log operation is completed in all log replicas 
template <typename T>
void GeneralWriteLogOperation<T>::complete(int result)
{
  appending();

  Context *on_persist = nullptr;
  {
    Mutex::Locker locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }

  if (on_persist) {
    on_persist->complete(result);
  }
}



// ===================================


template <typename T>
DiscardLogOperation<T>::DiscardLogOperation(T &rwl, std::shared_ptr<SyncPoint<T>> sync_point,
                                            const uint64_t image_offset_bytes, const uint64_t write_bytes,
                                            const utime_t dispatch_time)
  : GeneralWriteLogOperation<T>(rwl, sync_point, dispatch_time),
    log_entry(std::make_shared<DiscardLogEntry>(sync_point->log_entry, image_offset_bytes, write_bytes))
{
  on_write_append = sync_point->m_prior_log_entries_persisted->new_sub();
  on_write_persist = nullptr;

  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
DiscardLogOperation<T>::~DiscardLogOperation() {}

template <typename T>
std::ostream &DiscardLogOperation<T>::format(std::ostream &os) const
{
  os << "(Discard) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  return os;
};

// ===================================


template <typename T>
WriteLogOperation<T>::WriteLogOperation(WriteLogOperationSet<T> &set,
                                        uint64_t image_offset_bytes, uint64_t write_bytes)
 : GeneralWriteLogOperation<T>(set.rwl, set.sync_point, set.m_dispatch_time),
   log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes))
{
  on_write_append = set.m_extent_ops_appending->new_sub();
  on_write_persist = set.m_extent_ops_persist->new_sub();

  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
WriteLogOperation<T>::~WriteLogOperation() {}

template <typename T>
std::ostream &WriteLogOperation<T>::format(std::ostream &os) const
{
  os << "(Write) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }

  os << "bl=[" << bl << "]," << "buffer_alloc=" << buffer_alloc;

  return os;
};


// ===================================


template <typename T>
WriteSameLogOperation<T>::WriteSameLogOperation(WriteLogOperationSet<T> &set, uint64_t image_offset_bytes,
                                                uint64_t write_bytes, uint32_t data_len)
 : WriteLogOperation<T>(set, image_offset_bytes, write_bytes)
{
  auto ws_entry = std::make_shared<WriteSameLogEntry>(set.sync_point->log_entry,
                                                      image_offset_bytes, write_bytes, data_len);
  log_entry = static_pointer_cast<WriteLogEntry>(ws_entry);
}

template <typename T>
WriteSameLogOperation<T>::~WriteSameLogOperation() {}


// ===================================


template <typename T>
WriteLogOperationSet<T>::WriteLogOperationSet(T &rwl, utime_t dispatched,
                                              std::shared_ptr<SyncPoint<T>> sync_point,
                                              bool persist_on_flush,
                                              BlockExtent extent,
                                              Context *on_finish)
  : rwl(rwl),
    m_extent(extent),
    m_on_finish(on_finish),
    m_persist_on_flush(persist_on_flush),
    m_dispatch_time(dispatched),
    sync_point(sync_point)
{
  m_on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
  m_on_ops_persist = nullptr;

  m_extent_ops_persist = new C_Gather(rwl.m_image_ctx.cct,
    new FunctionContext( [this](int r)
  {
    if (m_on_ops_persist) {
      m_on_ops_persist->complete(r);
    }

    m_on_finish->complete(r);
  }));

  auto appending_persist_sub = m_extent_ops_persist->new_sub();

  m_extent_ops_appending = new C_Gather(rwl.m_image_ctx.cct,
    new FunctionContext( [this, appending_persist_sub](int r)
  {
    m_on_ops_appending->complete(r);
    appending_persist_sub->complete(r);
  }));
}

template <typename T>
WriteLogOperationSet<T>::~WriteLogOperationSet() {}
*/



} // namespace rwl
} // namespace cache
} // namespace librbd

#endif
