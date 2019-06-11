#ifndef RWL_BLOCK_GUARDED_H
#define RWL_BLOCK_GUARDED_H


namespace librbd {
namespace cache {
namespace rwl {

struct BlockGuardReqState
{
  /* This is a barrier request */
  bool barrier = false;

  /* This is the currently active barrier */
  bool current_barrier = false;

  bool detained = false;

  /* Queued for barrier */
  bool queued = false;

  friend std::ostream &operator<<(std::ostream &os, const BlockGuardReqState &r) {
    os << "barrier=" << r.barrier << ", " << "current_barrier=" << r.current_barrier << ", "
       << "detained=" << r.detained << ", " << "queued=" << r.queued;
    return os;
  };
};

class GuardedRequestFunctionContext : public Context
{
private:
  /* lambda function */
  boost::function<void(GuardedRequestFunctionContext&)> m_callback;

  void finish(int r) override {
    ceph_assert(m_cell);
    m_callback(*this);
  }


public:

  BlockGuardCell *m_cell = nullptr;
  BlockGuardReqState m_state;

  GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback)
    : m_callback(std::move(callback)){}

  ~GuardedRequestFunctionContext(void) {};

  GuardedRequestFunctionContext(const GuardedRequestFunctionContext&) = delete;
  GuardedRequestFunctionContext &operator=(const GuardedRequestFunctionContext&) = delete;
};

struct GuardedRequest
{
  const BlockExtent block_extent;

  /* Work to do when guard on range obtained */
  GuardedRequestFunctionContext *guard_ctx;

  GuardedRequest(const BlockExtent block_extent,
                 GuardedRequestFunctionContext *on_guard_acquire,
                 bool barrier = false)
    : block_extent(block_extent), guard_ctx(on_guard_acquire)
  {
    // the following situation, barrier is true.
    //  - aio_flush
    //  - internal_flush
    guard_ctx->m_state.barrier = barrier;
  }

  friend std::ostream &operator<<(std::ostream &os, const GuardedRequest &r) {
    os << "guard_ctx->m_state=[" << r.guard_ctx->m_state << "], "
       << "block_extent.block_start=" << r.block_extent.block_start << ", "
       << "block_extent.block_start=" << r.block_extent.block_end;
    return os;
  };
};


} // rwl
} // cache
} // librbd

#endif
