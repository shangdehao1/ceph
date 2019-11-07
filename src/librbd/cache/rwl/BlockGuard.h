#ifndef RWL_BLOCK_GUARDED_H
#define RWL_BLOCK_GUARDED_H

namespace librbd {
namespace cache {
namespace rwl {

struct BlockGuardReqState
{
  bool barrier = false;
  bool current_barrier = false;
  bool detained = false;
  bool queued = false; // enqueue m_awaiting_barrier

  friend std::ostream &operator<<(std::ostream &os, const BlockGuardReqState &r) {
    os << "barrier=" << r.barrier << ", " << "current_barrier=" << r.current_barrier << ", "
       << "detained=" << r.detained << ", " << "queued=" << r.queued;
    return os;
  };
};

class GuardedRequestFunctionContext : public Context
{
private:
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
  /* [image_first_byte, image_last_byte] */
  const BlockExtent block_extent;

  /* work to do when guard on range obtained */
  GuardedRequestFunctionContext *guard_ctx;

  GuardedRequest(const BlockExtent block_extent,
                 GuardedRequestFunctionContext *on_guard_acquire,
                 bool barrier = false)
    : block_extent(block_extent), guard_ctx(on_guard_acquire)
  {
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

/*

BlockGuardCell rwl.m_barrier_cell 
bool rwl.m_barrier_in_progress
queue<GuardedRequest> rwl.m_awaiting_barrier


                                 rwl_internface
                                       |
                                       v
                                C_WriteRequest
                                 |          | 
                                 v          v
                          BlockExtent     GuardedRequestFunctionContext    aio_flush or internal_flush
                               |                    |                                |
                               v                    v                                v true
                               |                    |                                |
                               -------------------------------------------------------
                                         |             |          |
                                         v             v          v
                      GuardedRequest(block_extent, guard_ctx, is_barrier)
                               |
                               v
                               |
rwl.detain_guarded_request(guard_request)
    |
    ---> cell = detain_guarded_request_barrier_helper
    |                      |
    |                      v
    |                      |                       true
    |            rwl.m_barrier_in_progress  -------------------> guard_request->guard_ctx->m_state.queued = true
    |                      |                               |
    |                false v                               ----> rwl.m_awaiting_barrier.push_back(guard_request)
    |                      |                               |
    |                      |                               ----> cell = nullptr  --------------------->------------------------------------------------------
    |                      |                                                                                                                                |    
    |                      |                                                                                                                                |            
    |                      |                              true                                                                                              |    
    |      guard_request->guard_ctx->m_state.barrier ------------->  rwl.m_barrier_in_progress = true                                                       |            
    |                      |                                                       |                                                                        |            
    |                false v                                                       v                                                                        |    
    |                      |                                         guard_request->guard_ctx->m_state.current_barrier = true                               |            
    v                      |                                                       |                                                                        |            
    |                      ------------<-----------------------<--------------------                                                                        |            
    |                      |                                                                                                                                |    
    |                      v                                                                                                                                |    
    |       cell = detain_guarded_request_helper  ----> rwl.m_write_log_guard.detain(guard_request->block_extent, block_request, &cell)                     |            
    |                      |                                                                                                       |                        |            
    |                      v        yes                                                                                            ------------------------>|            
    |                if(barrier) -------->  rwl.m_barrier_cell = cell                                                                                       |            
    |                                                                                                                                                       |
    |                                                                                                                                                       |
    |                                                                                                                                                       |
    |                                                                                                                                                       |
    |        ------------------------------------------------------------------------------------------------------------------------------------------------   
    |        |
    |        v
    |        |
    ---> if(cell)
           |
           ----> guard_request->guard_ctx->m_cell = cell
           |
           ----> guard_request->guard_ctx->complete(0)
                                              |
                                              v
                                              |
    -------------------------------------------
    |
    |
    |
    |
    |      rwl.internal_flush    rwl.aio_flush       GuardedBlockIORequest::release_cell
    |         |                      |                             |
    |         v                      v                             v
    |         |                      |                             |
    |         ------------------------------------------------------
    |                                |
    |                                v
    |                  rwl.release_guarded_request
    |                     |
    |                     ----> 
    |
    |
    |
    |
    --------
           |
           v
           |
      context::complete 
         |
         ---> GuardedRequestFunctionContext::finish 
                |
               ---> ReplicatedWriteLog::aio_xxx::lambda_function
                         |
                         ---> C_xxx_Request::blockguard_acquired(guarded_ctx)
                         |
                         ---> rwl.alloc_and_dispatch_io_req(C_xxx_request)
    
    




*/

