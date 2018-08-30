// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/SharedPersistentObjectCacherObjectDispatch.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SharedPersistentObjectCacherObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I>
SharedPersistentObjectCacherObjectDispatch<I>::SharedPersistentObjectCacherObjectDispatch(
    I* image_ctx) : m_image_ctx(image_ctx) {
}

template <typename I>
SharedPersistentObjectCacherObjectDispatch<I>::~SharedPersistentObjectCacherObjectDispatch() {
    delete m_object_store;
    delete m_cache_client;
}

// TODO if connect fails, init will return error to high layer.
template <typename I>
void SharedPersistentObjectCacherObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (m_image_ctx->parent != nullptr) {
    //TODO(): should we cover multi-leveled clone?
    ldout(cct, 5) << "child image: skipping SRO cache client" << dendl;
    return;
  }

  ldout(cct, 20) << "parent image: setup SRO cache client = " << dendl;

  std::string controller_path = "/tmp/rbd_shared_readonly_cache_demo";
  m_cache_client = new rbd::cache::CacheClient(controller_path.c_str(),
    ([&](std::string s){client_handle_request(s);}), m_image_ctx->cct);

  int ret = m_cache_client->connect();
  if (ret < 0) {
    ldout(cct, 5) << "SRO cache client fail to connect with local controller: "
                  << "please start rbd-cache daemon"
		  << dendl;
  } else {
    ldout(cct, 5) << "SRO cache client to register volume on rbd-cache daemon: "
                   << "name = " << m_image_ctx->id 
                   << dendl;

    ret = m_cache_client->register_volume(m_image_ctx->data_ctx.get_pool_name(),
                                    m_image_ctx->id, m_image_ctx->size);

    if (ret >= 0) {
      // add ourself to the IO object dispatcher chain
      m_image_ctx->io_object_dispatcher->register_object_dispatch(this);
    }
  }
}

template <typename I>
bool SharedPersistentObjectCacherObjectDispatch<I>::read(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  // IO chained in reverse order
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  //std::cout<<"read: --> lookup" << std::endl;

  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);
  auto ctx = new FunctionContext([this, oid, object_off, object_len, read_data, dispatch_result, on_dispatched](bool cache) {
    //std::cout<<"read: --> lookup callback" << std::endl;
    handle_read_cache(cache, oid, object_off, object_len, read_data, dispatch_result, on_dispatched);
  });

  if (m_cache_client && m_cache_client->connected && m_object_store) {
    m_cache_client->lookup_object(m_image_ctx->data_ctx.get_pool_name(),
      m_image_ctx->id, oid, ctx);
  }
  return true;
}

template <typename I>
int SharedPersistentObjectCacherObjectDispatch<I>::handle_read_cache(
    bool cache,
    const std::string &oid, uint64_t object_off, uint64_t object_len,
    ceph::bufferlist* read_data, io::DispatchResult* dispatch_result,
    Context* on_dispatched) {
  // IO chained in reverse order
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // try to read from parent image
  if (cache) {
    int r = m_object_store->read_object(oid, read_data, object_off, object_len, on_dispatched);
    //int r = object_len;
    if (r != 0) {
      *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
      //TODO(): complete in syncfile
      on_dispatched->complete(r);
      ldout(cct, 20) << "AAAAcomplete=" << *dispatch_result <<dendl;
      return true;
    }
  } else {
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    ldout(cct, 20) << "BBB no cache" << *dispatch_result <<dendl;
    return false;
  }
}
template <typename I>
void SharedPersistentObjectCacherObjectDispatch<I>::client_handle_request(std::string msg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  rbd::cache::rbdsc_req_type_t *io_ctx = (rbd::cache::rbdsc_req_type_t*)(msg.c_str());

  switch (io_ctx->type) {
    case rbd::cache::RBDSC_REGISTER_REPLY: {
      // open cache handler for volume
      ldout(cct, 20) << "SRO cache client open cache handler" << dendl;
      m_object_store = new SharedPersistentObjectCacher<I>(m_image_ctx, m_image_ctx->shared_cache_path);

      break;
    }
    case rbd::cache::RBDSC_READ_REPLY: {
      ldout(cct, 20) << "SRO cache client start to read cache" << dendl;
      //TODO(): should call read here

      break;
    }
    case rbd::cache::RBDSC_READ_RADOS: {
      ldout(cct, 20) << "SRO cache client start to read rados" << dendl;
      //TODO(): should call read here

      break;
    }
    default: ldout(cct, 20) << "nothing" << io_ctx->type <<dendl;
      break;

  }
}

} // namespace cache
} // namespace librbd

template class librbd::cache::SharedPersistentObjectCacherObjectDispatch<librbd::ImageCtx>;
