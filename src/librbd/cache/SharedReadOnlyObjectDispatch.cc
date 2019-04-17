// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "librbd/cache/SharedReadOnlyObjectDispatch.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SharedReadOnlyObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I, typename C>
SharedReadOnlyObjectDispatch<I, C>::SharedReadOnlyObjectDispatch(
    I* image_ctx) : m_image_ctx(image_ctx), m_cache_client(nullptr),
    m_initialzed(false) {
}

template <typename I, typename C>
SharedReadOnlyObjectDispatch<I, C>::~SharedReadOnlyObjectDispatch() {
    delete m_object_store;
    delete m_cache_client;
}

// TODO if connect fails, init will return error to high layer.
template <typename I, typename C>
void SharedReadOnlyObjectDispatch<I, C>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  if (m_image_ctx->parent != nullptr) {
    ldout(cct, 5) << "child image: skipping" << dendl;
    return;
  }

  ldout(cct, 5) << "parent image: setup SRO cache client" << dendl;

  std::string controller_path = ((CephContext*)cct)->_conf.get_val<std::string>("immutable_object_cache_sock");
  if(m_cache_client == nullptr) {
    m_cache_client = new C(controller_path.c_str(), m_image_ctx->cct);
  }
  m_cache_client->run();

  int ret = m_cache_client->connect();
  if (ret < 0) {
    ldout(cct, 5) << "SRO cache client fail to connect with local controller: "
                  << "please start ceph-immutable-object-cache daemon"
		  << dendl;
  } else {
    ldout(cct, 5) << "SRO cache client to register volume "
                  << "name = " << m_image_ctx->id
                  << " on ceph-immutable-object-cache daemon"
                  << dendl;

    auto ctx = new FunctionContext([this](bool reg) {
      handle_register_client(reg);
    });

    ret = m_cache_client->register_client(ctx);

    if (ret >= 0) {
      // add ourself to the IO object dispatcher chain
      m_image_ctx->io_object_dispatcher->register_object_dispatch(this);
      m_initialzed = true;
    }
  }
}

template <typename I, typename C>
bool SharedReadOnlyObjectDispatch<I, C>::read(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // if any session failed, reads will go to rados
  if(!m_cache_client->is_session_work()) {
    ldout(cct, 5) << "SRO cache client session failed " << dendl;
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return true;
  }

  auto ctx = new LambdaGenContext<std::function<void(ObjectCacheRequest*)>,
      ObjectCacheRequest*>([this, snap_id, read_data, dispatch_result, on_dispatched,
      oid, object_off, object_len](ObjectCacheRequest* ack) {

    if (ack->type == RBDSC_READ_REPLY) {
      std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
      ceph_assert(file_path != "");
      handle_read_cache(file_path, object_off, object_len, read_data,
                        dispatch_result, on_dispatched);
    } else {
      // go back to read rados
      *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
      on_dispatched->complete(0);
    }
  });

  if (m_cache_client && m_cache_client->is_session_work() && m_object_store) {

    m_cache_client->lookup_object(m_image_ctx->data_ctx.get_namespace(),
                                  m_image_ctx->data_ctx.get_id(),
                                  (uint64_t)snap_id, oid, ctx);
  }
  return true;
}

template <typename I, typename C>
int SharedReadOnlyObjectDispatch<I, C>::handle_read_cache(
    const std::string file_path, uint64_t read_off,
    uint64_t read_len, ceph::bufferlist* read_data,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // try to read from parent image cache
  int r = m_object_store->read_object(file_path, read_data, read_off, read_len, on_dispatched);
  if (r == read_len) {
    *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
    //TODO(): complete in syncfile
    on_dispatched->complete(r);
    ldout(cct, 20) << "read cache: " << *dispatch_result <<dendl;
    return true;
  }

  // cache read error, fall back to read rados
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  on_dispatched->complete(0);
  return false;
}

template <typename I, typename C>
int SharedReadOnlyObjectDispatch<I, C>::handle_register_client(bool reg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if (reg) {
    ldout(cct, 20) << "SRO cache client open cache handler" << dendl;
    m_object_store = new SharedPersistentObjectCacher<I>(m_image_ctx, m_image_ctx->shared_cache_path);
  }
  return 0;
}

template <typename I, typename C>
void SharedReadOnlyObjectDispatch<I, C>::client_handle_request(std::string msg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

}

} // namespace cache
} // namespace librbd

template class librbd::cache::SharedReadOnlyObjectDispatch<librbd::ImageCtx, ceph::immutable_obj_cache::CacheClient>;
