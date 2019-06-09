// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/Utils.h"
#include "librbd/cache/ParentCacheObjectDispatch.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ParentCacheObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I, typename C>
ParentCacheObjectDispatch<I, C>::ParentCacheObjectDispatch(
    I* image_ctx) : m_image_ctx(image_ctx), m_cache_client(nullptr),
    m_initialized(false), m_object_store(nullptr) {
}

template <typename I, typename C>
ParentCacheObjectDispatch<I, C>::~ParentCacheObjectDispatch() {
    delete m_object_store;
    delete m_cache_client;
}

// TODO if connect fails, init will return error to high layer.
template <typename I, typename C>
void ParentCacheObjectDispatch<I, C>::init() {
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
      m_initialized = true;
    }
  }
}

template <typename I, typename C>
bool ParentCacheObjectDispatch<I, C>::read(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // if any failse, reads will go to rados
  if(m_cache_client == nullptr || !m_cache_client->is_session_work() ||
     m_object_store == nullptr || !m_initialized) {
    ldout(cct, 5) << "SRO cache client session failed " << dendl;
    return false;
  }

  CacheGenContextURef ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                     std::function<void(ObjectCacheRequest*)>>
   ([this, snap_id, read_data, dispatch_result, on_dispatched,
      oid, object_off, object_len](ObjectCacheRequest* ack) {
     handle_read_cache(ack, object_off, object_len, read_data,
                       dispatch_result, on_dispatched);
  });

  m_cache_client->lookup_object(m_image_ctx->data_ctx.get_namespace(),
                                m_image_ctx->data_ctx.get_id(),
                                (uint64_t)snap_id, oid, std::move(ctx));
  return true;
}

template <typename I, typename C>
void ParentCacheObjectDispatch<I, C>::handle_read_cache(
    ObjectCacheRequest* ack, uint64_t read_off,
    uint64_t read_len, ceph::bufferlist* read_data,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if(ack->type != RBDSC_READ_REPLY) {
    // go back to read rados
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
    return;
  }

  ceph_assert(ack->type == RBDSC_READ_REPLY);
  std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
  ceph_assert(file_path != "");

  // try to read from parent image cache
  int r = m_object_store->read_object(file_path, read_data, read_off, read_len, on_dispatched);
  if(r < 0) {
    // cache read error, fall back to read rados
    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
    on_dispatched->complete(0);
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  on_dispatched->complete(r);
}

template <typename I, typename C>
int ParentCacheObjectDispatch<I, C>::handle_register_client(bool reg) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  if (reg) {
    ldout(cct, 20) << "SRO cache client open cache handler" << dendl;
    m_object_store = new SharedPersistentObjectCacher<I>(m_image_ctx, m_image_ctx->shared_cache_path);
  }
  return 0;
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ParentCacheObjectDispatch<librbd::ImageCtx, ceph::immutable_obj_cache::CacheClient>;
