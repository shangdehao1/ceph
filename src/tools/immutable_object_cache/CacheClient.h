// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_CACHE_CLIENT_H
#define CEPH_CACHE_CACHE_CLIENT_H

#include <atomic>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/asio/error.hpp>
#include <boost/algorithm/string.hpp>
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "SocketCommon.h"


using boost::asio::local::stream_protocol;

namespace ceph {
namespace immutable_obj_cache {

class CacheClient {
public:
  CacheClient(const std::string& file, CephContext* ceph_ctx);
  ~CacheClient();
  void run();
  bool is_session_work();

  void close();
  int stop();
  int connect();

  int register_client(Context* on_finish);
  int lookup_object(std::string pool_name, std::string object_id, Context* on_finish);
  void get_result(Context* on_finish);

  // ====
  void new_lookup_object(std::string pool_name, std::string object_id, Context* on_finish, bufferlist* bl=nullptr);
  void send_message();
  void try_send();
  void fault(const int err_type, const boost::system::error_code& err);
  void try_receive();
  void receive_message();
  int new_register_client(Context* on_finish);

private:
  boost::asio::io_service m_io_service;
  boost::asio::io_service::work m_io_service_work;
  stream_protocol::socket m_dm_socket;
  ClientProcessMsg m_client_process_msg;
  stream_protocol::endpoint m_ep;
  char m_recv_buffer[1024];
  std::shared_ptr<std::thread> m_io_thread;

  // atomic modfiy for this variable.
  // thread 1 : asio callback thread modify it.
  // thread 2 : librbd read it.
  std::atomic<bool> m_session_work;
  CephContext* cct;

// ===== 

  char* m_header_buffer;
  std::atomic<bool> m_writing;
  std::atomic<bool> m_reading;
  std::atomic<uint64_t> m_sequence_id;
  Mutex m_lock;
  bufferlist m_outcoming_bl;
  Mutex m_sequence_id_lock;
  Mutex m_map_lock;
  std::map<uint64_t, ObjectCacheRequest*> m_seq_to_req;
};

} // namespace immutable_obj_cache
} // namespace ceph
#endif
