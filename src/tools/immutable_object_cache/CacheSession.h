// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SESSION_H
#define CEPH_CACHE_SESSION_H

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "SocketCommon.h"

using boost::asio::local::stream_protocol;
using boost::asio::io_service;

namespace ceph {
namespace immutable_obj_cache {

class CacheSession : public std::enable_shared_from_this<CacheSession> {
public:
  CacheSession(uint64_t session_id, io_service& io_service,
               ProcessMsg processmsg, CephContext* cct);
  ~CacheSession();

  stream_protocol::socket& socket();
  void start();
  void close();
  void handing_request();

private:

  void handle_read(const boost::system::error_code& error,
                   size_t bytes_transferred);

  void handle_write(const boost::system::error_code& error,
                    size_t bytes_transferred);

public:
  void send(std::string msg);



// =======================
  CacheSession(uint64_t session_id, io_service& io_service, NewProcessMsg process_msg, CephContext* ctx);
  void new_start();
  void read_request_header();
  void handle_request_header(const boost::system::error_code& err, size_t bytes_transferred);
  void read_request_mid(uint64_t mid_len);
  void handle_request_mid(bufferptr bp, uint64_t mid_len, 
                          const boost::system::error_code& err, size_t bytes_transferred);

  void process(ObjectCacheRequest* req);
  void fault();
  void send(ObjectCacheRequest* msg);


private:
  uint64_t m_session_id;
  stream_protocol::socket m_dm_socket;
  ProcessMsg process_msg;
  CephContext* cct;

  // Buffer used to store data received from the client.
  //std::array<char, 1024> data_;
  char m_buffer[1024];

  // 
  char* m_head_buffer;
  NewProcessMsg m_new_process_msg;
  bool if_new;

};

typedef std::shared_ptr<CacheSession> CacheSessionPtr;

} // namespace immutable_obj_cache
} // namespace ceph

#endif
