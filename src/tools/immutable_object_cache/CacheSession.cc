// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/ceph_context.h"
#include "CacheSession.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheSession: " << this << " " \
                           << __func__ << ": "


namespace ceph {
namespace immutable_obj_cache {

CacheSession::CacheSession(uint64_t session_id, io_service& io_service,
                           ProcessMsg processmsg, CephContext* cct)
    : m_session_id(session_id), m_dm_socket(io_service),
      m_head_buffer(new char[sizeof(ObjectCacheMsgHeader)]),
      process_msg(processmsg), cct(cct)
    {}

CacheSession::~CacheSession() {
  close();
  delete[] m_head_buffer;
}

stream_protocol::socket& CacheSession::socket() {
  return m_dm_socket;
}

void CacheSession::close() {
  if(m_dm_socket.is_open()) {
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if(close_ec) {
       ldout(cct, 20) << "close: " << close_ec.message() << dendl;
    }
  }
}

void CacheSession::start() {
  handing_request();
}

void CacheSession::handing_request() {
  boost::asio::async_read(m_dm_socket,
                          boost::asio::buffer(m_buffer, RBDSC_MSG_LEN),
                          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                          boost::bind(&CacheSession::handle_read,
                                      shared_from_this(),
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_read(const boost::system::error_code& err,
                               size_t bytes_transferred) {
  if (err == boost::asio::error::eof ||
     err == boost::asio::error::connection_reset ||
     err == boost::asio::error::operation_aborted ||
     err == boost::asio::error::bad_descriptor) {
    ldout(cct, 20) << "fail to handle read : " << err.message() << dendl;
    close();
    return;
  }

  if(err) {
    ldout(cct, 1) << "faile to handle read: " << err.message() << dendl;
    return;
  }

  if(bytes_transferred != RBDSC_MSG_LEN) {
    ldout(cct, 1) << "incomplete read" << dendl;
    return;
  }

  //process_msg(m_session_id, std::string(m_buffer, bytes_transferred));
}

void CacheSession::handle_write(const boost::system::error_code& error,
                                size_t bytes_transferred) {
  if (error) {
    ldout(cct, 20) << "async_write failed: " << error.message() << dendl;
    assert(0);
  }

  if(bytes_transferred != RBDSC_MSG_LEN) {
    ldout(cct, 20) << "reply in-complete. "<<dendl;
    assert(0);
  }

  boost::asio::async_read(m_dm_socket, boost::asio::buffer(m_buffer),
                          boost::asio::transfer_exactly(RBDSC_MSG_LEN),
                          boost::bind(&CacheSession::handle_read,
                          shared_from_this(),
                          boost::asio::placeholders::error,
                          boost::asio::placeholders::bytes_transferred));

}


void CacheSession::send(std::string msg) {
    boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(msg.c_str(), msg.size()),
        boost::asio::transfer_exactly(RBDSC_MSG_LEN),
        boost::bind(&CacheSession::handle_write,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));

}



//============================================================================

CacheSession::CacheSession(uint64_t session_id, io_service& io_service,
                           NewProcessMsg processmsg, CephContext* cct)
    : m_session_id(session_id), m_dm_socket(io_service),
      m_head_buffer(new char[sizeof(ObjectCacheMsgHeader)]),
      m_new_process_msg(processmsg), cct(cct)
    {}


void CacheSession::new_start() {

  //TODO
  // delicated thread to setup....

  read_request_header();
}

void CacheSession::read_request_header() {

  boost::asio::async_read(m_dm_socket,
                          boost::asio::buffer(m_head_buffer, sizeof(ObjectCacheMsgHeader)),
                          boost::asio::transfer_exactly(sizeof(ObjectCacheMsgHeader)),
                          boost::bind(&CacheSession::handle_request_header,
                                      shared_from_this(),
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_request_header(const boost::system::error_code& err,
                               size_t bytes_transferred) {
  if(err || bytes_transferred != sizeof(ObjectCacheMsgHeader)) {
    fault();
    return;
  }


  ObjectCacheMsgHeader* head = (ObjectCacheMsgHeader*)(m_head_buffer);

  // currently, just use version 1.
  ceph_assert(head->version == 0);
  ceph_assert(head->data_len == 0);
  ceph_assert(head->reserved == 0);
  ceph_assert(head->type == RBDSC_REGISTER || head->type == RBDSC_READ || 
              head->type == RBDSC_LOOKUP);

  read_request_mid(head->mid_len);
}

void CacheSession::read_request_mid(uint64_t mid_len) {
  bufferptr bp_mid(buffer::create(mid_len));
  boost::asio::async_read(m_dm_socket,
                          boost::asio::buffer(bp_mid.c_str(), bp_mid.length()),
                          boost::asio::transfer_exactly(mid_len),
                          boost::bind(&CacheSession::handle_request_mid, 
                                      shared_from_this(), bp_mid, mid_len,
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}

void CacheSession::handle_request_mid(bufferptr bp, uint64_t mid_len,  
                                      const boost::system::error_code& err, 
                                      size_t bytes_transferred) {

  if(err || bytes_transferred != mid_len) {
    fault();
    return;
  }

  // bufferlist --> request
  bufferlist bl_mid;
  bl_mid.append(std::move(bp));
  ObjectCacheRequest* req = decode_object_cache_request(
                               (ObjectCacheMsgHeader*)m_head_buffer, bl_mid);

  // handle request
  process(req);
  
  // delete req; // TODO here ?

  // startup next round read
  //read_request_header();
}

// use addtional threadpool to execute this entry.
void CacheSession::process(ObjectCacheRequest* req) {
    
   m_new_process_msg(m_session_id, req);

}

// next-round receive operation have begun...
void CacheSession::send(ObjectCacheRequest* req) {

    req->m_head_buffer.clear();
    req->m_mid_buffer.clear();

    req->encode();
    bufferlist bl;
    bl.append(req->get_head_buffer());
    bl.append(req->get_mid_buffer());
    
    // TODO : directly using move(bl) will lead to bl.length() == 0
    boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(bl.c_str(), bl.length()),
        boost::asio::transfer_exactly(bl.length()),
        [this, bl, req](const boost::system::error_code& err, size_t cb) {
          if(err || cb != bl.length()) {
            fault();
            return;
          }
          delete req;
          read_request_header();
        });
}

void CacheSession::fault() {
}



} // namespace immutable_obj_cache
} // namespace ceph
