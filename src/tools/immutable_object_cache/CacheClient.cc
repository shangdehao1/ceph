// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CacheClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheClient: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

  CacheClient::CacheClient(const std::string& file, CephContext* ceph_ctx)
    : m_io_service_work(m_io_service),
      m_dm_socket(m_io_service),
      m_ep(stream_protocol::endpoint(file)),
      m_io_thread(nullptr),
      m_session_work(false),
      m_writing(false),
      m_lock("ceph::cache::cacheclient::m_lock"),
      m_map_lock("ceph::cache::cacheclient::m_map_lock"),
      m_sequence_id_lock("ceph::cache::cacheclient::m_sequence_id_lock"),
      m_sequence_id(0),
      m_header_buffer(new char[sizeof(ObjectCacheMsgHeader)]),
      cct(ceph_ctx)
  {}

  CacheClient::~CacheClient() {
    stop();
    delete m_header_buffer;
  }

  void CacheClient::run(){
     m_io_thread.reset(new std::thread([this](){m_io_service.run(); }));
  }

  bool CacheClient::is_session_work() {
    return m_session_work.load() == true;
  }

  int CacheClient::stop() {
    m_session_work.store(false);
    m_io_service.stop();

    if(m_io_thread != nullptr) {
      m_io_thread->join();
    }
    return 0;
  }

  // just when error occur, call this method.
  void CacheClient::close() {
    m_session_work.store(false);
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if(close_ec) {
       ldout(cct, 20) << "close: " << close_ec.message() << dendl;
    }
    ldout(cct, 20) << "session don't work, later all request will be" <<
                      " dispatched to rados layer" << dendl;
  }

  int CacheClient::connect() {
    boost::system::error_code ec;
    m_dm_socket.connect(m_ep, ec);
    if(ec) {
      if(ec == boost::asio::error::connection_refused) {
        ldout(cct, 20) << ec.message()
                       << " : immutable-object-cache daemon is down?"
                       << "Now data will be read from ceph cluster " << dendl;
      } else {
        ldout(cct, 20) << "connect: " << ec.message() << dendl;
      }

      if(m_dm_socket.is_open()) {
        // Set to indicate what error occurred, if any.
        // Note that, even if the function indicates an error,
        // the underlying descriptor is closed.
        boost::system::error_code close_ec;
        m_dm_socket.close(close_ec);
        if(close_ec) {
          ldout(cct, 20) << "close: " << close_ec.message() << dendl;
        }
      }
      return -1;
    }

    ldout(cct, 20) <<"connect success"<< dendl;

    return 0;
  }

  // TODO
  int CacheClient::register_client(Context* on_finish) {
    // cache controller will init layout
    rbdsc_req_type_t *message = new rbdsc_req_type_t();
    message->type = RBDSC_REGISTER;

    uint64_t ret;
    boost::system::error_code ec;

    ret = boost::asio::write(m_dm_socket,
      boost::asio::buffer((char*)message, message->size()), ec);

    if(ec) {
      ldout(cct, 20) << "write fails : " << ec.message() << dendl;
      return -1;
    }

    if(ret != message->size()) {
      ldout(cct, 20) << "write fails : ret != send_bytes " << dendl;
      return -1;
    }

    // hard code TODO
    ret = boost::asio::read(m_dm_socket,
      boost::asio::buffer(m_recv_buffer, RBDSC_MSG_LEN), ec);

    if(ec == boost::asio::error::eof) {
      ldout(cct, 20) << "recv eof" << dendl;
      return -1;
    }

    if(ec) {
      ldout(cct, 20) << "write fails : " << ec.message() << dendl;
      return -1;
    }

    if(ret != RBDSC_MSG_LEN) {
      ldout(cct, 20) << "write fails : ret != receive bytes " << dendl;
      return -1;
    }

    std::string reply_msg(m_recv_buffer, ret);
    rbdsc_req_type_t *io_ctx = (rbdsc_req_type_t*)(reply_msg.c_str());

    if (io_ctx->type == RBDSC_REGISTER_REPLY) {
      on_finish->complete(true);
    } else {
      on_finish->complete(false);
    }

    delete message;

    ldout(cct, 20) << "register volume success" << dendl;

    // TODO
    m_session_work.store(true);

    return 0;
  }

  // if occur any error, we just return false. Then read from rados.
  int CacheClient::lookup_object(std::string pool_name, std::string object_id,
                                 Context* on_finish) {
    rbdsc_req_type_t *message = new rbdsc_req_type_t();
    message->type = RBDSC_READ;
    memcpy(message->pool_name, pool_name.c_str(), pool_name.size());
    memcpy(message->oid, object_id.c_str(), object_id.size());
    message->vol_size = 0;
    message->offset = 0;
    message->length = 0;

    boost::asio::async_write(m_dm_socket,
      boost::asio::buffer((char*)message, message->size()),
      boost::asio::transfer_exactly(RBDSC_MSG_LEN),
      [this, on_finish, message](const boost::system::error_code& err, size_t cb) {
          delete message;
          if(err) {
            ldout(cct, 20) << "async_write failed"
                           << err.message() << dendl;
            close();
            on_finish->complete(false);
            return;
          }
          if(cb != RBDSC_MSG_LEN) {
            ldout(cct, 20) << "async_write failed in-complete request" << dendl;
            close();
            on_finish->complete(false);
            return;
          }
          get_result(on_finish);
    });

    return 0;
  }

  void CacheClient::get_result(Context* on_finish) {
    char* lookup_result = new char[RBDSC_MSG_LEN + 1];
    boost::asio::async_read(m_dm_socket,
      boost::asio::buffer(lookup_result, RBDSC_MSG_LEN),
      boost::asio::transfer_exactly(RBDSC_MSG_LEN),
      [this, lookup_result, on_finish](const boost::system::error_code& err,
                                       size_t cb) {
          if(err == boost::asio::error::eof ||
            err == boost::asio::error::connection_reset ||
            err == boost::asio::error::operation_aborted ||

            err == boost::asio::error::bad_descriptor) {
            ldout(cct, 20) << "fail to read lookup result"
                           << err.message() << dendl;
            close();
            on_finish->complete(false);
            delete lookup_result;
            return;
          }

          if(err) {
            ldout(cct, 1) << "fail to read lookup result"
                          << err.message() << dendl;
            close();
            on_finish->complete(false);
            delete lookup_result;
            return;
          }

          if (cb != RBDSC_MSG_LEN) {
            ldout(cct, 1) << "incomplete lookup result" << dendl;
            close();
            on_finish->complete(false);
            delete lookup_result;
            return;
          }

	  rbdsc_req_type_t *io_ctx = (rbdsc_req_type_t*)(lookup_result);

          if (io_ctx->type == RBDSC_READ_REPLY) {
	    on_finish->complete(true);
          } else {
	    on_finish->complete(false);
          }
          delete lookup_result;
          return;
    });
  }


// ===============================

  void CacheClient::new_lookup_object(std::string pool_name, std::string object_id, 
                                      Context* on_finish, bufferlist* bl) {

    ObjectCacheRequest* req = new ObjectCacheRequest();    

    // make sure that assignment and ++ operation are exected atomicially.
    {
      Mutex::Locker locker(m_sequence_id_lock);
      req->m_head.seq = m_sequence_id++;
    }
    req->m_head.type = RBDSC_READ;
    req->m_head.version = 0;
    req->m_head.reserved = 0;

    req->m_mid.m_read_offset = 0; // TODO 
    req->m_mid.m_read_len = 0;
    req->m_mid.m_pool_name = std::move(pool_name);
    req->m_mid.m_image_name = pool_name;
    req->m_mid.m_oid = std::move(object_id);
    req->m_on_finish = on_finish;
    /*
    if(bl) {
      req->m_data_buffer = *bl;
    }
    */

    req->encode();

    ceph_assert(req->get_head_buffer().length() == sizeof(ObjectCacheMsgHeader));
    ceph_assert(req->get_mid_buffer().length() == req->m_head.mid_len);

    {
      Mutex::Locker locker(m_lock); 
      m_outcoming_bl.append(req->get_head_buffer());
      m_outcoming_bl.append(req->get_mid_buffer());
      if(bl) {
        m_outcoming_bl.append(req->get_data_buffer());
      }
    }

    {
      Mutex::Locker locker(m_map_lock);
      ceph_assert(m_seq_to_req.find(req->m_head.seq) == m_seq_to_req.end());
      m_seq_to_req[req->m_head.seq] = req;
    }

    // try to send message to server.
    try_send();

    // try to receive ack from server.
    try_receive();

  }

  void CacheClient::try_send() {
    if(!m_writing) {
      m_writing.store(true);
      send_message();
    }
  }

  void CacheClient::send_message() {
    bufferlist bl;
    {
      Mutex::Locker locker(m_lock);
      bl.swap(m_outcoming_bl); 
    }

    // send bytes as many as possible.
    boost::asio::async_write(m_dm_socket, 
        boost::asio::buffer(bl.c_str(), bl.length()),
        boost::asio::transfer_exactly(bl.length()),
        [this, bl](const boost::system::error_code& err, size_t cb) {
        if(err || cb != bl.length()) {
           fault(ASIO_ERROR_WRITE, err);
           return;
        }
        ceph_assert(cb == bl.length());
         
        {
           Mutex::Locker locker(m_lock);
           if(m_outcoming_bl.length() == 0) {
             m_writing.store(false);
             return;
           }
        }

        // still have left bytes, continue to send.
        send_message();
    });

    try_receive();
  }

  void CacheClient::try_receive() {
    if(!m_reading.load()) {
      m_reading.store(true);
      receive_message(); 
    } 
  }
  
  void CacheClient::receive_message() {
    bufferptr bp_head(buffer::create_static(sizeof(ObjectCacheMsgHeader), m_header_buffer));

    boost::asio::async_read(m_dm_socket, 
      boost::asio::buffer(bp_head.c_str(), bp_head.length()),    
      boost::asio::transfer_exactly(sizeof(ObjectCacheMsgHeader)),
      [this, bp_head](const boost::system::error_code& err, size_t cb) {
        if(err || cb != sizeof(ObjectCacheMsgHeader)) {
          fault(ASIO_ERROR_READ, err);
          return;
        }
       
        ObjectCacheMsgHeader* head = (ObjectCacheMsgHeader*)bp_head.c_str();     
        uint64_t mid_len = head->mid_len;
        uint64_t seq_id = head->seq;
        bufferptr bp_mid(buffer::create(mid_len));

        boost::asio::async_read(m_dm_socket,            
          boost::asio::buffer(bp_mid.c_str(), mid_len), 
          boost::asio::transfer_exactly(mid_len), 
          [this, seq_id, bp_mid, 
                bp_head, mid_len](const boost::system::error_code& err, size_t cb) {
            if(err || cb != mid_len) {
              fault(ASIO_ERROR_READ, err);
              return;
            } 

            bufferlist head_buffer;
            head_buffer.append(std::move(bp_head));
            bufferlist mid_buffer; 
            mid_buffer.append(std::move(bp_mid));

            ceph_assert(head_buffer.length() == 28);
            ceph_assert(mid_buffer.length() == mid_len);

            ObjectCacheRequest* ack = decode_object_cache_request(head_buffer, mid_buffer);

            // TODO : high layer go to decide how to handle this ack...
            // TODO : threadpool to execute these completion entry.....
            // namely, m_on_finish....
            if(ack->m_head.type == RBDSC_READ_REPLY) {
              m_seq_to_req[ack->m_head.seq]->m_on_finish->complete(true); 
            } else {
              m_seq_to_req[ack->m_head.seq]->m_on_finish->complete(false); 
            }

            {
              Mutex::Locker locker(m_map_lock);
              ceph_assert(m_seq_to_req.find(seq_id) != m_seq_to_req.end());
              // TODO 
              delete m_seq_to_req[seq_id];
              m_seq_to_req.erase(seq_id);
              if(m_seq_to_req.size() == 0) {
                m_reading.store(false);
                return;
              }
            }
            // still have left response from server side.
            receive_message();
        });
    });
  }

  void CacheClient::fault(const int err_type, const boost::system::error_code& ec) {
    // if one request fails, just call its callback, then close this socket.
    if(!m_session_work.load()) {
      return;
    }

    // when session don't work, ASIO will don't receive any new request. 
    // Otherwise, for pending request of ASIO, cancle them, then call their callback.
    // , which will re-dispatch the currespong request to RADOS layer.
    // 
    // make sure just have one thread to modify execute below code. 
    m_session_work.store(false);

    if(err_type == ASIO_ERROR_MSG_INCOMPLETE) {
       std::cout << "incomplete msg........" << std::endl;
       ceph_assert(0);
    }

    if(err_type == ASIO_ERROR_READ) {
       std::cout << "async read error : " << ec.message() << std::endl;
    }

    if(err_type == ASIO_ERROR_WRITE) {
       std::cout << "async write error : " << ec.message() << std::endl;
    }

    if(err_type == ASIO_ERROR_CONNECT) {
       std::cout << "async connect error : " << ec.message() << std::endl;
    }

    // currently, for any asio error, just shutdown RO, then later all request will be 
    // dispatched to RADOS layer.
    // TODO 

    {
      Mutex::Locker locker(m_map_lock); 
      for(auto it : m_seq_to_req) {
        it.second->m_on_finish->complete(false);
      }
      m_seq_to_req.clear();
      m_outcoming_bl.clear();
    }
  }  


  int CacheClient::new_register_client(Context* on_finish) {
    ObjectCacheRequest* message = new ObjectCacheRequest();

    message->m_head.version = 0;
    message->m_head.seq = m_sequence_id++;
    message->m_head.type = RBDSC_REGISTER;
    message->m_head.reserved = 0;
    message->encode();

    bufferlist bl;
    bl.append(message->get_head_buffer());
    bl.append(message->get_mid_buffer());

    uint64_t ret;
    boost::system::error_code ec;

    ret = boost::asio::write(m_dm_socket,
      boost::asio::buffer(bl.c_str(), bl.length()), ec);

    if(ec || ret != bl.length()) {
      fault(ASIO_ERROR_WRITE, ec);
      return -1;
    }

    ret = boost::asio::read(m_dm_socket,
      boost::asio::buffer(m_header_buffer, sizeof(ObjectCacheMsgHeader)), ec);

    if(ec || ret != sizeof(ObjectCacheMsgHeader)) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    ObjectCacheMsgHeader* head = (ObjectCacheMsgHeader*)m_header_buffer;

    uint64_t mid_len = head->mid_len;
    bufferptr bp_mid(buffer::create(mid_len));
  
    ret = boost::asio::read(m_dm_socket, boost::asio::buffer(bp_mid.c_str(), mid_len), ec);
    if(ec || ret != mid_len) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    bufferlist mid_buffer;
    mid_buffer.append(std::move(bp_mid));
    ObjectCacheRequest* req = decode_object_cache_request(head, mid_buffer);
    

    if (req->m_head.type == RBDSC_REGISTER_REPLY) {
      on_finish->complete(true);
    } else {
      on_finish->complete(false);
    }

    delete req;

    // TODO
    m_session_work.store(true);

    return 0;
  }

} // namespace immutable_obj_cache
} // namespace ceph
