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
  {
    // TODO : release these resources.
    m_use_dedicated_worker = true;
    // TODO : configure it.
    m_worker_thread_num = 2;
    if(m_use_dedicated_worker) {
      m_worker = new boost::asio::io_service();
      m_worker_io_service_work = new boost::asio::io_service::work(*m_worker);
      for(int i = 0; i < m_worker_thread_num; i++) {
        std::thread* thd = new std::thread([this](){m_worker->run();});
        m_worker_threads.push_back(thd);
      } 
    }
  }

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

  // TODO : just one paramter : object cache request
  void CacheClient::lookup_object(std::string pool_name, std::string object_id, 
                                      Context* on_finish, bufferlist* bl) {

    // TODO : move new/delete to higher layer..
    ObjectCacheRequest* req = new ObjectCacheRequest();    

    {
      Mutex::Locker locker(m_sequence_id_lock);
      req->m_head.seq = m_sequence_id++;
    }

    req->m_head.type = RBDSC_READ;
    req->m_head.version = 0;
    req->m_head.reserved = 0;

    // TODO : move offset/len from lambda function to ObjectCacheRequest
    req->m_mid.m_read_offset = 0; // TODO 
    req->m_mid.m_read_len = 0;
    req->m_mid.m_pool_name = std::move(pool_name);
    req->m_mid.m_image_name = pool_name;
    req->m_mid.m_oid = std::move(object_id);
    req->m_on_finish = on_finish;
    if(bl) {
      req->m_data_buffer = *bl;
    }

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
    if(!m_writing.load()) {
      m_writing.store(true);
      send_message();
    }
  }

  void CacheClient::send_message() {
    // local variable.
    bufferlist bl;
    {
      Mutex::Locker locker(m_lock);
      bl.swap(m_outcoming_bl); 
      ceph_assert(m_outcoming_bl.length() == 0);
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
    ceph_assert(m_reading.load());

    /* one head buffer for all arrived message. */
    // bufferptr bp_head(buffer::create_static(sizeof(ObjectCacheMsgHeader), m_header_buffer));

    // create new head buffer for every arrived message
    bufferptr bp_head(buffer::create(sizeof(ObjectCacheMsgHeader)));

    boost::asio::async_read(m_dm_socket, 
      boost::asio::buffer(bp_head.c_str(), sizeof(ObjectCacheMsgHeader)),    
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
          [this, bp_mid, bp_head, mid_len, seq_id](const boost::system::error_code& err, size_t cb) {
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

            mid_buffer.clear();
            ceph_assert(mid_buffer.length() == 0);

            auto req_on_finish = m_seq_to_req[ack->m_head.seq]->m_on_finish;
            auto req_type  = ack->m_head.type;
            delete ack;
             
            // if hit, this context will read file from local cache.
            auto user_ctx = new FunctionContext([this, req_on_finish, req_type](bool dedicated){
              if(dedicated) {
                // current thread belog to worker.
              }
              if(req_type == RBDSC_READ_REPLY) {
                req_on_finish->complete(true); 
              } else {
                req_on_finish->complete(false); 
              }   
            }); 

            // Because user_ctx will read file and execute their callback, we think it hold on current thread 
            // for long time, then defer read/write message from/to socket. 
            // if want to use dedicated thread to execute this context, enable it.
            if(m_use_dedicated_worker) { 
              m_worker->post([user_ctx](){
                user_ctx->complete(true);
              });
            } else {
              // use read/write thread to execute this context.
              user_ctx->complete(false);
            }

            {
              Mutex::Locker locker(m_map_lock);
              ceph_assert(m_seq_to_req.find(seq_id) != m_seq_to_req.end());
              delete m_seq_to_req[seq_id];
              m_seq_to_req.erase(seq_id);
              if(m_seq_to_req.size() == 0) {
                m_reading.store(false);
                return;
              }
            }
         
            receive_message();
        });
    });
  }

  void CacheClient::fault(const int err_type, const boost::system::error_code& ec) {
    ldout(cct, 20) << "fault." << ec.message() << dendl;
    // if one request fails, just call its callback, then close this socket.
    if(!m_session_work.load()) {
      return;
    }

    // when current session don't work, ASIO will don't receive any new request from hook. 
    // On the other hand, for pending request of ASIO, cancle these request, then call their callback.
    // there request which are cancled by fault, will be re-dispatched to RADOS layer.
    // 
    // make sure just have one thread to modify execute below code. 
    m_session_work.store(false);

    if(err_type == ASIO_ERROR_MSG_INCOMPLETE) {
       ldout(cct, 20) << "ASIO In-complete message." << ec.message() << dendl;
       ceph_assert(0);
    }

    if(err_type == ASIO_ERROR_READ) {
       ldout(cct, 20) << "ASIO async read fails : " << ec.message() << dendl;
    }

    if(err_type == ASIO_ERROR_WRITE) {
       ldout(cct, 20) << "ASIO asyn write fails : " << ec.message() << dendl;
       // should not occur this error.
       ceph_assert(0);
    }

    if(err_type == ASIO_ERROR_CONNECT) {
       ldout(cct, 20) << "ASIO async connect fails : " << ec.message() << dendl;
    }

    // TODO : currently, for any asio error, just shutdown RO.
    // TODO : re-write close / shutdown/
    // will cancel all asio callback function...
    //close();


    // all pending request, which have entered into ASIO, whill be re-dispatched to RADOS.
    {
      Mutex::Locker locker(m_map_lock); 
      for(auto it : m_seq_to_req) {
        it.second->m_on_finish->complete(false);
      }
      m_seq_to_req.clear();
    }

    //m_outcoming_bl.clear();
  }  


  // TODO : use async + wait_event
  int CacheClient::register_client(Context* on_finish) {
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
