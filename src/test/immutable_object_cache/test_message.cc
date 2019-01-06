#include <iostream> 
#include <string>

#include <cstring>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "include/buffer.h"
#include "include/types.h"
#include "include/encoding.h"
#include "include/int_types.h"

#include "tools/immutable_object_cache/SocketCommon.h"

using namespace ceph::immutable_obj_cache;



int main() {

/*
  // write file
  {
    std::string content("1234567890"); 
    std::string yy(1000, '0');

    ceph::bufferlist bl;
    bl.append((content+yy).c_str(),(content+yy).size());

    std::cout << "before move : bl length : " << bl.length() << std::endl;
    ceph::bufferlist xx(std::move(bl));
    std::cout << "after move : bl length : " << bl.length() << std::endl;
    std::cout << "after move : xx length : " << xx.length() << std::endl;

    xx.write_file("/root/git/read-only-cache/12_25/src/tools/immutable_object_cache/test_file/test_file_1");
    std::cout << std::endl;
  } 

  // reading file
  bufferlist g_bl; 
 
  {
    ceph::bufferlist read_buffer;
    std::string ec;

    std::cout << "before reading file, bl length : "<< read_buffer.length() << std::endl;
    read_buffer.read_file("/root/git/read-only-cache/12_25/src/tools/immutable_object_cache/test_file/test_file_1", &ec);
    std::cout << "after reading file, bl length : "<< read_buffer.length() << std::endl;

    g_bl.substr_of(read_buffer, 0, 20);
   
    
  }
 */ 

  // testing memeory copy
  {
    std::cout << " ==== testing data structure size ==" << std::endl;

    std::cout << "$$$$$$$$$$$$$$$" << sizeof(ObjectCacheMsgHeader) << std::endl;
    std::cout << " ==== testing data structure size ==" << std::endl << std::endl;
  }


  // testing basic type
  {
    std::cout << "===== testing ceph::encoding function ======" << std::endl;
    bufferlist bl;

    std::string n("shangdehao ");
    bool bool_test = true;
    uint64_t int_test = 123456;
   
    ceph::encode(n, bl);
    ceph::encode(bool_test, bl);
    ceph::encode(int_test, bl);

    std::string x;
    bool y;
    uint64_t z;

    auto i = bl.cbegin();
    ceph::decode(x, i);
    ceph::decode(y, i);
    ceph::decode(z, i);

    std::cout << x << std::endl;
    std::cout << y << std::endl;
    std::cout << z << std::endl;

    std::cout << "===== testing ceph::encoding function ======" << std::endl << std::endl;
  }


  // test header
  {

    ObjectCacheMsgHeader header;
    header.seq = 1;
    header.type = 2;
    header.version =3;
    header.mid_len = 4;
    header.data_len = 5;
    header.reserved = 6;

    bufferlist bl;
    
    ::encode(header, bl);

    ObjectCacheMsgHeader res;
    auto i = bl.cbegin();
    ::decode(res, i);

    std::cout << "sizeof ObjectCacheMsgHeader is : " << sizeof(res) << std::endl;
    std::cout << "corresponding bufferlist size is " << bl.length() << std::endl;

    std::cout << " = " << res.seq << " = " << std::endl;
    std::cout << " = " << res.type << " = " << std::endl;
    std::cout << " = " << res.version << " = " << std::endl;
    std::cout << " = " << res.data_len << " = " << std::endl;
    std::cout << " = " << res.reserved << " = " << std::endl;
  }

  // testing request
  {

    std::cout << "==================== test object cache request ==============" << std::endl; 

    ObjectCacheRequest req;
    
    std::cout << "before encode, req_m_head_buffer size is " << req.m_head_buffer.length() << std::endl;
    std::cout << "before encode, req_m_mid_buffer size is " << req.m_mid_buffer.length() << std::endl;
    std::cout << "before encode, req_m_data_buffer size is " << req.m_data_buffer.length() << std::endl;
  
    req.m_head.seq = 1;
    req.m_head.type = 2;
    req.m_head.version =3;
    req.m_head.mid_len =4;
    req.m_head.data_len = 5;
    req.m_head.reserved = 6;

    req.m_mid.m_image_size = 111111;
    req.m_mid.m_read_offset = 222222;
    req.m_mid.m_read_len = 333333;
    req.m_mid.m_pool_name = "this is a pool name ";
    req.m_mid.m_image_name = "this is a volume name ";
    req.m_mid.m_oid = "this is a object name ";


    req.encode();

    std::cout << "after encode, req_m_head_buffer size is " << req.m_head_buffer.length() << std::endl;
    std::cout << "after encode, req_m_mid_buffer size is " << req.m_mid_buffer.length() << std::endl;
    std::cout << "after encode, req_m_data_buffer size is " << req.m_data_buffer.length() << std::endl;

    // ================================


    ObjectCacheRequest* req_decode;

    auto x = req.get_head_buffer();
    auto y = req.get_mid_buffer();
    auto z = req.get_data_buffer();

    req_decode = decode_object_cache_request(x, y , z);

    std::cout << "after decoding, request is as below : " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.seq= " << req_decode->m_head.seq << " = " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.type= " << req_decode->m_head.type << " = " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.version= " << req_decode->m_head.version << " = " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.mid_len= " << req_decode->m_head.mid_len << " = " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.data_len= " << req_decode->m_head.data_len << " = " << std::endl;
    std::cout << " ObjectCacheReqest.m_header.reserved= " << req_decode->m_head.reserved << " = " << std::endl;

    std::cout << " ObjectCacheReqest.m_middle.m_vol_size = " << req_decode->m_mid.m_image_size << std::endl; 
    std::cout << " ObjectCacheReqest.m_middle.m_offset = " << req_decode->m_mid.m_read_offset << std::endl; 
    std::cout << " ObjectCacheReqest.m_middle.m_length = " << req_decode->m_mid.m_read_len << std::endl; 
    std::cout << " ObjectCacheReqest.m_middle.m_pool_name = " << req_decode->m_mid.m_pool_name << std::endl;    
    std::cout << " ObjectCacheReqest.m_middle.m_vol_name = " << req_decode->m_mid.m_image_name << std::endl;    
    std::cout << " ObjectCacheReqest.m_middle.m_oid = " << req_decode->m_mid.m_oid << std::endl;    

  }

  // testing size 
  {


  }

  std::cout << "hello world..." << std::endl;

  return 0;
}

