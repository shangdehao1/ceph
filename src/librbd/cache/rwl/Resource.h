#ifndef RWL_RESOURCE_H
#define RWL_RESOURCE_H

#include "PmemLayout.h"

namespace librbd {
namespace cache {

struct WriteBufferAllocation
{
  unsigned int allocation_size = 0;

  pobj_action buffer_alloc_action;

  //  alloc    : C_WriteRequest::alloc_resource 
  //  transfer : C_WriteRequest::dispatch 
  TOID(uint8_t) buffer_oid = OID_NULL;

  bool allocated = false;
  utime_t allocation_lat;
};

struct WriteRequestResources
{
  bool allocated = false;
  std::vector<WriteBufferAllocation> buffers;
};


} // namespace cache
} // namespace librbd

#endif
