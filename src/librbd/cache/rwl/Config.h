#ifndef RWL_CONFIG_H
#define RWL_CONFIG_H

namespace librbd {
namespace cache {
namespace rwl {

static const bool RWL_VERBOSE_LOGGING = true;

} // namespace rwl 

static const bool COPY_PMEM_FOR_READ = true;
const unsigned long int ops_appended_together = MAX_ALLOC_PER_TRANSACTION;
const unsigned long int ops_flushed_together = 4;
static const unsigned int HANDLE_FLUSHED_SYNC_POINT_RECURSE_LIMIT = 4;

}
}

#endif
