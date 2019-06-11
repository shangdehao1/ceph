#ifndef DEFERRED_CONTEXTS_H
#define DEFERRED_CONTEXTS_H

#include "include/Context.h"

namespace librbd {
namespace cache {
namespace rwl {

/* Used for deferring work on a given thread until a required lock is dropped.*/

class DeferredContexts
{
public:
  std::vector<Context*> contexts;

public:
  ~DeferredContexts() {
    finish_contexts(nullptr, contexts, 0);
  }

  void add(Context* ctx) {
    contexts.push_back(ctx);
  }
};


} // namespace rwl
} // namespace cache
} // namespace librbd
#endif
