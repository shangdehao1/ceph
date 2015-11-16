// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator.h"
#include "newstore_types.h"
#include "NewStore.h"
#include "FreelistManager.h"

#define dout_subsys ceph_subsys_newstore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc "

StupidAllocator::StupidAllocator()
  : fm(NULL),
    lock("StupicAllocator::lock"),
    num_free(0),
    num_uncommitted(0),
    num_committing(0),
    num_reserved(0),
    free(10),
    last_alloc(0)
{
}

StupidAllocator::~StupidAllocator()
{
}

unsigned StupidAllocator::_choose_bin(uint64_t orig_len)
{
  uint64_t len = orig_len / g_conf->newstore_min_alloc_size;
  assert(len);
  int bin = -1;
  while (len && bin + 1 < (int)free.size()) {
    len >>= 1;
    bin++;
  }
  dout(30) << __func__ << " len " << orig_len << " -> " << bin << dendl;
  return bin;
}

void StupidAllocator::_insert_free(uint64_t off, uint64_t len)
{
  unsigned bin = _choose_bin(len);
  dout(30) << __func__ << " " << off << "~" << len << " in bin " << bin << dendl;
  while (true) {
    free[bin].insert(off, len, &off, &len);
    unsigned newbin = _choose_bin(len);
    if (newbin == bin)
      break;
    dout(30) << __func__ << " promoting " << off << "~" << len
	     << " to bin " << newbin << dendl;
    free[bin].erase(off, len);
    bin = newbin;
  }
}

int StupidAllocator::reserve(uint64_t need)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " need " << need << " num_free " << num_free
	   << " num_reserved " << num_reserved << dendl;
  if (need > num_free - num_reserved)
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

int StupidAllocator::allocate(
  uint64_t need_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " need_size " << need_size
	   << " alloc_unit " << alloc_unit
	   << " hint " << hint
	   << dendl;
  uint64_t want = MAX(alloc_unit, need_size);
  int bin = _choose_bin(want);
  int orig_bin = bin;

  interval_set<uint64_t>::iterator p = free[0].begin();

  if (!hint)
    hint = last_alloc;

  // search up (from hint)
  if (hint) {
    for (bin = orig_bin; bin < (int)free.size(); ++bin) {
      p = free[bin].lower_bound(hint);
      if (p != free[bin].end()) {
	goto found;
      }
    }
  }

  // search up (from origin)
  for (bin = orig_bin; bin < (int)free.size(); ++bin) {
    p = free[bin].begin();
    if (p != free[bin].end()) {
      goto found;
    }
  }

  // search down (hint)
  if (hint) {
    for (bin = orig_bin-1; bin >= 0; --bin) {
      p = free[bin].lower_bound(hint);
      if (p != free[bin].end()) {
	goto found;
      }
    }
  }

  // search down (origin)
  for (bin = orig_bin-1; bin >= 0; --bin) {
    p = free[bin].begin();
    if (p != free[bin].end()) {
      goto found;
    }
  }

  assert(0 == "caller didn't reserve?");
  return -ENOSPC;

 found:
  *offset = p.get_start();
  *length = MIN(MAX(alloc_unit, need_size), p.get_len());
  dout(30) << __func__ << " got " << *offset << "~" << *length << " from bin "
	   << bin << dendl;

  free[bin].erase(p.get_start(), *length);
  uint64_t off, len;
  if (p.get_start() && free[bin].contains(p.get_start() - 1, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting " << off << "~" << len
	       << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }
  if (free[bin].contains(p.get_start() + *length, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {
      dout(30) << __func__ << " demoting " << off << "~" << len
	       << " to bin " << newbin << dendl;
      free[bin].erase(off, len);
      _insert_free(off, len);
    }
  }

  num_free -= *length;
  num_reserved -= need_size;
  last_alloc = *offset + *length;
  return 0;
}

int StupidAllocator::release(
  uint64_t offset, uint64_t length)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  uncommitted.insert(offset, length);
  num_uncommitted += length;
  return 0;
}

void StupidAllocator::dump(ostream& out)
{
  Mutex::Locker l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    dout(30) << __func__ << " free bin " << bin << ": "
	     << free[bin].num_intervals() << " extents" << dendl;
    for (interval_set<uint64_t>::iterator p = free[bin].begin();
	 p != free[bin].end();
	 ++p) {
      dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
    }
  }
  dout(30) << __func__ << " committing: "
	   << committing.num_intervals() << " extents" << dendl;
  for (interval_set<uint64_t>::iterator p = committing.begin();
       p != committing.end();
       ++p) {
    dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
  }
  dout(30) << __func__ << " uncommitted: "
	   << uncommitted.num_intervals() << " extents" << dendl;
  for (interval_set<uint64_t>::iterator p = uncommitted.begin();
       p != uncommitted.end();
       ++p) {
    dout(30) << __func__ << "  " << p.get_start() << "~" << p.get_len() << dendl;
  }
  fm->dump();
}

int StupidAllocator::init(FreelistManager *f)
{
  dout(1) << __func__ << dendl;
  fm = f;

  // load state from freelist
  unsigned num = 0;
  const map<uint64_t,uint64_t>& fl = fm->get_freelist();
  for (map<uint64_t,uint64_t>::const_iterator p = fl.begin(); p != fl.end(); ++p) {
    dout(30) << "  " << p->first << "~" << p->second << dendl;
    _insert_free(p->first, p->second);
    ++num;
    num_free += p->second;
  }

  dout(10) << __func__ << " loaded " << pretty_si_t(num_free)
	   << " in " << num << " extents"
	   << dendl;
  return 0;
}

void StupidAllocator::shutdown()
{
  dout(1) << __func__ << dendl;
}

void StupidAllocator::commit_start()
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " releasing " << num_uncommitted
	   << " in extents " << uncommitted.num_intervals() << dendl;
  assert(committing.empty());
  committing.swap(uncommitted);
  num_committing = num_uncommitted;
  num_uncommitted = 0;
}

void StupidAllocator::commit_finish()
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " released " << num_committing
	   << " in extents " << committing.num_intervals() << dendl;
  for (interval_set<uint64_t>::iterator p = committing.begin();
       p != committing.end();
       ++p) {
    _insert_free(p.get_start(), p.get_len());
  }
  committing.clear();
  num_free += num_committing;
  num_committing = 0;
}
