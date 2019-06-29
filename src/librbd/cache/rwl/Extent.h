#ifndef RWL_EXTENT_H
#define RWL_EXTENT_H

namespace librbd {
namespace cache {

namespace rwl {

/**
 * Extent --> [offset, len]
 * BlockExtent --> [first_byte, last_byte]  ---> librbd/BlockGuard.h
 * 
 */

typedef ReplicatedWriteLog<ImageCtx>::Extent Extent;
typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

// =================== extent =========================

/*
 * A BlockExtent identifies a range by first and last.
 *
 * An Extent ("image extent") identifies a range by start and length.
 *
 * The ImageCache interface is defined in terms of image extents, and
 * requires no alignment of the beginning or end of the extent. We
 * convert between image and block extents here using a "block size" of 1.
 */
// <offset, len> ---> BlockExtent
const BlockExtent block_extent(const uint64_t offset_bytes, const uint64_t length_bytes) {
  return BlockExtent(offset_bytes, offset_bytes + length_bytes - 1);
}

// Extent --> BlockExtent
const BlockExtent block_extent(const Extent& image_extent) {
  return block_extent(image_extent.first, image_extent.second);
}

// BlockExtent --> Extent
const Extent image_extent(const BlockExtent& block_extent) {
  return Extent(block_extent.block_start, block_extent.block_end - block_extent.block_start + 1);
}

const BlockExtent WriteLogPmemEntry::block_extent() {
  return BlockExtent(librbd::cache::rwl::block_extent(image_offset_bytes, write_bytes));
}


} // namespace rwl


template <typename ExtentsType>
class ExtentsSummary
{
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;

  friend std::ostream &operator<<(std::ostream &os, const ExtentsSummary &s)
  {
    os << "total_bytes=" << s.total_bytes << ", " << "first_image_byte=" 
       << s.first_image_byte << ", last_image_byte= "<< s.last_image_byte << "";
    return os;
  };

  ExtentsSummary(const ExtentsType &extents)
  {
    total_bytes = 0;
    first_image_byte = 0;
    last_image_byte = 0;

    if (extents.empty()) return;

    /* These extents refer to image offsets between first_image_byte and last_image_byte, inclusive,
     * but we don't guarantee here that they address all of those bytes. There may be gaps. */
    first_image_byte = extents.front().first;
    last_image_byte = first_image_byte + extents.front().second;

    for (auto &extent : extents)
    {
      /* Ignore zero length extents */
      if (extent.second)
      {
        total_bytes += extent.second;

        // try to update the first image_byte
        if (extent.first < first_image_byte) {
          first_image_byte = extent.first;
        }

        // try to update the last image_byte
        if ((extent.first + extent.second) > last_image_byte) {
          last_image_byte = extent.first + extent.second;
        }
      }
    }
  }

  const BlockExtent block_extent()
  {
    return BlockExtent(first_image_byte, last_image_byte);
  }

  // [first_image_byte, len]
  const Extent image_extent()
  {
    return rwl::image_extent(block_extent());
  }
};


struct ImageExtentBuf : public Extent
{
public:
  bufferlist m_bl;

  ImageExtentBuf(Extent extent, buffer::raw *buf = nullptr)
   : Extent(extent) {
    if (buf) {
      m_bl.append(buf);
    }
  }

  ImageExtentBuf(Extent extent, bufferlist bl) : Extent(extent), m_bl(bl) {}
};

typedef std::vector<ImageExtentBuf> ImageExtentBufs;


} // namespace cache
} // namespace librbd


#endif
