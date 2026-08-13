/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_RTREE_H_
#define VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_RTREE_H_

#include <cstddef>
#include <functional>
#include <memory>

namespace valkey_search::indexes {

// Axis-aligned bounding box
struct BBox {
  double min_x, min_y, max_x, max_y;

  void Expand(double x, double y);
};

BBox MakeBBox();

// Opaque R-tree implementation (Boost.Geometry R*-tree hidden in .cc)
template <typename T>
class RTree {
 public:
  RTree();
  ~RTree();
  RTree(RTree&&) noexcept;
  RTree& operator=(RTree&&) noexcept;

  void Insert(const BBox& bbox, const T& value);
  void Remove(const BBox& bbox, const T& value);
  void Query(const BBox& query_bbox,
             std::function<void(const T&, const BBox&)> cb) const;
  size_t Size() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_RTREE_H_
