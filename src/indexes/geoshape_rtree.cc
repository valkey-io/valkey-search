/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/geoshape_rtree.h"

#include <algorithm>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <utility>
#include <vector>

#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using BoostPoint = bg::model::point<double, 2, bg::cs::cartesian>;
using BoostBox = bg::model::box<BoostPoint>;

static BoostBox ToBoostBox(const BBox& b) {
  return BoostBox(BoostPoint(b.min_x, b.min_y), BoostPoint(b.max_x, b.max_y));
}

void BBox::Expand(double x, double y) {
  min_x = std::min(min_x, x);
  min_y = std::min(min_y, y);
  max_x = std::max(max_x, x);
  max_y = std::max(max_y, y);
}

BBox MakeBBox() {
  return BBox{std::numeric_limits<double>::max(),
              std::numeric_limits<double>::max(),
              std::numeric_limits<double>::lowest(),
              std::numeric_limits<double>::lowest()};
}

// Explicit instantiation for InternedStringPtr
using T = InternedStringPtr;
using Value = std::pair<BoostBox, T>;
using TreeType = bgi::rtree<Value, bgi::rstar<16>>;

template <>
struct RTree<T>::Impl {
  TreeType tree;
};

template <>
RTree<T>::RTree() : impl_(std::make_unique<Impl>()) {}

template <>
RTree<T>::~RTree() = default;

template <>
RTree<T>::RTree(RTree&&) noexcept = default;

template <>
RTree<T>& RTree<T>::operator=(RTree&&) noexcept = default;

template <>
void RTree<T>::Insert(const BBox& bbox, const T& value) {
  impl_->tree.insert(std::make_pair(ToBoostBox(bbox), value));
}

template <>
void RTree<T>::Remove(const BBox& bbox, const T& value) {
  impl_->tree.remove(std::make_pair(ToBoostBox(bbox), value));
}

template <>
void RTree<T>::Query(const BBox& query_bbox,
                     std::function<void(const T&, const BBox&)> cb) const {
  auto boost_box = ToBoostBox(query_bbox);
  for (auto it = impl_->tree.qbegin(bgi::intersects(boost_box));
       it != impl_->tree.qend(); ++it) {
    BBox entry_bbox{bg::get<bg::min_corner, 0>(it->first),
                    bg::get<bg::min_corner, 1>(it->first),
                    bg::get<bg::max_corner, 0>(it->first),
                    bg::get<bg::max_corner, 1>(it->first)};
    cb(it->second, entry_bbox);
  }
}

template <>
size_t RTree<T>::Size() const {
  return impl_->tree.size();
}

}  // namespace valkey_search::indexes
