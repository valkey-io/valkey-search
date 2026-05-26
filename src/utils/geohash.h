/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * 52-bit geohash math, ported from valkey-core/src/geohash.{c,h} and
 * geohash_helper.{c,h}. The algorithm is identical: longitude and latitude
 * are mapped to 26-bit integers in their respective ranges and bit-interleaved
 * (Morton / Z-order) into a 52-bit value. The score type here is uint64_t
 * (top 12 bits zero) rather than a double, eliminating the float quantization
 * that valkey-core's zset-based GEO* commands incur.
 */

#ifndef VALKEYSEARCH_SRC_UTILS_GEOHASH_H_
#define VALKEYSEARCH_SRC_UTILS_GEOHASH_H_

#include <array>
#include <cstdint>
#include <utility>

namespace valkey_search::geohash {

inline constexpr int kStepMax = 26;  // 26 * 2 = 52 bits
inline constexpr double kLatMin = -85.05112878;
inline constexpr double kLatMax = 85.05112878;
inline constexpr double kLonMin = -180.0;
inline constexpr double kLonMax = 180.0;

// Earth's quadratic mean radius for WGS-84.
inline constexpr double kEarthRadiusMeters = 6372797.560856;

struct Bits {
  uint64_t bits;
  uint8_t step;
};

struct Range {
  double min;
  double max;
};

struct Area {
  Bits hash;
  Range lon;
  Range lat;
};

struct Neighbors {
  Bits north;
  Bits east;
  Bits west;
  Bits south;
  Bits north_east;
  Bits south_east;
  Bits north_west;
  Bits south_west;
};

enum class ShapeType { kCircular, kRectangle };

struct Shape {
  ShapeType type;
  std::array<double, 2> xy;       // [0]: lon, [1]: lat
  double conversion;              // m=1, km=1000, mi=1609.34, ft=0.3048
  std::array<double, 4> bounds;   // {min_lon, min_lat, max_lon, max_lat}
  union {
    double radius;                // kCircular
    struct {
      double height;
      double width;
    } rect;                       // kRectangle
  } u;
};

struct RadiusCells {
  Bits hash;
  Area area;
  Neighbors neighbors;
};

inline bool HashIsZero(const Bits& b) { return b.bits == 0 && b.step == 0; }
inline bool RangeIsZero(const Range& r) { return r.min == 0.0 && r.max == 0.0; }

void GetCoordRange(Range* lon_range, Range* lat_range);

bool Encode(const Range& lon_range, const Range& lat_range,
            double longitude, double latitude, uint8_t step, Bits* out);
bool EncodeWGS84(double longitude, double latitude, uint8_t step, Bits* out);

bool Decode(Range lon_range, Range lat_range, Bits hash, Area* out);
bool DecodeWGS84(Bits hash, Area* out);
bool DecodeAreaToLonLat(const Area& area, std::array<double, 2>* xy);
bool DecodeToLonLatWGS84(Bits hash, std::array<double, 2>* xy);

void GetNeighbors(Bits hash, Neighbors* neighbors);

uint8_t EstimateStepsByRadius(double range_meters, double lat);
bool BoundingBox(Shape* shape, std::array<double, 4>* bounds);
RadiusCells CalculateAreasByShapeWGS84(Shape* shape);

// Top-align the hash bits to a 52-bit field (the high 12 bits are zero).
// This is the integer score the index keys on. Matches valkey-core's
// geohashAlign52Bits.
uint64_t Align52Bits(const Bits& hash);

// [min, max) score range covering all leaf cells inside the given hash box.
// max is exclusive. Equivalent to valkey-core's scoresOfGeoHashBox.
std::pair<uint64_t, uint64_t> ScoreRangeOfBox(Bits hash);

double GetLatDistance(double lat1d, double lat2d);
double GetDistance(double lon1d, double lat1d, double lon2d, double lat2d);

bool DistanceIfInRadius(double x1, double y1, double x2, double y2,
                        double radius, double* distance);
bool DistanceIfInRectangle(double width_m, double height_m,
                           double x1, double y1, double x2, double y2,
                           double* distance);

}  // namespace valkey_search::geohash

#endif  // VALKEYSEARCH_SRC_UTILS_GEOHASH_H_
