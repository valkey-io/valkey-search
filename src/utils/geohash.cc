/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * C++ port of valkey-core/src/geohash.c and geohash_helper.c. The math is
 * unchanged; only types and namespacing differ. See geohash.h for the
 * deviations from valkey-core (uint64_t scores instead of doubles).
 */

#include "src/utils/geohash.h"

#include <cmath>
#include <cstdint>

namespace valkey_search::geohash {
namespace {

constexpr double kPi = 3.14159265358979323846;
constexpr double kDegToRad = kPi / 180.0;
constexpr double kMercatorMax = 20037726.37;

inline double DegToRad(double d) { return d * kDegToRad; }
inline double RadToDeg(double r) { return r / kDegToRad; }

// Bit-interleave the low bits of x and y; x in even positions, y in odd.
// x and y must initially be < 2^32.
// From https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
inline uint64_t Interleave64(uint32_t xlo, uint32_t ylo) {
  static constexpr uint64_t B[] = {
      0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
      0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL};
  static constexpr unsigned int S[] = {1, 2, 4, 8, 16};

  uint64_t x = xlo;
  uint64_t y = ylo;

  x = (x | (x << S[4])) & B[4];
  y = (y | (y << S[4])) & B[4];

  x = (x | (x << S[3])) & B[3];
  y = (y | (y << S[3])) & B[3];

  x = (x | (x << S[2])) & B[2];
  y = (y | (y << S[2])) & B[2];

  x = (x | (x << S[1])) & B[1];
  y = (y | (y << S[1])) & B[1];

  x = (x | (x << S[0])) & B[0];
  y = (y | (y << S[0])) & B[0];

  return x | (y << 1);
}

// Reverse of Interleave64.
inline uint64_t Deinterleave64(uint64_t interleaved) {
  static constexpr uint64_t B[] = {
      0x5555555555555555ULL, 0x3333333333333333ULL, 0x0F0F0F0F0F0F0F0FULL,
      0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
  static constexpr unsigned int S[] = {0, 1, 2, 4, 8, 16};

  uint64_t x = interleaved;
  uint64_t y = interleaved >> 1;

  x = (x | (x >> S[0])) & B[0];
  y = (y | (y >> S[0])) & B[0];

  x = (x | (x >> S[1])) & B[1];
  y = (y | (y >> S[1])) & B[1];

  x = (x | (x >> S[2])) & B[2];
  y = (y | (y >> S[2])) & B[2];

  x = (x | (x >> S[3])) & B[3];
  y = (y | (y >> S[3])) & B[3];

  x = (x | (x >> S[4])) & B[4];
  y = (y | (y >> S[4])) & B[4];

  x = (x | (x >> S[5])) & B[5];
  y = (y | (y >> S[5])) & B[5];

  return x | (y << 32);
}

void MoveX(Bits* hash, int8_t d) {
  if (d == 0) return;
  uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
  uint64_t y = hash->bits & 0x5555555555555555ULL;
  uint64_t zz = 0x5555555555555555ULL >> (64 - hash->step * 2);
  if (d > 0) {
    x = x + (zz + 1);
  } else {
    x = x | zz;
    x = x - (zz + 1);
  }
  x &= (0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2));
  hash->bits = (x | y);
}

void MoveY(Bits* hash, int8_t d) {
  if (d == 0) return;
  uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
  uint64_t y = hash->bits & 0x5555555555555555ULL;
  uint64_t zz = 0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2);
  if (d > 0) {
    y = y + (zz + 1);
  } else {
    y = y | zz;
    y = y - (zz + 1);
  }
  y &= (0x5555555555555555ULL >> (64 - hash->step * 2));
  hash->bits = (x | y);
}

}  // namespace

void GetCoordRange(Range* lon_range, Range* lat_range) {
  lon_range->max = kLonMax;
  lon_range->min = kLonMin;
  lat_range->max = kLatMax;
  lat_range->min = kLatMin;
}

bool Encode(const Range& lon_range, const Range& lat_range,
            double longitude, double latitude, uint8_t step, Bits* out) {
  if (out == nullptr || step > 32 || step == 0 || RangeIsZero(lat_range) ||
      RangeIsZero(lon_range)) {
    return false;
  }
  if (longitude > kLonMax || longitude < kLonMin || latitude > kLatMax ||
      latitude < kLatMin) {
    return false;
  }

  out->bits = 0;
  out->step = step;

  if (latitude < lat_range.min || latitude > lat_range.max ||
      longitude < lon_range.min || longitude > lon_range.max) {
    return false;
  }

  double lat_offset =
      (latitude - lat_range.min) / (lat_range.max - lat_range.min);
  double lon_offset =
      (longitude - lon_range.min) / (lon_range.max - lon_range.min);

  lat_offset *= (1ULL << step);
  lon_offset *= (1ULL << step);
  out->bits = Interleave64(static_cast<uint32_t>(lat_offset),
                           static_cast<uint32_t>(lon_offset));
  return true;
}

bool EncodeWGS84(double longitude, double latitude, uint8_t step, Bits* out) {
  Range lon_range, lat_range;
  GetCoordRange(&lon_range, &lat_range);
  return Encode(lon_range, lat_range, longitude, latitude, step, out);
}

bool Decode(Range lon_range, Range lat_range, Bits hash, Area* out) {
  if (HashIsZero(hash) || out == nullptr || RangeIsZero(lat_range) ||
      RangeIsZero(lon_range)) {
    return false;
  }
  out->hash = hash;
  uint8_t step = hash.step;
  uint64_t hash_sep = Deinterleave64(hash.bits);  // [LAT][LON]

  double lat_scale = lat_range.max - lat_range.min;
  double lon_scale = lon_range.max - lon_range.min;

  uint32_t ilato = static_cast<uint32_t>(hash_sep);
  uint32_t ilono = static_cast<uint32_t>(hash_sep >> 32);

  out->lat.min =
      lat_range.min + (ilato * 1.0 / (1ull << step)) * lat_scale;
  out->lat.max =
      lat_range.min + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
  out->lon.min =
      lon_range.min + (ilono * 1.0 / (1ull << step)) * lon_scale;
  out->lon.max =
      lon_range.min + ((ilono + 1) * 1.0 / (1ull << step)) * lon_scale;
  return true;
}

bool DecodeWGS84(Bits hash, Area* out) {
  Range lon_range, lat_range;
  GetCoordRange(&lon_range, &lat_range);
  return Decode(lon_range, lat_range, hash, out);
}

bool DecodeAreaToLonLat(const Area& area, std::array<double, 2>* xy) {
  if (xy == nullptr) return false;
  (*xy)[0] = (area.lon.min + area.lon.max) / 2;
  if ((*xy)[0] > kLonMax) (*xy)[0] = kLonMax;
  if ((*xy)[0] < kLonMin) (*xy)[0] = kLonMin;
  (*xy)[1] = (area.lat.min + area.lat.max) / 2;
  if ((*xy)[1] > kLatMax) (*xy)[1] = kLatMax;
  if ((*xy)[1] < kLatMin) (*xy)[1] = kLatMin;
  return true;
}

bool DecodeToLonLatWGS84(Bits hash, std::array<double, 2>* xy) {
  Area area = {};
  if (xy == nullptr || !DecodeWGS84(hash, &area)) return false;
  return DecodeAreaToLonLat(area, xy);
}

void GetNeighbors(Bits hash, Neighbors* neighbors) {
  neighbors->east = hash;
  neighbors->west = hash;
  neighbors->north = hash;
  neighbors->south = hash;
  neighbors->south_east = hash;
  neighbors->south_west = hash;
  neighbors->north_east = hash;
  neighbors->north_west = hash;

  MoveX(&neighbors->east, 1);
  MoveY(&neighbors->east, 0);

  MoveX(&neighbors->west, -1);
  MoveY(&neighbors->west, 0);

  MoveX(&neighbors->south, 0);
  MoveY(&neighbors->south, -1);

  MoveX(&neighbors->north, 0);
  MoveY(&neighbors->north, 1);

  MoveX(&neighbors->north_west, -1);
  MoveY(&neighbors->north_west, 1);

  MoveX(&neighbors->north_east, 1);
  MoveY(&neighbors->north_east, 1);

  MoveX(&neighbors->south_east, 1);
  MoveY(&neighbors->south_east, -1);

  MoveX(&neighbors->south_west, -1);
  MoveY(&neighbors->south_west, -1);
}

uint8_t EstimateStepsByRadius(double range_meters, double lat) {
  if (range_meters == 0) return 26;
  int step = 1;
  while (range_meters < kMercatorMax) {
    range_meters *= 2;
    step++;
  }
  step -= 2;  // Make sure range is included in most of the base cases.

  // Wider range towards the poles.
  if (lat > 66 || lat < -66) {
    step--;
    if (lat > 80 || lat < -80) step--;
  }

  if (step < 1) step = 1;
  if (step > 26) step = 26;
  return static_cast<uint8_t>(step);
}

bool BoundingBox(Shape* shape, std::array<double, 4>* bounds) {
  if (bounds == nullptr) return false;
  double height = 0.0, width = 0.0;
  if (shape->type == ShapeType::kCircular) {
    height = shape->conversion * shape->u.radius;
    width = shape->conversion * shape->u.radius;
  } else if (shape->type == ShapeType::kRectangle) {
    height = shape->conversion * shape->u.rect.height / 2;
    width = shape->conversion * shape->u.rect.width / 2;
  }
  double longitude = shape->xy[0];
  double latitude = shape->xy[1];
  const double lat_delta = RadToDeg(height / kEarthRadiusMeters);
  const double lon_delta_top =
      RadToDeg(width / kEarthRadiusMeters /
               std::cos(DegToRad(latitude + lat_delta)));
  const double lon_delta_bottom =
      RadToDeg(width / kEarthRadiusMeters /
               std::cos(DegToRad(latitude - lat_delta)));
  // Northern vs southern: choose corners that maximize bounds.
  bool southern = latitude < 0;
  (*bounds)[0] = southern ? longitude - lon_delta_bottom
                          : longitude - lon_delta_top;
  (*bounds)[2] = southern ? longitude + lon_delta_bottom
                          : longitude + lon_delta_top;
  (*bounds)[1] = latitude - lat_delta;
  (*bounds)[3] = latitude + lat_delta;
  return true;
}

RadiusCells CalculateAreasByShapeWGS84(Shape* shape) {
  Range lon_range, lat_range;
  RadiusCells radius;
  Bits hash;
  Neighbors neighbors;
  Area area;

  BoundingBox(shape, &shape->bounds);
  double min_lon = shape->bounds[0];
  double min_lat = shape->bounds[1];
  double max_lon = shape->bounds[2];
  double max_lat = shape->bounds[3];

  double longitude = shape->xy[0];
  double latitude = shape->xy[1];
  double radius_meters = 0.0;
  if (shape->type == ShapeType::kCircular) {
    radius_meters = shape->u.radius;
  } else if (shape->type == ShapeType::kRectangle) {
    radius_meters = std::sqrt(
        (shape->u.rect.width / 2) * (shape->u.rect.width / 2) +
        (shape->u.rect.height / 2) * (shape->u.rect.height / 2));
  }
  radius_meters *= shape->conversion;

  uint8_t steps = EstimateStepsByRadius(radius_meters, latitude);

  GetCoordRange(&lon_range, &lat_range);
  Encode(lon_range, lat_range, longitude, latitude, steps, &hash);
  GetNeighbors(hash, &neighbors);
  Decode(lon_range, lat_range, hash, &area);

  // If the estimated step is too coarse to cover the bounding box, drop one.
  bool decrease_step = false;
  {
    Area north, south, east, west;
    Decode(lon_range, lat_range, neighbors.north, &north);
    Decode(lon_range, lat_range, neighbors.south, &south);
    Decode(lon_range, lat_range, neighbors.east, &east);
    Decode(lon_range, lat_range, neighbors.west, &west);
    if (north.lat.max < max_lat) decrease_step = true;
    if (south.lat.min > min_lat) decrease_step = true;
    if (east.lon.max < max_lon) decrease_step = true;
    if (west.lon.min > min_lon) decrease_step = true;
  }

  if (steps > 1 && decrease_step) {
    steps--;
    Encode(lon_range, lat_range, longitude, latitude, steps, &hash);
    GetNeighbors(hash, &neighbors);
    Decode(lon_range, lat_range, hash, &area);
  }

  // Drop neighbors entirely outside the bounding box.
  auto zero = [](Bits& b) { b.bits = 0; b.step = 0; };
  if (steps >= 2) {
    if (area.lat.min < min_lat) {
      zero(neighbors.south);
      zero(neighbors.south_west);
      zero(neighbors.south_east);
    }
    if (area.lat.max > max_lat) {
      zero(neighbors.north);
      zero(neighbors.north_east);
      zero(neighbors.north_west);
    }
    if (area.lon.min < min_lon) {
      zero(neighbors.west);
      zero(neighbors.south_west);
      zero(neighbors.north_west);
    }
    if (area.lon.max > max_lon) {
      zero(neighbors.east);
      zero(neighbors.south_east);
      zero(neighbors.north_east);
    }
  }
  radius.hash = hash;
  radius.neighbors = neighbors;
  radius.area = area;
  return radius;
}

uint64_t Align52Bits(const Bits& hash) {
  uint64_t bits = hash.bits;
  bits <<= (52 - hash.step * 2);
  return bits;
}

std::pair<uint64_t, uint64_t> ScoreRangeOfBox(Bits hash) {
  uint64_t lo = Align52Bits(hash);
  Bits next = hash;
  next.bits++;
  uint64_t hi = Align52Bits(next);
  return {lo, hi};
}

double GetLatDistance(double lat1d, double lat2d) {
  return kEarthRadiusMeters *
         std::fabs(DegToRad(lat2d) - DegToRad(lat1d));
}

double GetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
  double lon1r = DegToRad(lon1d);
  double lon2r = DegToRad(lon2d);
  double v = std::sin((lon2r - lon1r) / 2);
  // ~6 nm threshold; below this, treat longitudes as coincident and skip
  // the more expensive haversine, matching valkey-core's behavior.
  constexpr double kEpsilon = 1e-15;
  if (std::fabs(v) <= kEpsilon) return GetLatDistance(lat1d, lat2d);
  double lat1r = DegToRad(lat1d);
  double lat2r = DegToRad(lat2d);
  double u = std::sin((lat2r - lat1r) / 2);
  double a = u * u + std::cos(lat1r) * std::cos(lat2r) * v * v;
  return 2.0 * kEarthRadiusMeters * std::asin(std::sqrt(a));
}

bool DistanceIfInRadius(double x1, double y1, double x2, double y2,
                        double radius, double* distance) {
  *distance = GetDistance(x1, y1, x2, y2);
  return *distance <= radius;
}

bool DistanceIfInRectangle(double width_m, double height_m,
                           double x1, double y1, double x2, double y2,
                           double* distance) {
  double lat_dist = GetLatDistance(y2, y1);
  if (lat_dist > height_m / 2) return false;
  double lon_dist = GetDistance(x2, y2, x1, y2);
  if (lon_dist > width_m / 2) return false;
  *distance = GetDistance(x1, y1, x2, y2);
  return true;
}

}  // namespace valkey_search::geohash
