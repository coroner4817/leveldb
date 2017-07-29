// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include <string>

namespace leveldb {

class Histogram {
 public:
  // YW - need to reset all the stats before start
  Histogram() { Clear(); }
  ~Histogram() { }

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

 private:
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;

  enum { kNumBuckets = 154 };
  // YW - static class member only have one copy in the static memory no matter how many times you declare the class instance
  // Access through Histogram::kBucketLimit
  // The static class member must be initlized out of line, 
  // When have a static class variable, when initilize it must remove the static identifier, like this example
  static const double kBucketLimit[kNumBuckets];
  double buckets_[kNumBuckets];

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
