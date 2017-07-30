// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <vector>
#include <thread>
#include <mutex>
#include <iostream>
#include "util/coding.h"
#include "util/testharness.h"

namespace leveldb {

// Conversions between numeric keys/values and the types expected by Cache.
// YW - encode or decode the key(Slice), base on little endian or big endian
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
// YW - reinterpret_cast the uintptr_t int to void*, is this means that we can reinterpret_cast anything to anything?
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest {
 public:
  // YW - TODO: understand why this singleton design pattern is here while we actuall have multiple instance of this class
  static CacheTest* current_;

  // YW - deleter here is just push to vector, for test deleter only
  // deleted_* vectors are not static, so that they are not shared by all the _TEST_name, each one hold their own deleted_* vector
  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static const int kCacheSize = 1000;
  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  Cache* cache_;

  // YW - initilize current_ to this;
  CacheTest() : cache_(NewLRUCache(kCacheSize)) {
    current_ = this;
  }

  ~CacheTest() {
    delete cache_;
  }
  // YW - this is a release handle lookup so the refs is not increased
  int Lookup(int key) {
    Cache::Handle* handle = cache_->Lookup(EncodeKey(key));
    const int r = (handle == NULL) ? -1 : DecodeValue(cache_->Value(handle));
    if (handle != NULL) {
      cache_->Release(handle);
    }
    return r;
  }

  // YW - do an extra release so the refs after is 1, here we don't want to hold a handle after insert so the refs is 1
  void Insert(int key, int value, int charge = 1) {
    cache_->Release(cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                                   &CacheTest::Deleter));
  }

  // YW - refs afterward is 2, because we hold a handle
  Cache::Handle* InsertAndReturnHandle(int key, int value, int charge = 1) {
    return cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                          &CacheTest::Deleter);
  }

  void Erase(int key) {
    cache_->Erase(EncodeKey(key));
  }

  void ShowTable() const{
    cache_->ShowTable();
  }

  void Prune(){
    cache_->Prune();
  }

  void ShowCacheListSize() const{
    cache_->ShowCacheListSize(); 
  }
};
// YW - declare this so that we can invoke the CacheTest constructor, then we can get current_ initialized in the static memory 
// Global static pointer used to ensure a single instance of the class.
CacheTest* CacheTest::current_;

// YW - every test use its own CacheTest instance, this because everytime we will declare a derived _TEST_name class
// everytime we will initilize a NewLRUCache
TEST(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  // YW - calling base class method, doesn't need to be Base::Insert();
  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  // YW - only have one replacement so the delete count is 1
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  // YW - at end of this scope, the CacheTest instance is destoried, so that the cache_ is destoried, so the deconstructor was called
  // so at this time there should be three entries in the deleted_* vectors, but since deleted_* is not static so they are also released afterward 
}

TEST(CacheTest, Erase) {
  // YW - Notice that there is no key=200 in the cache now (Lookup(200)=-1), Erase(200) do nothing here
  // so ShowTable() here shows empty cache, also deleted_keys_ is empty at this time, reason check 5 lines above
  // ShowTable();
  // ASSERT_EQ(0, deleted_keys_.size());
  Erase(200);
  ASSERT_EQ(0, deleted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
}

TEST(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  // (100)->refs = 1, due to there is a release at Insert()!
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  // (100)->refs = 2
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0, deleted_keys_.size());

  cache_->Release(h1);
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  // YW - not actual delete due to the refs is 2 here, so just act as a release
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, deleted_keys_.size());

  // YW - at here we delete h2
  cache_->Release(h2);
  ASSERT_EQ(2, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[1]);
  ASSERT_EQ(102, deleted_values_[1]);
}

TEST(CacheTest, YWEraseTest){
  Insert(100, 101);
  // ShowTable(); // refs = 1
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  // ShowTable(); // refs = 2
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  // ShowTable(); // refs = 3
  cache_->Release(h1);
  // ShowTable(); // refs = 2
  cache_->Release(h2);
  // ShowTable(); // refs = 1

  // YW - when refs = 1 we can safely delete it now
  Erase(100);
  ASSERT_EQ(1, deleted_keys_.size());
}

TEST(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);
  Insert(300, 301);
  Cache::Handle* h = cache_->Lookup(EncodeKey(300));

  // Frequently used entry must be kept around,
  // as must things that are still in use.
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000+i, 2000+i);
    ASSERT_EQ(2000+i, Lookup(1000+i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(301, Lookup(300));
  cache_->Release(h);
  // YW - after this the key=300's refs is 1 still not deleted, but we must do a release here or the LRUCache deconstructor throw assert error
  // all the storage will be deleted when cache deconstructor is called
}

TEST(CacheTest, UseExceedsCacheSize) {
  // Overfill the cache, keeping handles on all inserted entries.
  std::vector<Cache::Handle*> h;
  for (int i = 0; i < kCacheSize + 100; i++) {
    h.push_back(InsertAndReturnHandle(1000+i, 2000+i));
  }
  // YW - notice after InsertAndReturnHandle all the 1100 inserted item's refs is 2, and they are on in_use_
  // However when use Insert, all the item will be push into lru_ due to have one release op
  // When overfill happened, they will not be deleted but push to lru_ list due to lru_ is empty
  // lru_ only be clear when prune() is called 
  // ShowCacheListSize();

  // Check that all the entries can be found in the cache.
  for (int i = 0; i < h.size(); i++) {
    ASSERT_EQ(2000+i, Lookup(1000+i));
  }

  for (int i = 0; i < h.size(); i++) {
    cache_->Release(h[i]);
  }
}

TEST(CacheTest, YWExceedCacheSize){
  std::vector<Cache::Handle*> h;
  for (int i = 0; i < kCacheSize + 100; i++) {
    h.push_back(InsertAndReturnHandle(0+i, 2000+i));
  }

  // YW - the second Insert has less refs due to not return a handle
  // So when the size of the usage_ larger than capacity, all of this insert goto lru_ and then size of lru_ is always around 1
  for(int i=0;i<kCacheSize+100;i++){
    Insert(1200+i, 2000+i);
  }

  // ShowCacheListSize();
  // ShowTable();

  for (int i = 0; i < h.size(); i++) {
    cache_->Release(h[i]);
  }
}

TEST(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2*kCacheSize) {
    // YW - 50% of entry is heavy
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000+index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize/10);
}

TEST(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}

TEST(CacheTest, Prune) {
  Insert(1, 100);
  Insert(2, 200);

  Cache::Handle* handle = cache_->Lookup(EncodeKey(1));
  ASSERT_TRUE(handle);
  // YW - prune only delete key=2
  cache_->Prune();
  cache_->Release(handle);

  ASSERT_EQ(100, Lookup(1));
  ASSERT_EQ(-1, Lookup(2));
}

// YW - Worker will increse each key's value by one
// need have a lock here due to although all those funcs like Lookup and Insert they have mutex lock inside 
// but they will release the lock after the function call, while the worker required everything exec in sequence together
// so here we need a lock; 
// or we need to use the condition_variable to syn each step
void worker1(int key, CacheTest* ct, std::mutex& m_mutex){
  std::lock_guard<std::mutex> lk(m_mutex);
  Cache::Handle* handle = ct->cache_->Lookup(EncodeKey(key));
  ASSERT_TRUE(handle);
  ct->Insert(key, DecodeValue(ct->cache_->Value(handle)) + 1);
  ct->cache_->Release(handle);
}

TEST(CacheTest, YWMultiThread1){
  std::mutex m_mutex;
  const int KEY_ = 100;
  Insert(KEY_, 0);
  const int TEST_THREADS_ = 100;

  std::vector<std::thread> v_;
  for(int i=0;i<TEST_THREADS_;++i){
    std::thread tmp(worker1, KEY_, this, std::ref(m_mutex));
    v_.push_back(std::move(tmp));
  }

  for(int i=0;i<v_.size();++i){
    v_[i].join();
  }

  ASSERT_EQ(100, Lookup(KEY_));
}

void worker2(int key, int value, CacheTest* ct){
  ct->Insert(key, value);
  // print at the insert to see the actual insert sequence 
  // there is no missing in the sequence so the mutex lock in each func works
}

TEST(CacheTest, YWMultiThread2){
  const int KEY_ = 100;
  Insert(KEY_, 0);
  const int TEST_THREADS_ = 100;

  std::vector<std::thread> v_;
  for(int i=0;i<TEST_THREADS_;++i){
    std::thread tmp(worker2, KEY_, i, this);
    v_.push_back(std::move(tmp));
  }

  for(int i=0;i<v_.size();++i){
    v_[i].join();
  }

  ASSERT_GE(99, Lookup(KEY_));
}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
