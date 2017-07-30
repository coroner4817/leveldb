// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// YW - two level cache

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// YW - this LRUHandle is entry of the cache
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  // YW - can memcpy any size of data to a char arr pointer, no matter how you declare
  // but cannot apply this to a pointer if no allocation
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// YW - implement a custom unordered_map
// use hash map to store the key of the LRUHandle and value as the LRUHandle in the list
// The hash is stored in the LRUHandle 
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  // YW - here since FindPointer return a **, so the user of this functino can modify the *
  // but this seems not very useful in this scenerio, only for code reuse
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  // YW - return the old LRUHandle if is a replacement and needed to be released in the outter function
  LRUHandle* Insert(LRUHandle* h) {
    // YW - FindPointer return the place we should insert, always find with hash match && key match. 
    // if there is an XOR, the key generation has a problem
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    // YW - push the original one back and 
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    // YW - if old is exist then just replace it without store the old one
    // So this can ensure that we have only have one in each bucket
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      // YW - resize only when elems > length, this doesn't mean all the bucket has been occupied 
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  // YW - don't need to reassign the (*ptr->prev)->next_hash, since we are dealing with ** 
  // Notice that this ** can be considered as rvalue
  // checkout the notice in the FindPointer func!
  // Because FindPointer return a ** so we can modify the content of the *
  // but not delete the actual memory, user need to delete through the returned handle
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

  // YW - func for debug
  void ToString() const{
    std::cout << "------------------" << std::endl;
    std::cout << "Length: " << length_ << ", elements: " << elems_ << std::endl;
    for(size_t i = 0; i<length_; ++i){
        LRUHandle* h = list_[i];
        bool hasPrintFronter = false, shouldPrintFooter = false;
        while(h){
          if(!hasPrintFronter){
            std::cout << "Bucket " << i << ": ";
            hasPrintFronter = true;
          }
          uint32_t result;
          memcpy(&result, h->key().data(), sizeof(result));
          std::cout << "(" << result << ", " << reinterpret_cast<uintptr_t>(h->value) << ", " << h->refs <<  ")" << " -> "; 
          h = h->next_hash;
          shouldPrintFooter = true;
        }
        if(shouldPrintFooter){
          std::cout << "NULL"  << std::endl;
        }
    }
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  // YW - length_ is the number of buckets
  // elems is the total number of elements in all buckets
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // YW - return a ** so that user can modify the LRUHandle* pointer. user can assign the pointer to new instance
  // Find a hash match and key match, so when Key is different then hash should also be different
  // but here we are using less bucket to store so LRUHandle with different hash might fall into same bucket
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      // YW - notice !!!!!!!!
      // here return is the &prev->next not the &curr!!!! that why we can do the remove above
      // This doesn't affect other funcs like lookup and insert
      // But when deal with remove we can directly replace the prev->next to curr->next to delete cur!
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // YW - Resize is use to both initilze the HashTable and enlarge the table when elems_ < length_
  // this is not a reformat deepcpy of the hashtable but only manipulate the LRUHandle** which is the index of the actual storage
  void Resize() {
    uint32_t new_length = 4;
    // YW - always set length > elems so that we have a sparse hash table
    // notice that new_length is always power of 2
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      // YW - iterate through the whole bucket
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        // YW - (hash & (length_ - 1)) is a simple map from hash to the bucket index
        // new_length is always power of 2 so that each hash will be mapped to a different bucket
        // here use ** so that *ptr = h will actually put the prev LRUHandle* to the new position
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        // YW - replace the new position LRUHandle* to the prev position LRUHandle*
        // and also push the original LRUHandle* at the new position to the next of (*ptr)
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    // YW - MutexLock just like c++11 lock_guard, checkout the src
    MutexLock l(&mutex_);
    return usage_;
  }
  void ShowTable() const{
    MutexLock l(&mutex_);
    table_.ToString();
  };

  const size_t GetListSize(const LRUHandle* list) const{
    MutexLock l(&mutex_);
    LRUHandle* tmp = list->next;
    size_t ret = 0;
    while(tmp!=list){
      ret++;
      tmp = tmp->next;
    }
    return ret;
  };

  const size_t Get_in_use_size() const{
    return GetListSize(&in_use_);
  }
  const size_t Get_lru_size() const{
    return GetListSize(&lru_);
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  // Initialized before use.
  // YW - the capacity of the whole cache, 
  size_t capacity_;

  // mutex_ protects the following state.
  // YW - mutable mean that this variable can be changed even within a const class member function or a const class instance
  // the differece between mutable and volitale: https://stackoverflow.com/questions/2444695/volatile-vs-mutable-in-c
  mutable port::Mutex mutex_;
  size_t usage_;

  // YW - for the difference read the top section of this file, dummy head should not be used to storage
  // YW - all these two list are circular list!!!!
  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_;

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_;

  // YW - the point of using a hashmap to manage the key and the pointer to the actual instance is that
  // we can fast determine whether the object exist in the cache or not. We don't need to go through the list to find it
  // the actual storage is a listnode
  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  // YW - when deconstruct, all the storage in the in_use_ must have been released and left dummy head only
  // This means that all the stroage's refs must be 1, so we must do a release of the lookup handle which will lead the strogae transfer to in_use_
  // if we don't manually released them it throws assert error
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  // YW - release all the storage in the normal lru_ list
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    // YW - use Unref's second functionality
    Unref(e);
    e = next;
  }
}

// YW - move the lru_ list entry to the in_use_ list and always insert to the dummy_head->prev of the in_use_
// if on in_use_ then just increase the refs because in_use_ is not in order
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

// YW - Unref has three functionalities:
// 0, deduce the refs count by one
// 1, delete memory from lru_ when refs==0
// 2, remove the in_use_ entry to the lru_ for refs==1
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    // YW - delete the memory through the function pointer
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// YW - circular list remove an element
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// YW - always insert the new element prev to the dummy head of the circular list, no matter in_use_ or lru_
// because only lru_ is obey the lru order, in_use_ is random order
// Insertion for the circular list, always insert to the position of the dummy_head->prev
// So the dummy_head->next should be the oldest entry to be erased
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// YW - Release a mapping returned by a previous Lookup(), not erase
// since when lookup we did a ref, in order to keep the original cache we unref to release it
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);
  
  // YW - minus 1 because minus the sizeof char key_data[1]
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    // YW - every insert first in in_use_ list then move to lru_ if necessary
    LRU_Append(&in_use_, e);
    usage_ += charge;
    // YW - release the old handle if the insert is a replacement
    FinishErase(table_.Insert(e));
  } // else don't cache.  (Tests use capacity_==0 to turn off caching.)

  // YW - delete lru back when exceed capacity
  // Notice lru_ is only set to use if it's size is not 0, so we need to manually do a release so that the lru_ list will be used
  // otherwise all the exceed items will still write in in_use_
  while (usage_ > capacity_ && lru_.next != &lru_) {
    // YW - lru_.next is the oldest entry and prev is the newest entry
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash table.  Return whether e != NULL.  Requires mutex_ held.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != NULL;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

// YW - delete everything in the lru_
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

// YW - wrapper of the LRUcache
class ShardedLRUCache : public Cache {
 private:
  // YW - hold 16 instance of the LRUcache and will divide the whole capacity these 16 shard
  // this can kind of reduce the situation of multiple edit the same cache
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // YW - determine which LRUcache shard to go by the MSB 4 bits, hash is 32bits
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }

  void ShowTable() const {
    std::cout << "+-+-+-+-Showing Table for Cache-+-+-+-+" << std::endl;
    for(int s=0;s<kNumShards;++s){
      shard_[s].ShowTable();
    }
  }

  void ShowCacheListSize() const{
    std::cout << "+-+-+-+-Showing Cache List Size-+-+-+-+" << std::endl;
    for(int s=0;s<kNumShards;++s){
      std::cout << "Shard_: " << s << std::endl;
      std::cout << "in_use_ size: " << shard_[s].Get_in_use_size() << std::endl;
      std::cout << "lru_ size: " << shard_[s].Get_lru_size() << std::endl;
    }
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
