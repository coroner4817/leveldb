// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/port_posix.h"

#include <cstdlib>
#include <stdio.h>
#include <string.h>

namespace leveldb {
namespace port {

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { 
  PthreadCall("lock", pthread_mutex_lock(&mu_)); 
#ifndef NDEBUG
  self = pthread_self();
#endif // #ifndef NDEBUG
}

void Mutex::Unlock() { 
  PthreadCall("unlock", pthread_mutex_unlock(&mu_)); 
#ifndef NDEBUG
  self = NULL;
#endif // #ifndef NDEBUG
}

void Mutex::PrintHolder(){
  uint64_t tid = 0;
  memcpy(&tid, &self, sizeof(tid)>sizeof(self)?sizeof(self):sizeof(tid));
  fprintf(stdout, "DEBUG::Mutex is hold by tid: 0x%04x\n", tid);
  unsigned char *ptc = (unsigned char*)(void*)(&self);
  fprintf(stdout, "DEBUG::Mutex is hold by tid: 0x");
  for (size_t i=0; i<sizeof(self); i++) {
    fprintf(stdout, "%02x", (unsigned)(ptc[i]));
  }
  fprintf(stdout, "\n");
}

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
    PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}
// YW - notify_one
void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

}  // namespace port
}  // namespace leveldb
