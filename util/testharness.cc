// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/testharness.h"

#include <string>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace leveldb {
namespace test {

namespace {
struct Test {
  const char* base;
  const char* name;
  void (*func)();
};
// YW - std::vector pointer, pointer is static memory but the content is on TODO: heap?
// and when program died at end the static vector pointer is deconstructed, the content will also automatically released
std::vector<Test>* tests;
}

// YW - register a test func into the vector
bool RegisterTest(const char* base, const char* name, void (*func)()) {
  if (tests == NULL) {
    tests = new std::vector<Test>;
  }
  Test t;
  t.base = base;
  t.name = name;
  t.func = func;
  tests->push_back(t);
  return true;
}

int RunAllTests() {
  const char* matcher = getenv("LEVELDB_TESTS");

  int num = 0;
  if (tests != NULL) {
    for (size_t i = 0; i < tests->size(); i++) {
      const Test& t = (*tests)[i];
      if (matcher != NULL) {
        std::string name = t.base;
        name.push_back('.');
        name.append(t.name);
        // YW - if there is no match between matcher and name, will continue
        if (strstr(name.c_str(), matcher) == NULL) {
          continue;
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      // YW - exec the _RunIt() and the actual all _Run()
      (*t.func)();
      ++num;
    }
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
  return 0;
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  ASSERT_TRUE(s.ok()) << s.ToString();
  return dir;
}

// YW - get the TEST_RANDOM_SEED env variable, if not exist then return 301
int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != NULL ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

}  // namespace test
}  // namespace leveldb
