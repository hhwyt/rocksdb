// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// Licensed under both GPLv2 and Apache 2.0 (found in LICENSE files).

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {

// This test attempts to reproduce the data race where the memtable publishes its
// DynamicBloom pointer without acquire/release semantics. On weak memory
// models, readers can observe a partially constructed DynamicBloom and crash in
// AddConcurrently(). ThreadSanitizer should detect this race condition.
TEST(MemTableBloomConcurrentTest, ConcurrentInitAddStress) {
  const int rounds = 100;
  const int num_threads = 64;
  const int per_thread_inserts = 4;

  for (int r = 0; r < rounds; ++r) {
    Options options;
    options.memtable_factory = std::make_shared<SkipListFactory>();
    options.allow_concurrent_memtable_write = true;
    options.memtable_whole_key_filtering = true;
    options.memtable_prefix_bloom_size_ratio = 0.1;
    options.write_buffer_size = 1 << 20;

    InternalKeyComparator cmp(BytewiseComparator());
    ImmutableOptions ioptions(options);
    MutableCFOptions moptions(options);
    WriteBufferManager wb(options.db_write_buffer_size);
    std::unique_ptr<MemTable> mem(new MemTable(
        cmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0));

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([&, i]() {
        ready.fetch_add(1, std::memory_order_relaxed);
        while (!go.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        SequenceNumber seq = static_cast<SequenceNumber>(i + 1);
        std::string key = "k" + std::to_string(i);
        std::string value = "v";
        MemTablePostProcessInfo post_process_info;
        for (int attempt = 0; attempt < per_thread_inserts; ++attempt) {
          Status s = mem->Add(
              seq + static_cast<SequenceNumber>(attempt), kTypeValue,
              key + ":" + std::to_string(attempt), value,
              /*kv_prot_info=*/nullptr,
              /*allow_concurrent=*/true, &post_process_info);
          (void)s;
        }
      });
    }

    // Wait for all threads to be ready
    while (ready.load(std::memory_order_acquire) < num_threads) {
      std::this_thread::yield();
    }

    // Trigger bloom filter initialization by adding a key (this will cause
    // GetBloomFilter() to be called and initialize the bloom filter)
    std::string init_key = "init_key";
    std::string init_value = "init_value";
    void* hint = nullptr;
    Status init_status =
        mem->Add(static_cast<SequenceNumber>(1000000 + r), kTypeValue,
                 init_key, init_value, /*kv_prot_info=*/nullptr,
                 /*allow_concurrent=*/false, /*post_process_info=*/nullptr,
                 &hint);
    (void)init_status;

    // Release all threads to start concurrent Add() calls
    go.store(true, std::memory_order_release);

    for (auto& t : threads) {
      t.join();
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


