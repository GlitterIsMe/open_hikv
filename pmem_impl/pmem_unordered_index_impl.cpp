#include "pmem_unordered_index_impl.h"

#include <iostream>
#include <mutex>
#include <thread>

#include "pmem_op.h"

#define PERM_rw_r__r__ 0644
#define PERM_rwxr_xr_x 0755

namespace open_hikv::pmem {
using namespace terark;

struct HashEntry {
  uint64_t signature;
  uint64_t offset;
};

// it leaks, too! but fine for benchmark
UnorderedIndexImpl::UnorderedIndexImpl(Store* store, const std::string& path,
                                       size_t shard_size, size_t shard_num)
    : store_(store),
      path_(path),
      kShardSize(shard_size),
      kEntryNum(kShardSize / sizeof(HashEntry)),
      shards_(shard_num) {
  std::vector<std::thread> jobs;
  for (size_t i = 0; i < shard_num; ++i) {
    const auto& filename =
        path_ + "/" + std::to_string(i) + ".open_hikv_index_pool";
    size_t mapped_len;
    int is_pmem;
    auto addr = reinterpret_cast<char*>(
        pmem_map_file(filename.c_str(), shard_size + 4096, PMEM_FILE_CREATE,
                      PERM_rw_r__r__, &mapped_len, &is_pmem));

    auto& shard = shards_[i];
    shard.data = addr;
    shard.max_probe_len.store(0);
    shard.entry_nums.store(0);
    // don't support recovery yet
    jobs.emplace_back(
        [addr, shard_size]() { memset(addr, 0, shard_size + 4096); });
  }

  for (auto& job : jobs) {
    job.join();
  }
}

inline static uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w;
    memcpy(&w, data, sizeof(w));
    data += sizeof(w);
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += static_cast<uint8_t>(data[2]) << 16;
    case 2:
      h += static_cast<uint8_t>(data[1]) << 8;
    case 1:
      h += static_cast<uint8_t>(data[0]);
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

ErrorCode UnorderedIndexImpl::Get(const Slice& k, Slice* v){
  auto hash = Hash(k.data(), k.size(), 0);
  auto& shard = shards_[hash % shards_.size()];

  uint64_t slot = hash % kEntryNum;
  size_t probe_len = shard.max_probe_len;
  HashEntry* entry;
  for (size_t i = 0; i < probe_len; ++i, slot = (slot + 1) % kEntryNum) {
      entry = &reinterpret_cast<HashEntry*>(shard.data)[slot];
    //__int128_t old_val{0};
    //__int128_t new_val{0};
    //__sync_bool_compare_and_swap(reinterpret_cast<__int128_t*>(entry), old_val, new_val);
    //HashEntry entry_val;
    //memcpy(&entry_val, &old_val, sizeof(entry_val));

    if (entry->offset == 0 && entry->signature == 0) {
      break;
    }

    if (entry->signature == hash) {
      Slice exist_k;
      Slice exist_v;
      auto ec = store_->Get(entry->offset, &exist_k, &exist_v);
      if (ec != ErrorCode::kOk) {
        return ec;
      }

      if (exist_k == k) {
        *v = exist_v;
        return ErrorCode::kOk;
      }
    }
  }
  return ErrorCode::kNotFound;
}

// don't support update yet
ErrorCode UnorderedIndexImpl::Set(const Slice& k, const Slice& v,
                                  uint64_t* offset) {
  ErrorCode code = store_->Set(k, v, offset);
  if (code != ErrorCode::kOk) {
    return code;
  }

  //auto hash = Hash(k.data(), k.size(), 0) | 1;
  auto hash = Hash(k.data(), k.size(), 0);
  auto& shard = shards_[hash % shards_.size()];
  uint64_t slot = hash % kEntryNum;
  HashEntry* entry;

  size_t probe_len = 1;
  while (true) {
    entry = &reinterpret_cast<HashEntry*>(shard.data)[slot];
    if (entry->offset == 0 && entry->signature == 0) {
      __int128_t old_val{0};
      HashEntry new_entry;
      new_entry.signature = hash;
      new_entry.offset = *offset;
      __int128_t new_val;
      memcpy(&new_val, &new_entry, sizeof(new_entry));

      bool success = __sync_bool_compare_and_swap(
          reinterpret_cast<__int128_t*>(entry), old_val, new_val);
      if (success) {
        PMemPersist(entry);
        shard.entry_nums++;
        break;
      } else {
        printf("CAS failed\n");
      }
    }

    //++entry;
    slot = (slot + 1) % kEntryNum;
    //printf("probe [%lu] [%lu]\n", probe_len, slot);
    ++probe_len;
    if (probe_len > kEntryNum) {
      fprintf(stderr, "shard [%lu] probe [%lu] all entry [%lu] and cannot find a empty slot\n",hash % shards_.size(), probe_len, kEntryNum);
      printShardEntries();
      exit(-1);
    }
    //std::cout << "thread [" << std::this_thread::get_id() << "], probelen[" << probe_len << "] shard [" << hash % shards_.size() << "]\n";
  }

  size_t curr_max_proble_len = shard.max_probe_len.load();
  while (probe_len > curr_max_proble_len) {
    if (shard.max_probe_len.compare_exchange_strong(curr_max_proble_len,
                                                    probe_len)) {
      break;
      printf("max probe len: %lu\n", shard.max_probe_len.load(std::memory_order_release));
    }
  }
  //std::cout << "thread [" << std::this_thread::get_id() << "] end set\n";
  return ErrorCode::kOk;
}

void UnorderedIndexImpl::printShardEntries() {
  for(size_t i = 0; i < shards_.size(); i++) {
    printf("shard [%lu] has [%lu] entries\n", i, shards_[i].entry_nums.load(std::memory_order_release));
  }
}

}  // namespace open_hikv::pmem