//
// Created by zzyyyww on 2021/9/4.
//
#include <unistd.h>
#include "pmem_unordered_index_cceh.h"

namespace open_hikv::pmem {

const size_t initialSize = 1024 * 16;

inline uint64_t DecodeSize(const char* raw) {
  if (raw == nullptr) {
    return 0;
  }
  uint64_t size;
  memcpy(&size, global_log_->raw(), sizeof(uint64_t));
  return size;
}

UnorderedIndexCCEH::UnorderedIndexCCEH(const std::string &log_path,
                                       size_t log_size,
                                       const std::string &pool_path,
                                       size_t pool_size)
    : log_path_(std::move(log_path)),
      pool_path_(std::move(pool_path)){
  global_log_ = new pm::LogStore(log_path, log_size);
  log_ = global_log_;

  if (access(pool_path_.c_str(), 0) != 0) {
    pop_ = pmemobj_create(pool_path_.c_str(), "CCEH", pool_size, 0666);
    if (!pop_) {
      perror("pmemoj_create");
      exit(1);
    }
    cceh_ = POBJ_ROOT(pop_, CCEH);
    D_RW(cceh_)->initCCEH(pop_, initialSize);
  } else {
    pop_ = pmemobj_open(pool_path_.c_str(), "CCEH");
    if (pop_ == NULL) {
      perror("pmemobj_open");
      exit(1);
    }
    cceh_ = POBJ_ROOT(pop_, CCEH);
    if (D_RO(cceh_)->crashed) {
      D_RW(cceh_)->Recovery(pop_);
    }
  }
}

ErrorCode UnorderedIndexCCEH::Set(const Slice &k, const Slice &v, uint64_t *offset) {
  std::string whole_key = pm::GenerateRawEntry(k.ToString());
  std::string whole_value = pm::GenerateRawEntry(v.ToString());

  pm::PmAddr addr = log_->Alloc(whole_key.size() + whole_value.size());
  log_->Append(addr, whole_key + whole_value);

  D_RW(cceh_)->Insert(pop_, addr, (char*)(addr + whole_key.size()));
  return ErrorCode::kOk;
}

ErrorCode UnorderedIndexCCEH::Get(const Slice &k, Slice *v) {
  std::string whole_key = pm::GenerateRawEntry(k.ToString());
  std::string value;
  char* lookup = new char[whole_key.size()];
  memcpy(lookup, whole_key.c_str(), whole_key.size());
  Key_t lookup_key = (Key_t)(lookup);
  auto ret = D_RW(cceh_)->Get(lookup_key);
  if (ret) {
    uint64_t size = DecodeSize(ret);
    char* res = new char[size];
    memcpy(res, global_log_->raw() + (uint64_t)ret, size);
    *v = Slice(res, size);
    delete[] lookup;
    return ErrorCode::kOk;
  } else {
    return ErrorCode::kNotFound;
  }
}

ErrorCode UnorderedIndexCCEH::Del(const Slice &k) {
  std::string whole_key = pm::GenerateRawEntry(k.ToString());
  char* lookup = new char[whole_key.size()];
  memcpy(lookup, whole_key.c_str(), whole_key.size());
  Key_t lookup_key = (Key_t)(lookup);
  D_RW(cceh_)->Delete(pop_, lookup_key);
  delete[] lookup;
  return ErrorCode::kOk;
}

}  // namespace open_hikv::pmem