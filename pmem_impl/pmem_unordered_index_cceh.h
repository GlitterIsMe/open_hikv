//
// Created by zzyyyww on 2021/9/4.
//
#ifndef OPEN_HIKV_PMEM_UNORDERED_INDEX_CCEH_H
#define OPEN_HIKV_PMEM_UNORDERED_INDEX_CCEH_H

#include <libpmemobj.h>

#include "store.h"
#include "unordered_index.h"

#include "src/CCEH.h"
#include "src/global_log.h"

namespace open_hikv::pmem {

class UnorderedIndexCCEH : public UnorderedIndex {
 public:
  UnorderedIndexCCEH(const std::string& log_path, size_t log_size,
                     const std::string& pool_path, size_t pool_size);

  ~UnorderedIndexCCEH() override =default;

  ErrorCode Get(const Slice& k, Slice* v) const override;

  ErrorCode Set(const Slice& k, const Slice& v, uint64_t* offset) override;

  ErrorCode Del(const Slice& k) override;

 private:
  const std::string log_path_;
  const std::string pool_path_;

  PMEMobjpool *pop_;
  TOID(CCEH) cceh_;
  pm::LogStore* log_;
};

}

#endif  // OPEN_HIKV_PMEM_UNORDERED_INDEX_CCEH_H
