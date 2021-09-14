#include "pmem_ordered_index_impl.h"

namespace open_hikv::pmem {

OrderedIndexImpl::OrderedIndexImpl(Store* store) : store_(store) {
  btree_ = bplus_tree_init(16, 16);
}

OrderedIndexImpl::~OrderedIndexImpl() { bplus_tree_deinit(btree_); }

// it leaks!
ErrorCode OrderedIndexImpl::Set(const Slice& k, uint64_t offset) {
  char* ptr = (char*)malloc(4 + k.size());
  int len = k.size();
  memcpy(ptr, &len, 4);
  memcpy(ptr + 4, k.data(), k.size());
  bplus_tree_put(btree_, reinterpret_cast<uintptr_t>(ptr), offset, 1);
  return ErrorCode::kOk;
}

ErrorCode OrderedIndexImpl::Del(const Slice& k) {
  char* ptr = (char*)malloc(4 + k.size());
  int len = k.size();
  memcpy(ptr, &len, 4);
  memcpy(ptr + 4, k.data(), k.size());
  bplus_tree_put(btree_, reinterpret_cast<uintptr_t>(ptr), 0, 1);
  return ErrorCode::kOk;
}

ErrorCode OrderedIndexImpl::Scan(
    const Slice& k,
    const std::function<bool(const Slice&, const Slice&)>& func) const {
  // 1. btree_get k , return the data node that containes the smallest key that larger than k;
  char* ptr = (char*)malloc(4 + k.size());
  int len = k.size();
  memcpy(ptr, &len, 4);
  memcpy(ptr + 4, k.data(), k.size());
  int start_pos = 0;
  struct bplus_leaf* leaf = bplus_tree_get_range_start(btree_, reinterpret_cast<uintptr_t>(ptr), &start_pos);
  int pos = start_pos;
  while (leaf != nullptr) {
    int key_size;
    char* raw = reinterpret_cast<char*>(leaf->key[pos]);
    memcpy(&key_size, raw, 4);
    if (!func(Slice(raw+4, key_size), Slice(""))) {
      break;
    }
    printf("%s\n", Slice(raw+4, key_size).ToString().c_str());
    ++pos;
    if (pos >= leaf->entries) {
      if (list_is_last(&leaf->link, &btree_->list[0])) {
        return ErrorCode::kOk;
      }
      leaf = list_next_entry(leaf, link);
      pos = 0;
    }
  }
  return ErrorCode::kOk;
}

}  // namespace open_hikv::pmem
