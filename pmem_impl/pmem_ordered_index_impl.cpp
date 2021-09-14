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
  struct bplus_leaf* init_leaf = leaf;
    printf("%lld \n", init_leaf->entries);
    if (pos >= leaf->entries) {
        return ErrorCode::kOk;
    }
  while (leaf != nullptr) {
    int key_size;
    int value_size;
    char* raw_value = reinterpret_cast<char*>(leaf->data[pos]);
    memcpy(&key_size, raw_value, 4);
    memcpy(&value_size, raw_value + 4, 4);
    if (!func(Slice(raw_value + 4 * 2, key_size), Slice(raw_value + 4 * 2 + key_size, value_size))) {
      break;
    }
    //printf("%s-%s\n", Slice(raw_value + 4 * 2, key_size).ToString().c_str(), Slice(raw_value + 4 * 2 + key_size, value_size).ToString().c_str());
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
