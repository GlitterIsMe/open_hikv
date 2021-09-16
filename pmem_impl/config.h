#pragma once

namespace open_hikv::pmem {

enum {
  kMessageQueueShardNum = 32,
};

}

namespace open_hikv {
struct HiKVConfig{
  std::string pm_path_{"/mnt/pmem"};
  uint64_t store_size {60 * 1024UL * 1024UL * 1024UL};
  uint64_t shard_size {625000 * 16 * 2};
  uint64_t shard_num {256};
  uint64_t message_queue_shard_num {32};
};
}