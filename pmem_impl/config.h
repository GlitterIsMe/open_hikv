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

  std::string log_path_{"/mnt/pmem/hikv/log"};
  uint64_t log_size_{60UL * 1024 * 1024 * 1024};
  std::string cceh_path_{"mnt/pmem/hikv/cceh"};
  uint64_t cceh_size_{40UL * 1024 * 1024 * 1024};
};
}