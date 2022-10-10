//
// Created by zzyyyww on 2021/9/4.
//
#include <unistd.h>
#include "pmem_unordered_index_cceh.h"
#include "../../latency_counter.h"

//#define COUNT_FG

namespace open_hikv::pmem {

    const size_t initialSize = 1024 * 16;

    inline uint64_t DecodeSize(const char *raw) {
        if (raw == nullptr) {
            return 0;
        }
        int size;
        memcpy(&size, raw, pm::ENCODE_SIZE);
        return (uint64_t) size;
    }

    UnorderedIndexCCEH::UnorderedIndexCCEH(const std::string &log_path,
                                           size_t log_size,
                                           const std::string &pool_path,
                                           size_t pool_size)
            : log_path_(log_path + "/pool"),
              pool_path_(pool_path + "/cceh") {
        global_log_ = new pm::LogStore(log_path_, log_size);
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
#ifdef COUNT_FG
        START
#endif
        pm::PmAddr addr = log_->Alloc(whole_key.size() + whole_value.size());
        log_->Append(addr, whole_key + whole_value);
        *offset = addr;
#ifdef COUNT_FG
        LOG_END
#endif
        //{
        //    Key_t lookup = (Key_t)(global_log_->raw() + addr);
        //    D_RW(cceh_)->Delete(pop_, lookup);
        //}
#ifdef COUNT_FG
        START
#endif
                D_RW(cceh_)->Insert(pop_, addr, (char *) (addr));
#ifdef COUNT_FG
        INDEX_END
#endif
        //printf("insert ret %lu\n", (uint64_t)addr);
        //{
        //    Key_t lookup = (Key_t)(global_log_->raw() + addr);
        //    auto res = D_RW(cceh_)->Get(lookup);
        //    printf("insert check ret %lu\n", (uint64_t)res);
        //assert((uint64_t)res == addr + whole_key.size());
        //}
        return ErrorCode::kOk;
    }

    ErrorCode UnorderedIndexCCEH::Get(const Slice &k, Slice *v) {
        std::string whole_key = pm::GenerateRawEntry(k.ToString());
        std::string value;
#ifdef COUNT_FG
        START
#endif
        char *lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        Key_t lookup_key = (Key_t) (lookup);
        auto ret = D_RW(cceh_)->Get(lookup_key);
#ifdef COUNT_FG
        INDEX_END
#endif
        //printf("get ret %lu\n", (uint64_t)ret);
        if (ret) {
#ifdef COUNT_FG
            START
#endif
            uint64_t key_size = DecodeSize(global_log_->raw() + (uint64_t) ret);
            //printf("get key size %lu\n", key_size);
            uint64_t value_size = DecodeSize(global_log_->raw() + (uint64_t) ret + key_size + pm::ENCODE_SIZE);
            //printf("get key size %lu\n", value_size);
            char *res = new char[value_size];
            memcpy(res, global_log_->raw() + (uint64_t) ret + 2 * pm::ENCODE_SIZE + key_size, value_size);
            *v = Slice(res, value_size);
#ifdef COUNT_FG
            LOG_END
#endif
            delete[] lookup;
            return ErrorCode::kOk;
        } else {
            delete[] lookup;
            return ErrorCode::kNotFound;
        }
    }

    ErrorCode UnorderedIndexCCEH::Del(const Slice &k) {
#ifdef COUNT_FG
        START
#endif
        std::string whole_key = pm::GenerateRawEntry(k.ToString());
        char *lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        Key_t lookup_key = (Key_t) (lookup);
                D_RW(cceh_)->Delete(pop_, lookup_key);
#ifdef COUNT_FG
        INDEX_END
#endif
        delete[] lookup;
        return ErrorCode::kOk;
    }

}  // namespace open_hikv::pmem