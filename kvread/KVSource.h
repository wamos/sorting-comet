#ifndef KVSRC_H
#define KVSRC_H    

#include <string>
#include <cstdint>
#include <random>
#include <time.h>
#include "KVTuple.h"
//#include "ConcurrentQueue.h"
#include "MemoryPool.h"
#include "CircularQueue.h"
#include "spdlog/spdlog.h"
#include "StageTracker.h" 

class KVSource {
public:
    KVSource(int id, uint32_t ksize, uint32_t vsize, std::shared_ptr<CircularQueue> cq, std::shared_ptr<StageTracker> tracker, std::shared_ptr<MemoryPool> mpl);
    ~KVSource();
    KVSource(KVSource&& other);
    KVSource& operator= (KVSource&& other);
    KVSource(const KVSource& other) = delete;
    KVSource& operator= (const KVSource& other)=delete;

    //void preAllocMem(uint32_t alloc_size);
    void setHeaderSize(uint32_t hsize);
    void setBulkSize(uint32_t bsize);
    void startSource();
    void stopSource();

    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

private:
    void genKVTuple();
    void bulkKVTuple();
    std::shared_ptr<CircularQueue> Queue;
    std::shared_ptr<StageTracker> source_tracker;
    std::shared_ptr<MemoryPool> pl_ptr;
    std::shared_ptr<spdlog::logger> logger_;
    std::thread SourceThread;
    bool source_stop_flag = false;
    uint64_t counter=0;
    int source_id;
    uint32_t bulk_size=100;
    uint32_t key_size=10;
    uint32_t val_size=90;
    uint32_t header_size=10;
};

#endif
