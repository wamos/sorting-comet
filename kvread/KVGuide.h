#ifndef KV_GUIDE_H
#define KV_GUIDE_H

#include "ConcurrentQueue.h"
#include "KVTuple.h"
#include "StageTracker.h"
#include <iostream>
#include <unordered_map>
#include <vector>

class KVGuide {
public:
    KVGuide(int id, std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num, std::shared_ptr<StageTracker> tracker);
    ~KVGuide();
    KVGuide(KVGuide&& other);
    KVGuide& operator= (KVGuide&& other);
    KVGuide(const KVGuide& other) = delete;
    KVGuide& operator= (const KVGuide& other)=delete;

	void addWriteQueue(std::shared_ptr<ConcurrentQueue> queue);
	void genSplitPoints(int num_split);
	void startGuide();
    void threadJoin();

    void setBulkSize(uint32_t bsize){
        bulk_size=bsize;
    }

    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

private: 
    void pushKV();
    void bulkKV();

    inline int getSplit(uint8_t key){
        return HashTable[key];
    }
    /*
    for(int index = 0; index < splitPoints.size(); index++){
        if(key < splitPoints[index]){
            //std::cout << "key:" << +key << ", index:" << index <<"\n";
            return index;
        }
        else{
            continue;
        }
    }
    */

    int gid;
    int num_thread;
    int key_range=256;
    int header_offset=10;
    uint32_t bulk_size=100;

    std::shared_ptr<spdlog::logger> logger_;
    std::vector<uint8_t> splitPoints;
    std::shared_ptr<ConcurrentQueue> GuideQueue;
    std::shared_ptr<StageTracker> guide_tracker;
    std::vector<std::shared_ptr<ConcurrentQueue>> WQueueList;
    std::unordered_map<int, std::vector<KVTuple> > BufferTable;
    std::unordered_map<uint8_t, int> HashTable;
    //MAYBE TODO: make astd::vector to std::array for a fixed length-> bulk_size 
    std::thread GuideThread;
};

#endif