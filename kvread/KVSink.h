#ifndef KVSINK_H
#define KVSINK_H

#include <thread>
#include <iostream>
#include <memory>
//#include "ConcurrentQueue.h"
#include "MemoryPool.h"
#include "KVTuple.h"
#include "CircularQueue.h"
#include "spdlog/spdlog.h"
#include "StageTracker.h"

class KVSink {
public:
	KVSink(int id, std::shared_ptr<CircularQueue> cq, std::shared_ptr<StageTracker> tracker, std::shared_ptr<MemoryPool> mpl, int connected_thread_num);
	~KVSink();
	KVSink(KVSink&& other);
    KVSink& operator= (KVSink&& other);
    KVSink(const KVSink& other) = delete;
    KVSink& operator= (const KVSink& other)=delete;

	void startSink();
	void startMultiSink();
	void startBulkSink();
	void threadJoin();

	void setBulkSize(size_t bsize){
    	bulk_size=bsize;
    }

	inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

private:
	void sinkKV();
	void multiSink();
	void bulkSinkKV();

	std::shared_ptr<spdlog::logger> logger_;
	//std::shared_ptr<ConcurrentQueue> Queue;
	std::shared_ptr<MemoryPool> pl_ptr;
	std::shared_ptr<CircularQueue> Queue;
	std::shared_ptr<StageTracker> sink_tracker;
	std::thread SinkThread;
	int sink_id;
	int num_file_guider;
	size_t bulk_size=100;
};

#endif
