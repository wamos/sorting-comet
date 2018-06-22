#ifndef KVSINK_H
#define KVSINK_H

#include <thread>
#include <iostream>
#include <memory>
#include "ConcurrentQueue.h"
#include "spdlog/spdlog.h"

class KVSink {
public:
	KVSink(std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num, int id);
	~KVSink();
	//TODO: move assign operator and move constructor
	// Copy assignment and copy constructor would be deleted
	void startSink();
	void startMultiSink();
	void threadJoin();

private:
	void sinkKV();
	void multiSinks();
	std::shared_ptr<ConcurrentQueue> Queue;
	std::thread SinkThread;
	int sink_id;
	int num_thread;
};

#endif
