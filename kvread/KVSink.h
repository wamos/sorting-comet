#ifndef KVSINK_H
#define KVSINK_H

#include <thread>
#include <iostream>
#include <memory>
#include "ConcurrentQueue.h"
#include "spdlog/spdlog.h"

class KVSink {
public:
	KVSink(std::shared_ptr<ConcurrentQueue> cq)
	:Queue(cq){};

	~KVSink() {
		SinkThread.join();
		std::cout<< "Sink Thread join" <<"\n";
		//_logger->info("kv read thread joins");
	}

	void startSink(){
		std::cout<< "Sink Thread start" <<"\n";
		 SinkThread = std::thread(&KVSink::sinkKV, this);
	}

	void threadJoin(){
		if(SinkThread.joinable())
			SinkThread.join();
	}

private:
	void sinkKV(){
		auto qptr= Queue.get();
		while(!qptr->isEmpty()){   	
			qptr->pop();
		}
	}
	std::shared_ptr<ConcurrentQueue> Queue;
	std::thread SinkThread;
};

#endif
