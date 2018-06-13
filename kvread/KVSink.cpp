#include "KVSink.h"

KVSink::KVSink(std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num)
	:Queue(cq),num_thread(connected_thread_num){}

KVSink::~KVSink() {
		if(SinkThread.joinable())
			SinkThread.join();
		std::cout<< "Sink Thread join" <<"\n";
		//_logger->info("kv read thread joins");
}

void KVSink::startSink(){
		std::cout<< "Sink Thread start" <<"\n";
		 SinkThread = std::thread(&KVSink::sinkKV, this);
}

void KVSink::threadJoin(){
		if(SinkThread.joinable())
			SinkThread.join();
}

void KVSink::sinkKV(){
	KVTuple kvr;
	int isLastCounter = 0;
	uint32_t loop_counter=0;
	auto qptr= Queue.get();
	while(isLastCounter < num_thread){   	
		qptr->pop(kvr);
		if(kvr.checkLast()){
			//std::cout << "kvr tag" << kvr.getTag() << "\n";
			isLastCounter++;
		}
		loop_counter++;
		//std::cout << "loop counter:" <<  +loop_counter << "\n";
	}
	std::cout << "last counter:" <<  +isLastCounter << "\n";
	std::cout << "queue is empty:"<< qptr->isEmpty() <<"\n";
	std::cout << "sink counter:" <<  +loop_counter << "\n";
}

