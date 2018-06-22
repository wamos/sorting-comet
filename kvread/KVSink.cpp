#include "KVSink.h"

KVSink::KVSink(std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num, int id)
	:Queue(cq),num_thread(connected_thread_num), sink_id(id){}

KVSink::~KVSink() {
		if(SinkThread.joinable())
			SinkThread.join();
		std::cout<< "Sink Thread join" <<"\n";
		//_logger->info("kv read thread joins");
}

void KVSink::startSink(){
		std::cout<< "Sink Thread start" <<"\n";
		SinkThread = std::thread(&KVSink::sinkKV, this);
		//SinkThread = std::thread(&KVSink::multiSinks, this);
}

void KVSink::startMultiSink(){
	std::cout<< "Sink Thread start" <<"\n";
	SinkThread = std::thread(&KVSink::multiSinks, this);
}

void KVSink::threadJoin(){
	if(SinkThread.joinable())
		SinkThread.join();
}

void KVSink::multiSinks(){ //TODO: this cannot end!?!
	std::cout << "sink id:" <<  +sink_id << "\n";
	KVTuple kvr;
    int isLastCounter = 0;
    uint32_t loop_counter=0;
    auto wqueue = Queue.get();
    while(true){    
        wqueue->pop(kvr);
        if(kvr.checkLast()){
            // if kvr directly comes from queue then //kv_io->writeTuple(kvr,key_index);
            // else don't read it. i.e. Drop the last one kvr if it comes from Guiders
            isLastCounter++;
            break;
        }
        /*else{
            kv_io->writeTuple(kvr,key_index);
        }*/
        loop_counter++;
        //std::cout << "sink id:"<< +sink_id <<",loop counter:" <<  +loop_counter << "\n";
    }
    std::cout << "sink id:"<< +sink_id << "sink last counter:" <<  +isLastCounter << "\n";
    //std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";
    std::cout << "sink id:"<< +sink_id << "sink loop counter:" <<  +loop_counter << "\n";
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
	std::cout << "sink last counter:" <<  +isLastCounter << "\n";
	std::cout << "queue is empty:"<< qptr->isEmpty() <<"\n";
	std::cout << "sink loop counter:" <<  +loop_counter << "\n";
}

