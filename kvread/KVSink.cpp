#include "KVSink.h"

/*KVSink::KVSink(std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num, int id)
	:Queue(cq),
	num_thread(connected_thread_num), 
	sink_id(id){
	logger_ = spdlog::get("sink_logger");
}*/

KVSink::KVSink(int id, std::shared_ptr<CircularQueue> cq, std::shared_ptr<StageTracker> tracker, std::shared_ptr<MemoryPool> mpl, int connected_file_guiders)
	:Queue(cq), 
	pl_ptr(mpl),
	sink_tracker(tracker),
	num_file_guider(connected_file_guiders),
	bulk_size(1000), 
	sink_id(id){
	logger_ = spdlog::get("sink_logger");
}

KVSink::~KVSink() {
	//if(SinkThread.joinable())
	SinkThread.join();
	std::cout<< "Sink Thread join" <<"\n";
	//_logger->info("kv read thread joins");
}

KVSink::KVSink(KVSink&& other)
	:sink_id(-1),
	num_file_guider(0),
	Queue(nullptr),
	pl_ptr(nullptr),
	SinkThread(std::move(other).SinkThread),
	bulk_size(5000),
	sink_tracker(nullptr){

	logger_ = spdlog::get("sink_logger");
    std::cout << "kvsink move ctor" << "\n";
    *this = std::move(other);
}

KVSink& KVSink::operator= (KVSink&& other){
	std::cout << "kvsink move assign" << "\n";
    if(this!=&other){ // prevent self-move
    	sink_id = other.sink_id;
    	num_file_guider = other.num_file_guider;
    	Queue = std::move(other.Queue);
    	SinkThread = std::move(other.SinkThread);
    	sink_tracker = std::move(other.sink_tracker);
    	pl_ptr = std::move(other.pl_ptr);
    	bulk_size = other.bulk_size;
    	
    	other.num_file_guider=0;
    	other.sink_id=-1;
    	//other.Queue = nullptr;
    	//other.SinkThread = nullptr;
    	//other.sink_tracker = nullptr;
    }
    return *this;
}

void KVSink::startSink(){
	std::cout<< "Sink Thread start" <<"\n";
	SinkThread = std::thread(&KVSink::sinkKV, this);
	//SinkThread = std::thread(&KVSink::multiSink, this);
}

void KVSink::startMultiSink(){
	std::cout<< "Sink Thread start" <<"\n";
	SinkThread = std::thread(&KVSink::multiSink, this);
	//SinkThread = std::thread(&KVSink::bulkSinkKV, this);	
}

/*void KVSink::startBulkSink(){
	std::cout<< "Sink Thread start" <<"\n";
	SinkThread = std::thread(&KVSink::bulkSinkKV, this);	
}*/

void KVSink::threadJoin(){
	if(SinkThread.joinable())
		SinkThread.join();
}

void KVSink::multiSink(){
	std::cout << "sink id:" <<  +sink_id << "\n";
    uint32_t loop_counter=0;
    int loop=0;
    auto wqueue = Queue.get();
    auto mpool = pl_ptr.get();
    auto tracker =sink_tracker.get();
    uint64_t last_size = wqueue->getSize();
    struct timespec ts1, ts2, ts3;
    uint64_t sink_start, sink_end;

    while(true){
    	KVTuple kvr;
    	sink_start = KVSink::getNanoSecond(ts1);
        bool goodpop= wqueue->spin_pop(kvr);
		//logger_->info("{0:d} {1:d}", sink_id, wqueue->getSize());
        //loop++;
		if(goodpop){
			//mpool->releaseMemBlock();
			sink_end = KVSink::getNanoSecond(ts2);
			//if(loop%100000)
				//std::cout << "loop last counter" <<  tracker->getReadCount() << "\n";        	
				//std::cout << "last counter"<< tracker->getReadCount() << ", queue size" << wqueue->getSize() << "\n";
			//std::cout << "sink id:"<< +sink_id << "bulk size" << kv_list.size() << "\n";
			logger_->info("{0:d}", sink_end-sink_start);
			loop_counter++;
		}

		// This line is for 0 guider case
		if(tracker->getReadCount() == num_file_guider && wqueue->isEmpty()){ 
		//if(tracker->getGuideCount() == num_file_guider && wqueue->isEmpty() ){ //wqueue->getSize()== 0){ 
			std::cout << "sink id:"<< +sink_id << ", queue size" << wqueue->getSize() << "\n";
			//logger_->info("sink_end {0:d} {1:d} {2:d}", sink_id, wqueue->getSize(), loop_counter);
			break;
		}
        
    }
    //logger_->info("sink_count {0:d}", loop_counter);
	//std::cout << "sink id:"<< +sink_id << " End last counter:" <<  counter->getGuideCount() << "\n";
    std::cout << "sink id:"<< +sink_id << " valid loop:" <<  +loop_counter << "\n";
    //std::cout << "sink id:"<< +sink_id << " all loop:" <<  +loop << "\n";
    std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";
}

/*void KVSink::bulkSinkKV(){
	std::cout << "sink id:" <<  +sink_id << "\n";
	KVTuple kvr;
    uint32_t loop_counter=0;
    int loop=0;
    auto wqueue = Queue.get();
    auto tracker =sink_tracker.get();
    uint64_t last_size = wqueue->getSize();
    struct timespec ts1, ts2, ts3;
    uint64_t sink_start, sink_end;

    while(true){
    	std::vector<KVTuple> kv_list;
    	sink_start = KVSink::getNanoSecond(ts1);
        bool goodpop= wqueue->bulk_pop(kv_list, bulk_size);

		//logger_->info("{0:d} {1:d}", sink_id, wqueue->getSize());
        //loop++;
		if(goodpop){
			sink_end = KVSink::getNanoSecond(ts2);
			//std::cout << "sink id:"<< +sink_id << "loop last counter" <<  tracker->getGuideCount() << "\n";        	
			//std::cout << "sink id:"<< +sink_id << "queue size" << wqueue->getSize() << "\n";
			//std::cout << "sink id:"<< +sink_id << "bulk size" << kv_list.size() << "\n";
			logger_->info("{0:d}", sink_end-sink_start);
			loop_counter=loop_counter+kv_list.size();
		}

		// This line is for 0 guider case
		//if(tracker->getReadCount() == num_file_guider && wqueue->isEmpty() ){ //wqueue->getSize()== 0){ 
		if(tracker->getGuideCount() == num_file_guider && wqueue->isEmpty() ){ //wqueue->getSize()== 0){ 
			std::cout << "sink id:"<< +sink_id << ", queue size" << wqueue->getSize() << "\n";
			//logger_->info("sink_end {0:d} {1:d} {2:d}", sink_id, wqueue->getSize(), loop_counter);
			break;
		}
        
    }
    //logger_->info("sink_count {0:d}", loop_counter);
	//std::cout << "sink id:"<< +sink_id << " End last counter:" <<  counter->getGuideCount() << "\n";
    std::cout << "sink id:"<< +sink_id << " valid loop:" <<  +loop_counter << "\n";
    //std::cout << "sink id:"<< +sink_id << " all loop:" <<  +loop << "\n";
    std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";	


}*/

void KVSink::sinkKV(){
}
/*	struct timespec ts1,ts2;
    uint64_t q_start, q_end;
	int isLastCounter = 0;
	uint32_t loop_counter=0;
	size_t record_size=100;
	auto qptr= Queue.get();
	auto counter =file_counter.get();
	int num_thread = num_file_guider;

	while(isLastCounter < num_thread){   	
		KVTuple kvr;
		q_start = KVSink::getNanoSecond(ts1);  
		qptr->spin_pop(kvr);
		//if(!goodpop)
		//continue;
		//qptr->pop(kvr);
		if(kvr.checkLast()){ //we cannot find kvr.isLast
			isLastCounter++;
		}
		q_end = KVSink::getNanoSecond(ts2);
		//logger_->info("pop_time {0:d}", q_end-q_start);
		loop_counter++;
		//std::cout << "loop counter:" <<  +loop_counter << "\n";
	}
	std::cout << "sink id:"<< +sink_id << "last counter:" <<  counter->getGuideCount() << "\n";
	std::cout << "sink last counter:" <<  +isLastCounter << "\n";
	std::cout << "queue is empty:"<< qptr->isEmpty() <<"\n";
	std::cout << "sink loop counter:" <<  +loop_counter << "\n";
}*/

