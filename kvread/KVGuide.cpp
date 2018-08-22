#include "KVGuide.h"

KVGuide::KVGuide(int id, std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num, std::shared_ptr<StageTracker> tracker)
    : gid(id)
    ,GuideQueue(cq)
    ,num_thread(connected_thread_num)
    ,guide_tracker(tracker)
    ,key_range(256)
    ,header_offset(10)
    ,bulk_size(1000){
    logger_ = spdlog::get("guide_logger");    
};

KVGuide::~KVGuide() {
    if(GuideThread.joinable())
	   GuideThread.join();
	std::cout<< "Guide Thread join" <<"\n";
	//_logger->info("kv guide thread joins");
}

KVGuide::KVGuide(KVGuide&& other)
    :gid(-1),
    GuideQueue(nullptr),
    guide_tracker(nullptr),
    GuideThread(std::move(other).GuideThread),
    splitPoints(std::move(other).splitPoints),
    WQueueList(std::move(other).WQueueList),
    num_thread(0),
    header_offset(10),
    key_range(256),
    bulk_size(0){

    logger_ = spdlog::get("guide_logger");
    std::cout << "kvguide move ctor" << "\n";
    *this = std::move(other);
}

KVGuide& KVGuide::operator= (KVGuide&& other){
    std::cout << "kvguide move assign" << "\n";
    if(this!=&other){ // prevent self-move
    //TODO fill in move statements
        GuideQueue = std::move(other.GuideQueue);
        GuideThread = std::move(other.GuideThread);
        splitPoints = std::move(other.splitPoints);
        guide_tracker = std::move( other.guide_tracker);
        //for(int i=0; i < other.WQueueList..size(); i++){
        WQueueList = std::move(other.WQueueList); // Question: recursive move of shared_ptrs on vector?
        num_thread = other.num_thread;
        key_range = other.key_range;
        header_offset = other.header_offset;
        gid=other.gid;
        bulk_size=other.bulk_size;

        //other.GuideQueue = nullptr;
        //other.GuideThread = nullptr;
        //other.guide_tracker = nullptr;
        other.splitPoints.clear();
        other.WQueueList.clear();
        other.num_thread=0;
        other.key_range=0;
        other.gid=-1;
    }
    return *this;
}

void KVGuide::addWriteQueue(std::shared_ptr<ConcurrentQueue> queue){
	WQueueList.push_back(queue);
}


void KVGuide::genSplitPoints(int num_split){
	uint8_t step = floor( (double)key_range/ (double) num_split);

    //TODO fix this loop to an unordered_map, aka hashtable
	for(uint8_t i=0; i< (uint8_t) num_split; i++){
        uint8_t num = step*(i+1)-1;
        std::cout << "step_" << +i << ":" << +num <<"\n";
		splitPoints.push_back(num);	
	}

    for(uint8_t key=0; key <= (uint8_t) key_range-1 ; key++){
        for(int index = 0; index < splitPoints.size(); index++){
            if(key < splitPoints[index]){
                //std::cout << "key:" << +key << ", index:" << index <<"\n";
                HashTable[key]=index;
            }
            else{
                continue;
            }
        }    
    }

    for(int i=0; i< num_split; i++){
       std::vector<KVTuple> vec; 
       BufferTable[i] = vec;
    }
}

void KVGuide::startGuide(){
	std::cout<< "Guide Thread start" <<"\n";
	//GuideThread = std::thread(&KVGuide::pushKV, this);
    GuideThread = std::thread(&KVGuide::bulkKV, this);
}

void KVGuide::threadJoin(){
	if(GuideThread.joinable())
		GuideThread.join();
}

void KVGuide::pushKV(){  
    KVTuple kvr;
    auto gqptr= GuideQueue.get();
    auto tracker = guide_tracker.get();
    uint32_t loop_counter=0;
    int split=0;
    bool goodadd = false;
    struct timespec ts1, ts2, ts3;
    uint64_t guide_start, guide_end;
    std::vector<ConcurrentQueue*> qlist;
    for(int i=0; i< WQueueList.size();i++){
        qlist.push_back(WQueueList[i].get());
    }

    while(true){
        guide_start = KVGuide::getNanoSecond(ts1);
        bool goodpop = gqptr->spin_pop(kvr);
        if(goodpop){
            uint8_t key_byte = kvr.getKey(0,header_offset);
            split = getSplit(key_byte);
            auto wqptr = qlist[split];
            //auto wqptr = WQueueList[split].get();
            wqptr->spin_push(kvr);
            loop_counter++;
            guide_end = KVGuide::getNanoSecond(ts2);   
            logger_->info("{0:d}", guide_end-guide_start);
        }

        if(tracker->getReadCount() == num_thread && gqptr->getSize()== 0){ 
            tracker->guide_increment();
            break;
        }             
    }

    std::cout << "FG last counter:" <<  +tracker->getReadCount() << "\n";
    //std::cout << "FG queue is empty:"<< gqptr->isEmpty() <<"\n";
    std::cout << "FG loop counter:" <<  +loop_counter << "\n";
}

void KVGuide::bulkKV(){ 
    //KVTuple kvr;
    auto gqptr= GuideQueue.get();
    auto tracker = guide_tracker.get();
    uint32_t loop_counter=0;
    //int split=0;
    bool goodadd = false;
    struct timespec ts1, ts2, ts3, ts4;
    uint64_t guide_start, guide_end, push_start, push_end;
    std::vector<uint64_t> push_ts;
    std::vector<ConcurrentQueue*> qlist;
    for(int i=0; i< WQueueList.size();i++){
        qlist.push_back(WQueueList[i].get());
    } 

    while(true){
        guide_start = KVGuide::getNanoSecond(ts1);
        std::vector<KVTuple> kv_list;
        bool goodpop = gqptr->bulk_pop(kv_list, bulk_size);
        if(goodpop && kv_list.size()== bulk_size){
            for(int i=0; i < bulk_size; i++){
                KVTuple kvr = kv_list[i];
                uint8_t key_byte = kvr.getKey(0,header_offset);
                int split = getSplit(key_byte);
                BufferTable[split].push_back(std::move(kvr));           
            }
            guide_end = KVGuide::getNanoSecond(ts2);
            guide_end = guide_end - guide_start;
            //auto wqptr = qlist[split]; /
            //auto wqptr = WQueueList[split].get();
            //wqptr->bulk_push(kv_list);
            loop_counter=loop_counter+bulk_size;
            //check the bulk of which split is fullm and then bulk push it to the queue
            for( auto iter = BufferTable.begin(); iter != BufferTable.end(); ++iter){
                push_start = KVGuide::getNanoSecond(ts3); 
                //std::cout << "kvlist:" << iter->second.size() <<"\n";
                if( iter->second.size() >= bulk_size){
                    auto key_split = iter->first;
                    auto wqptr = qlist[key_split]; 
                    wqptr->bulk_push(iter->second);
                    iter->second.clear(); 
                    push_end = KVGuide::getNanoSecond(ts2);     
                    push_end = push_end - push_start; 
                    push_ts.push_back(push_end);
                }
                //std::cout << "kvlist:" << iter->second.size() <<"\n";
            }
            for(auto it = push_ts.begin(); it != push_ts.end(); ++it){
                logger_->info("{0:d}", guide_end + *it);
            }
            push_ts.clear();
        }
        else if(goodpop && kv_list.size() < bulk_size && gqptr->getSize() == 0){
            for(int i=0; i < kv_list.size(); i++){
                KVTuple kvr = kv_list[i];
                uint8_t key_byte = kvr.getKey(0,header_offset);
                int split = getSplit(key_byte);
                BufferTable[split].push_back(std::move(kvr));           
            }
            loop_counter=loop_counter+kv_list.size();
            //check the bulk of which split is fullm and then bulk push it to the queue
            for( auto iter = BufferTable.begin(); iter != BufferTable.end(); ++iter){
                auto kv_vector = iter->second;
                auto key_split = iter->first;
                auto wqptr = qlist[key_split]; 
                wqptr->bulk_push(kv_vector); 
                kv_vector.clear();
            }

        }

        if(tracker->getReadCount() == num_thread && gqptr->getSize()== 0){ 
            tracker->guide_increment();
            break;
        }             
    }

}

/*int KVGuide::getSplit(int key) const{
    for(int index=0; index<splitPoints.size(); index++){
        if(key < splitPoints[index]){
            return index;
        }
        else{
            continue;
        }
	}
}*/