class KVGuide {
public:
    KVGuide(std::shared_ptr<ConcurrentQueue> cq, int connected_thread_num)
    :GuideQueue(cq),num_thread(connected_thread_num){};

    ~KVGuide() {
    	GuideThread.join();
        WQueueList.clear();
        GuideQueue=nullptr;
    	std::cout<< "Sink Thread join" <<"\n";
    	//_logger->info("kv read thread joins");
	}

	void addWriteQueue(std::shared_ptr<ConcurrentQueue> queue){
		WQueueList.push_back(queue);
	}

	void genSplitPoints(int num_split){
		int step =floor( (double)256/ (double) num_split);
		for(int i=0; i< num_split; i++){
			splitPoints.push_back(step*i);	
		}
	}

	void startGuide(){
		std::cout<< "Sink Thread start" <<"\n";
		GuideThread = std::thread(&KVGuide::pushKV, this);
	}

    void threadJoin(){
   		if(GuideThread.joinable())
    		GuideThread.join();
    }

private:
    
void pushKV(){  
    KVTuple kvr;
    auto gqptr= GuideQueue.get();
    int isLastCounter = 0;
    uint32_t loop_counter=0;
    int split_0=0;
    int split_1=0;
    int split=0;
    while(isLastCounter < num_thread){ 
        gqptr->pop(kvr);
        int key_byte = kvr.getKey(0);
        split = getSplit(key_byte);
        auto wqptr =WQueueList[split].get();
        wqptr->push(kvr);
        if(kvr.checkLast()){
            kvr.unmarkLast();
            isLastCounter++;
        }
        loop_counter++;
    }
    // the guider pushes N kv-pairs to the N queues of KVWriters
    // here we got N= WQueueList.size()
    for(int i=0; i < WQueueList.size(); i++){
        auto wqptr =WQueueList[split].get();
        KVTuple kvr;
        kvr.markLast();
        wqptr->push(kvr);
        loop_counter++;
    }

    std::cout << "FG last counter:" <<  +isLastCounter << "\n";
    std::cout << "FG queue is empty:"<< gqptr->isEmpty() <<"\n";
    std::cout << "FG loop counter:" <<  +loop_counter << "\n";
}

int getSplit(int key) const{
    for(int index=0; index<splitPoints.size(); index++){
        if(key < splitPoints[index]){
            return index;
        }
        else{
            continue;
        }
	}
}

    int num_thread;
    int key_range=255;
    std::vector<int> splitPoints;
    std::shared_ptr<ConcurrentQueue> GuideQueue;
    std::vector<std::shared_ptr<ConcurrentQueue>> WQueueList;
    std::thread GuideThread;
};