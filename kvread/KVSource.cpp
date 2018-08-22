#include "KVSource.h"
#include <iostream>
#include <vector>

KVSource::KVSource(int id, uint32_t ksize, uint32_t vsize, std::shared_ptr<CircularQueue> cq, std::shared_ptr<StageTracker> tracker, std::shared_ptr<MemoryPool> mpl)
  :key_size(ksize),
  val_size(vsize),
  header_size(10),
  source_stop_flag(false),
  Queue(cq), 
  pl_ptr(mpl),
  source_tracker(tracker),
  bulk_size(1000),
  source_id(id){
  counter=0;
  logger_ = spdlog::get("source_logger");
}

KVSource::~KVSource() {
  if(SourceThread.joinable())
    SourceThread.join();
  std::cout<< "Source Thread join" <<"\n";
  //_logger->info("kv read thread joins");
}

KVSource::KVSource(KVSource&& other)
  :source_id(-1),
  Queue(nullptr),
  pl_ptr(nullptr),
  key_size(0),
  val_size(0),
  header_size(0),
  source_stop_flag(false),
  bulk_size(1000),
  SourceThread(std::move(other).SourceThread),
  source_tracker(nullptr){
    counter=0;
    logger_ = spdlog::get("source_logger");
    std::cout << "kvsource move ctor" << "\n";
    *this = std::move(other);
}

KVSource& KVSource::operator= (KVSource&& other){
  std::cout << "kvsource move assign" << "\n";
    if(this!=&other){ // prevent self-move
      source_stop_flag = other.source_stop_flag;
      source_id = other.source_id;
      key_size = other.key_size;
      val_size = other.val_size;
      header_size = other.header_size;
      pl_ptr = std::move(other.pl_ptr);
      Queue = std::move(other.Queue);
      SourceThread = std::move(other.SourceThread);
      source_tracker = std::move(other.source_tracker);
      bulk_size = other.bulk_size;
      
      other.key_size=0;
      other.val_size=0;
      other.source_id=-1;
      other.source_stop_flag=true;
    }
    return *this;
}

/*void KVSource::preAllocMem(uint64_t alloc_size){
  pl_ptr.get();
  uint64_t size = header_size+key_size+val_size;
  kv_mem = new char[size*alloc_size];
}

void KVSource::deAllocMem(){
  //delete [] kv_mem;
}*/

void KVSource::setHeaderSize(uint32_t hsize){
      header_size=hsize;
}

void KVSource::setBulkSize(uint32_t bsize){
      bulk_size=bsize;
}

void KVSource::startSource(){
  std::cout<< "Source Thread start" <<"\n";
  SourceThread = std::thread(&KVSource::genKVTuple, this);
  //SourceThread = std::thread(&KVSource::bulkKVTuple, this);
}

//Manually control it to stop from outside!! like main thread
void KVSource::stopSource(){
  source_stop_flag=true;
  std::cout<< "src counter:" << +counter << "\n";
  source_tracker->read_increment();
  if(SourceThread.joinable())
    SourceThread.join();
  std::cout<< "Source Thread is stop" <<"\n";
}


/*void KVSource::bulkKVTuple(){
  std::random_device rd;
  std::default_random_engine gen = std::default_random_engine(rd());
  std::uniform_int_distribution<char> dis(0,255);
  struct timespec ts1, ts2, ts3;
  uint64_t src_start, src_end; 
  auto queue_ptr = Queue.get();
  //uint32_t kv_size = key_size+ val_size;

  while(source_stop_flag==false){
    src_start = KVSource::getNanoSecond(ts1);
    std::vector<KVTuple> kv_list;
    for (size_t index=0; index < bulk_size; index++){
      KVTuple kvr;
      kvr.initRecord(header_size, key_size, val_size); 
      //kvr.setKeyWithIndex(0, 0, header_size); // only setting the first byte
      kvr.setKeyWithIndex(dis(gen), 0, header_size); // only setting the first byte, random may causes time?
      kv_list.push_back(kvr);
    }
    bool goodpush = queue_ptr->bulk_push(kv_list);
    src_end = KVSource::getNanoSecond(ts2);
    if(goodpush){
      counter=counter+bulk_size;
      logger_->info("{0:d}", src_end-src_start);
    }
  }
  std::cout<< "src counter:" << +counter << "\n";
}*/

void KVSource::genKVTuple(){
  std::random_device rd;
  std::default_random_engine gen = std::default_random_engine(rd());
  std::uniform_int_distribution<char> dis(0,255);
  struct timespec ts1, ts2, ts3;
  uint64_t src_start, src_end; 

  auto mpool = pl_ptr.get();
  auto queue_ptr = Queue.get();
  //uint32_t kv_size = key_size+ val_size;

  while(source_stop_flag==false){
    src_start = KVSource::getNanoSecond(ts1);
    //KVTuple kvr(header_size, key_size, val_size, mpool->acquireMemBlock());
    KVTuple kvr(header_size, key_size, val_size); //kvr.initRecord(header_size, key_size, val_size); 
    kvr.setKeyWithIndex(1, 0, header_size);
    //kvr.setKeyWithIndex(dis(gen), 0, header_size); // only setting the first byte
    bool goodpush = queue_ptr->spin_push(std::move(kvr));
    if(goodpush){
      counter=counter+1;
      src_end = KVSource::getNanoSecond(ts2);
      logger_->info("{0:d}", src_end-src_start);
    }
  }
  //std::cout<< "src counter:" << +counter << "\n";
}
