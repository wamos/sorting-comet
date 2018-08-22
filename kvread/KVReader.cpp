#include "KVReader.h"
#include <iostream>
#include <memory>
#include <cstdint>
#include <chrono>
#include <sys/syscall.h>
#include <cstdint>
#define gettid_syscall() syscall(__NR_gettid)

KVReader::KVReader(int id, std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, std::shared_ptr<StageTracker> tracker, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd)
:
rid(id), 
kv_io(std::move(io)), 
ReadQueue(std::move(queue)), 
tcp_socket(std::move(sock_ptr)), 
read_tracker(std::move(tracker)),
node_status(rcv_or_snd), 
record_size(100){
    std::cout << "kvread ctor" << "\n";
    _logger = spdlog::get("kvreader_logger");
}


/*KVReader::KVReader(std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd)
: kv_io(std::move(io)), 
  ReadQueue(std::move(queue)), 
  tcp_socket(std::move(sock_ptr)), 
  node_status(rcv_or_snd), 
  record_size(100){
  std::cout << "kvread ctor" << "\n";
  _logger = spdlog::get("kvreader_logger");
}*/

KVReader::~KVReader() {
    std::cout << "kvread dtor" << "\n";
    if(m_ReaderThread.joinable())
        m_ReaderThread.join();
    std::cout<< "ReaderThread " << (int32_t) gettid_syscall() << " join:" <<"\n";
    //_logger->info("kv read thread joins");
}

KVReader::KVReader(KVReader&& other)
    :rid(-1),
    ReadQueue(nullptr),
    kv_io(nullptr),
    read_tracker(nullptr),
    tcp_socket(nullptr),
    m_ReaderThread(std::move(other).m_ReaderThread),
    record_size(100),
    node_status(0){

    _logger = spdlog::get("kvreader_logger");
    std::cout << "kvread move ctor" << "\n";
    *this = std::move(other);
}

KVReader& KVReader::operator= (KVReader&& other){
    std::cout << "kvread move assign" << "\n";
    if(this!=&other){ // prevent self-move
        rid = other.rid;
        ReadQueue = std::move(other.ReadQueue);
        read_tracker=std::move(other.read_tracker);
        kv_io     = std::move(other.kv_io); 
        m_ReaderThread = std::move(other.m_ReaderThread);
        tcp_socket = std::move(other.tcp_socket);
        record_size = other.record_size;
        node_status = other.node_status;

        //other.tcp_socket = nullptr;
        //other.ReadQueue = nullptr;
        //other.kv_io = nullptr;
        //other.m_ReaderThread = nullptr;
        other.rid = -1;
        other.record_size = 0;
        other.node_status = -1;
    }
    return *this;  
}

void KVReader::threadJoin(){
    if(m_ReaderThread.joinable())
        m_ReaderThread.join();  
}


/*void KVReader::setNetworkConfig(std::string ip, std::string port){
        server_IP=ip;
        server_port=port;
}

void KVReader::setRecvConfig(uint32_t num, uint32_t iter){
    num_host = num;
    num_iter = iter;
    int64_t rounds = (int64_t) num_iter;
    int64_t sizes  = (int64_t)num_record * (int64_t)record_size;
    total_expected_bytes = rounds * sizes;
    std::cout << "total expected bytes:" <<  +total_expected_bytes << "\n";
}*/


void KVReader::submitKVRead() {
	if(node_status == 1){
    	//std::thread(&KVReader::readingKV, this).swap(m_ReaderThread);
        std::thread(&KVReader::bulkReadingKV, this).swap(m_ReaderThread);
    	//_logger->info("kv reader thread starts");
	}
	else if(node_status ==2){
		std::thread(&KVReader::receiveKV, this).swap(m_ReaderThread);
		//_logger->info("kv receiver thread starts");
	}
    //std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
}

void KVReader::receiveKV() {
}

void KVReader::bulkReadingKV() {
    //std::cout << "read kv num_record:" <<  +num_record << "\n";
    int loop_counter = 0;
    struct timespec ts1,ts2, ts3;
    uint64_t read_start, read_end, queue_end;
    int32_t tid =(int32_t) gettid_syscall();
    std::cout << "read_tid:" << tid << "\n";
    //std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
    //_logger->info("read kvr starts");
    auto q_ptr=ReadQueue.get();

    auto counter = read_tracker.get();
    int file_num = kv_io->getInputFileNum();

    for(int file_index = 0; file_index< file_num; file_index++){
        uint32_t iters =  (uint32_t) kv_io->genReadIters(file_index);
        //std::cout << "read_iters:" << iters << ", tid" << tid << "\n";
        size_t bulk_size = kv_io->getBulkSize();
        iters = iters/bulk_size;
        for(uint32_t i = 0; i < iters; i++){
            //std::cout << "file:" << file_index << ", iters:" << i << ", tid" << tid << "\n";
            read_start = KVReader::getNanoSecond(ts1);
            std::vector<KVTuple> kv_list = kv_io->bulkRead(file_index); 
            for(size_t j = 0; j < bulk_size; j++){
                read_end = KVReader::getNanoSecond(ts2);
                q_ptr->spin_push(kv_list[j]);
                queue_end = KVReader::getNanoSecond(ts3);
            }
            loop_counter=loop_counter+bulk_size;

            //MODIFY: move this if statement after the for loop!!!!!!!!!!
            if(i == iters-1 && file_index == file_num-1){ // the last one!
                std::cout<< "the last one!" <<"\n";
                bool goodadd = counter->read_increment(); 
                //kv_list.back().markLast();
                //std::cout << "reader counter:" <<  counter->getCount() << "\n";
                std::cout << "read markLast:" <<  loop_counter << "\n";
                //_logger->info("read_markLast");
            }

            //_logger->info("read_time {0:d}", read_end-read_start);
            _logger->info("{0:d}", read_end-read_start);
            //_logger->info("push_time {0:d}", queue_end-read_end);
            //_logger->info("gateway_kvr_time {0:d}", queue_end-read_end);    
        }

    }
    //_logger->info("read loop ends");
    std::cout << "read loop counter:" <<  loop_counter << "\n";
    //_logger->info("read_ends");
}

/*
void KVReader::readingKV() {
    //std::cout << "read kv num_record:" <<  +num_record << "\n";
    int loop_counter = 0;
    struct timespec ts1,ts2, ts3;
    uint64_t read_start, read_end, queue_end;
    int32_t tid =(int32_t) gettid_syscall();
    std::cout << "read_tid:" << tid << "\n";
    //std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
    //_logger->info("read kvr starts");
    auto q_ptr=ReadQueue.get();
    //auto counter = fileop_counter.get();
    int file_num = kv_io->getInputFileNum();
    for(int file_index = 0; file_index< file_num; file_index++){
        uint32_t iters =  (uint32_t) kv_io->genReadIters(file_index);
        std::cout << "read_iters:" << iters << ", tid" << tid << "\n";
        for(uint32_t i = 0; i < iters; i++){
            read_start = KVReader::getNanoSecond(ts1);
            KVTuple kvr;
            //kvr.initRecord(record_size);
            kv_io->readTuple(kvr,file_index);
            if(i == iters-1 && file_index == file_num-1){ // the last one!
                //bool goodadd =counter->read_increment();
                kvr.markLast();
                //_logger->info("read_markLast", kvr.getTag() );
            }
            read_end = KVReader::getNanoSecond(ts2);
            q_ptr->push(kvr);
            queue_end = KVReader::getNanoSecond(ts3);
            loop_counter++;
            _logger->info("{0:d}", read_end-read_start);
            //_logger->info("push_time {0:d}", queue_end-read_end);
            //_logger->info("gateway_kvr_time {0:d}", queue_end-read_end);    
        }

    }
    //_logger->info("read loop ends");
    std::cout << "read loop counter:" <<  loop_counter << "\n";
    //_logger->info("read_ends");
} 
*/
