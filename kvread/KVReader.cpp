#include "KVReader.h"
#include <iostream>
#include <memory>
#include <cstdint>
#include <chrono>

KVReader::KVReader(std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, uint32_t num_rec, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd)
: kv_io(std::move(io)), 
  ReadQueue(std::move(queue)),
  num_record(num_rec), 
  tcp_socket(std::move(sock_ptr)), 
  node_status(rcv_or_snd), 
  record_size(100){
  std::cout << "kvread ctor" << "\n";
  _logger = spdlog::get("kvreader_logger");
}

KVReader::~KVReader() {
    std::cout << "kvread dtor" << "\n";
    if(m_ReaderThread.joinable())
        m_ReaderThread.join();
    std::cout<< "ReaderThread join:" <<"\n";
    //_logger->info("kv read thread joins");
}

KVReader::KVReader(KVReader&& other)
    :ReadQueue(nullptr),
    kv_io(nullptr),
    tcp_socket(nullptr),
    m_ReaderThread(std::move(other).m_ReaderThread),
    record_size(100),
    num_record(0),
    node_status(0){

    _logger = spdlog::get("kvreader_logger");
    std::cout << "kvread move ctor" << "\n";
    *this = std::move(other);
}

KVReader& KVReader::operator= (KVReader&& other){
    std::cout << "kvread move assign" << "\n";
    if(this!=&other){ // prevent self-move
        ReadQueue = std::move(other.ReadQueue);
        kv_io     = std::move(other.kv_io); 
        m_ReaderThread = std::move(other.m_ReaderThread); // ERROR!!
        tcp_socket = std::move(other.tcp_socket);
        record_size = other.record_size;
        num_record= other.num_record;
        node_status = other.node_status;

        //other.tcp_socket = nullptr;
        //other.ReadQueue = nullptr;
        //other.kv_io = nullptr;
        //other.m_ReaderThread = nullptr;
        other.record_size = 0;
        other.num_record = 0;
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
    	std::thread(&KVReader::readingKV, this).swap(m_ReaderThread);
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

void KVReader::readingKV() {
    std::cout << "read kv num_record:" <<  +num_record << "\n";
    int loop_counter = 0;
    struct timespec ts1,ts2, ts3;
    uint64_t read_start, read_end, gateway_end;
    std::cout << "read thread id:" << std::this_thread::get_id() << "\n";
    //_logger->info("read kvr starts");
    auto q_ptr=ReadQueue.get();
    int file_num = kv_io->getInputFileNum();
    for(int file_index = 0; file_index< file_num; file_index++){
        uint32_t iters =  (uint32_t) kv_io->genReadIters(file_index);
        std::cout << "read iters:" << iters << "," << std::this_thread::get_id() << "\n";
        for(uint32_t i = 0; i< iters; i++){
            read_start = KVReader::getNanoSecond(ts1);
            /*-----start---*/
            KVTuple kvr;
            kvr.initRecord(record_size);
            kv_io->readTuple(kvr,file_index);
            /*-----end-----*/
            read_end = KVReader::getNanoSecond(ts2);
            _logger->info("read_time {0:d}", read_end-read_start);
            q_ptr->push(kvr);
            loop_counter++;
            //queue_end = KVReader::getNanoSecond(ts3);
            //_logger->info("gateway_kvr_time {0:d}", queue_end-read_end);    
        }       
    }
    //_logger->info("read loop ends");
    std::cout << "read loop counter:" <<  loop_counter << "\n";
    _logger->info("read ends");
} 

//std::this_thread::sleep_for(std::chrono::milliseconds(5));
/*std::cout << "r:" << kvr.getTag() <<":";
for(uint32_t j = 0; j < 10; j++){
    uint8_t k = (uint8_t) kvr.getKey(j);
    std::cout << +k << " ";
}
std::cout << "\n";*/
