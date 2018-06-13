#include "KVWriter.h"
#include <iostream>
#include <memory>

KVWriter::KVWriter(KVFileIO& io, std::shared_ptr<ConcurrentQueue> queue, int connected_thread_num, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd)
        : kv_io(io), WriteQueue(queue), tcp_socket(std::move(sock_ptr)), node_status(rcv_or_snd), num_thread(connected_thread_num){
    _logger = spdlog::get("kvwriter_logger");
}

KVWriter::~KVWriter() {
	//if(m_WriterThread.joinable())
	//std::cout << "kv w-queue empty? " << WriteQueue.empty() << "\n";
	//std::cout << "kv w-queue size " << GuideQueue.size() << "\n";
    m_WriterThread.join();
    std::cout<< "WriterThread join" << "\n";
	//_logger->info("writer thread joins");
}
KVWriter::KVWriter(KVReader&& other)
    :WriteQueue(nullptr),
    //GuideQueue(nullptr),
    kv_io(nullptr),
    tcp_socket(nullptr),
    m_WriterThread(std::move(other).m_WriterThread),
    record_size(100),
    node_status(0){

    _logger = spdlog::get("kvwriter_logger");
    //std::cout << "kvwrite move ctor" << "\n";
    *this = std::move(other);
}

KVWriter& KVWriter::operator= (KVWriter&& other){
    std::cout << "kvwrite move assign" << "\n";
    if(this!=&other){ // prevent self-move
        WriteQueue = std::move(other.WriteQueue);
        //GuideQueue = std::move(other.GuideQueue);
        kv_io     = std::move(other.kv_io); 
        m_WriterThread = std::move(other.m_WriterThread);
        tcp_socket = std::move(other.tcp_socket);
        record_size = other.record_size;
        node_status = other.node_status;

        //other.tcp_socket = nullptr;
        //other.ReadQueue = nullptr;
        //other.kv_io = nullptr;
        //other.m_ReaderThread = nullptr;
        other.record_size = 0;
        other.node_status = -1;
    }
    return *this;  
}


    
void KVWriter::startWriterThread(){
        if(node_status == 1){
            //std::cout<< "sendLoop" << "\n";
            m_WriterThread= std::thread(&KVWriter::sendingKV, this);
            //_logger->info("send thread starts");
        }
        else if(node_status == 2){
            //std::cout<< "writingLoop" << "\n";
            m_WriterThread= std::thread(&KVWriter::writingKV, this);
            //_logger->info("write thread starts");
        }
        else{
            std::cerr << "Network Thread Error: unknown node status\n";
        }
        std::cout << "write thread id:" << m_WriterThread.get_id() << "\n";
}

/*void KVWriter::submitWrite(const KVTuple& kvr) {
    KVTuple kvr;
    //auto rqueue=temp_ReadQueue.get();
    //rqueue->pop(kvr);
    auto wqueue = WriteQueue.get();
    wqueue->push(kvr);    
}*/

void KVWriter::setKeyIndex(uint32_t index){
	 key_index = index;
}

/*void AsyncKVWriter::setNetworkConfig(std::string ip, std::string port){
	server_IP=ip;
	server_port=port;
}*/
void KVWriter::writingKV_rwtest() {
    //struct timespec ts1,ts2, ts3;
    //uint64_t write_start, write_end, queue_end;
    /*int file_num = kv_io->getOutputFileNum();
    for(int file_index = 0; file_index< file_num; file_index++){
        uint32_t iters =  (uint32_t) kv_io->genReadIters(file_index);
        std::cout << "read iters:" << iters << "\n";
        for(uint32_t i = 0; i< iters; i++){
            wrtie_start = KVWriter::getNanoSecond(ts1);
            KVTuple kvr;
            auto wqueue = WriteQueue.get();
            wqueue->pop(kvr);
            kv_io->writeTuple(kvr,file_index);
            write_end = KVWriter::getNanoSecond(ts2);
            _logger->info("write_time {0:d}", write_end-write_start);
            loop_counter++;
            //queue_end = KVWriter::getNanoSecond(ts3);
            //_logger->info("gateway_kvr_time {0:d}", queue_end-write_end);    
        }       
    }*/
}

void KVWriter::writingKV() {
    KVTuple kvr;
    int isLastCounter = 0;
    uint32_t loop_counter=0;
    auto wqueue = WriteQueue.get();
    while(isLastCounter < num_thread){      
        wqueue->pop(kvr);
        kv_io->writeTuple(kvr,key_index);
        if(kvr.checkLast()){
            std::cout << "kvr tag" << kvr.getTag() << "\n";
            isLastCounter++;
        }
        loop_counter++;
        //std::cout << "loop counter:" <<  +loop_counter << "\n";
    }
    std::cout << "last counter:" <<  +isLastCounter << "\n";
    std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";
    std::cout << "writer counter:" <<  +loop_counter << "\n";
}

void KVWriter::sendingKV(){
}

/*
    int loop_counter = 0;
    struct timespec ts1,ts2, ts3;
    uint64_t write_start, write_end, queue_end;
    //_logger->info("write loop starts");
    int iters = (int) kv_io.getReadIters();
    std::cout << "write iters:" << iters << "\n";
    KVTuple kvr;
    for(int i = 0; i< iters; i++){
    	KVWriteQueue.pop(kvr);
    	kv_io.writeTuple(kvr, key_index);	
    	//KVFreeQueue.push(kvr);
    	loop_counter++;
    	//bool good_pop = KVWriteQueue.try_pop(kvr);
    	//if(good_pop){
    	//	kv_io.writeTuple(kvr, key_index);	
    	//	KVFreeQueue.push(kvr);
    	//	loop_counter++;
    	//}
	}
	//std::cout<< "WQ:" << KVWriteQueue.size() << "\n";
    //_logger->info("write loop ends");
    std::cout << "write loop counter:" <<  loop_counter << "\n";
}*/