#include "KVWriter.h"
#include <iostream>
#include <memory>

KVWriter::KVWriter(int id, int connected_file_guiders, std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, std::shared_ptr<StageTracker> tracker, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd)
    :
    wid(id),
    num_file_guider(connected_file_guiders),
    write_tracker(tracker),
    kv_io(std::move(io)), 
    WriteQueue(std::move(queue)), 
    tcp_socket(std::move(sock_ptr)), 
    node_status(rcv_or_snd),
    header_offset(10),
    record_size(100){
    _logger = spdlog::get("kvwriter_logger");
}

KVWriter::~KVWriter() {
	//std::cout << "kv w-queue empty? " << WriteQueue.empty() << "\n";
	//std::cout << "kv w-queue size " << GuideQueue.size() << "\n";
    if(m_WriterThread.joinable())
        m_WriterThread.join();
    std::cout<< "WriterThread join" << "\n";
	//_logger->info("writer thread joins");
}

KVWriter::KVWriter(KVWriter&& other)
    :wid(-1),
    num_file_guider(0),
    WriteQueue(nullptr),
    kv_io(nullptr),
    tcp_socket(nullptr),
    write_tracker(nullptr),
    m_WriterThread(std::move(other).m_WriterThread),
    record_size(100),
    header_offset(10),
    node_status(0)
{
    _logger = spdlog::get("kvwriter_logger");
    //std::cout << "kvwrite move ctor" << "\n";
    *this = std::move(other);
}

KVWriter& KVWriter::operator= (KVWriter&& other){
    std::cout << "kvwrite move assign" << "\n";
    if(this!=&other){ // prevent self-move
        wid = other.wid;
        num_file_guider=other.num_file_guider;
        WriteQueue = std::move(other.WriteQueue);
        write_tracker = std::move(other.write_tracker);
        kv_io     = std::move(other.kv_io); 
        m_WriterThread = std::move(other.m_WriterThread);
        tcp_socket = std::move(other.tcp_socket);
        record_size = other.record_size;
        header_offset = other.header_offset;
        node_status = other.node_status;

        //other.tcp_socket = nullptr;
        //other.ReadQueue = nullptr;
        //other.kv_io = nullptr;
        //other.m_ReaderThread = nullptr;
        other.num_file_guider=0;
        other.wid = -1;
        other.record_size = 0;
        other.header_offset=0;
        other.node_status = -1;
    }
    return *this;  
}


    
void KVWriter::startWriterThread(){
        if(node_status == 1){
            //std::cout<< "sendLoop" << "\n";
            //std::thread(&KVWriter::sendingKV, this).swap(m_WriterThread);
            std::thread(&KVWriter::bulkWritingKV, this).swap(m_WriterThread);
            //_logger->info("send thread starts");
        }
        else if(node_status == 2){
            //std::cout<< "writingLoop" << "\n";
            //std::thread(&KVWriter::writingKV, this).swap(m_WriterThread);
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

void KVWriter::setKeyIndex(int index){
	 key_index = index;
}

/*void KVWriter::setNetworkConfig(std::string ip, std::string port){
	server_IP=ip;
	server_port=port;
}*/

void KVWriter::bulkWritingKV() {
    KVTuple kvr;
    struct timespec ts1,ts2, ts3;
    uint64_t write_start, write_end;

    uint32_t loop_counter=0;
    auto wqueue = WriteQueue.get();
    auto tracker = write_tracker.get();
    size_t bulk_size = kv_io->getBulkSize();
    int drain_counter = 0;
    bool drained_flag = false;

    while(true){    
        //for(int j = 0; j < bulk_size; j++){
        std::vector<KVTuple> kv_list;

        write_start = KVWriter::getNanoSecond(ts1);

        while(kv_list.size() < bulk_size && !drained_flag){
            bool goodpop= wqueue->spin_pop(kvr);
            if(goodpop){
                kv_list.push_back(kvr);
            }
            else{
                std::cout << "writer id:"<< +wid<< "queue drained" << "when loop_counter"<< loop_counter <<"\n";
                drain_counter++;
                //_logger->info("write_qdrain {0:d} q:{1:d} lc:{2:d}", wid, wqueue->getSize(), loop_counter);
                //Q: Could the code enters this before the last chunk of data?
            }

            if(drain_counter > bulk_size){
                //_logger->info("write_popbrk {0:d} q:{1:d} lc:{2:d}", wid, wqueue->getSize(), loop_counter);
                drained_flag=true;
            }
        }
        drain_counter=0;
        drained_flag=false;

        //std::cout << "bulk_size:" << bulk_size<<",";
        //std::cout << "writer id:"<< +wid << "kv_list size:" << kv_list.size()<<"\n";
        bool goodwrite = kv_io->bulkWrite(kv_list,key_index);
        write_end = KVWriter::getNanoSecond(ts2);
        
        if(goodwrite){
            loop_counter=loop_counter+kv_list.size();
            if(kv_list.size() == bulk_size){
                //_logger->info("write_time {0:d}", write_end-write_start);
                _logger->info("{0:d}", write_end-write_start);
            }
        }
        //_logger->info("write_queue {0:d} q {1:d} lc {2:d}", wid, wqueue->getSize(), loop_counter);

        //std::cout << "writer id:"<< +wid << "writer counter:" <<  +loop_counter << ", queue size" << wqueue->getSize() << "\n";

        if(tracker->getGuideCount() == num_file_guider && wqueue->isEmpty() ){ //wqueue->getSize()== 0){ 
            std::cout << "writer id:"<< +wid << ", queue size" << wqueue->getSize() << ", counter" << +loop_counter <<"\n";
            //_logger->info("write_end {0:d} q {1:d} lc {2:d}", wid, wqueue->getSize(), loop_counter);
            drained_flag=true;
            break;
        }
    }

    std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";
    std::cout << "writer counter:" <<  +loop_counter << "\n";
}

void KVWriter::sendingKV(){
    KVTuple kvr;
    struct timespec ts1,ts2, ts3;
    uint64_t send_start, send_end;

    uint32_t loop_counter=0;
    auto wqueue = WriteQueue.get();
    auto tracker = write_tracker.get();
    size_t bulk_size = kv_io->getBulkSize();
    int drain_counter = 0;
    bool drained_flag = false;

    tcp_socket->tcpConnect(server_IP, server_port, bulk_size*record_size, retryDelayInMicros, Retry);

    while(true){    
        //for(int j = 0; j < bulk_size; j++){
        std::vector<KVTuple> kv_list;

        send_start = KVWriter::getNanoSecond(ts1);

        while(kv_list.size() < bulk_size && !drained_flag){
            bool goodpop= wqueue->spin_pop(kvr);
            if(goodpop){
                kv_list.push_back(kvr);
            }
            else{
                std::cout << "sender id:"<< +wid<< "queue drained" << "when loop_counter"<< loop_counter <<"\n";
                drain_counter++;
                _logger->info("send_qdrain {0:d} q:{1:d} lc:{2:d}", wid, wqueue->getSize(), loop_counter);
                //Q: Could the code enters this before the last chunk of data?
            }

            if(drain_counter > bulk_size){
                _logger->info("send_popbrk {0:d} q:{1:d} lc:{2:d}", wid, wqueue->getSize(), loop_counter);
                drained_flag=true;
            }
        }
        drain_counter=0;
        drained_flag=false;

        //std::cout << "bulk_size:" << bulk_size<<",";
        //std::cout << "writer id:"<< +wid << "kv_list size:" << kv_list.size()<<"\n";

        //borrow fileio_buffer from kvio to use here
        char* fileio_buffer= kv_io->getFileIOBuffer();

        size_t bufferBytes=0;
        char* writing_buffer;
        for(int i=0; i < kv_list.size(); i++){
            KVTuple kvr = kv_list[i];
            char* kv_bufptr = kvr.getBuffer(header_offset);
            writing_buffer = (char*) (fileio_buffer + bufferBytes);
            std::copy(kv_bufptr, kv_bufptr + record_size, writing_buffer); 
            bufferBytes = bufferBytes + record_size;
        }

        int64_t sent_bytes = tcp_socket->tcpSend(fileio_buffer, kv_list.size()*record_size);
        send_end = KVWriter::getNanoSecond(ts2);
        
        if(sent_bytes>0){
            loop_counter=loop_counter+kv_list.size();
            if(kv_list.size() == bulk_size){
                _logger->info("send_time {0:d}", send_end-send_start);
            }
        }
        _logger->info("send_queue {0:d} q {1:d} lc {2:d}", wid, wqueue->getSize(), loop_counter);

        //std::cout << "writer id:"<< +wid << "writer counter:" <<  +loop_counter << ", queue size" << wqueue->getSize() << "\n";

        if(tracker->getGuideCount() == num_file_guider && wqueue->isEmpty() ){ //wqueue->getSize()== 0){ 
            std::cout << "sender id:"<< +wid << ", queue size" << wqueue->getSize() << ", counter" << +loop_counter <<"\n";
            _logger->info("send_end {0:d} q {1:d} lc {2:d}", wid, wqueue->getSize(), loop_counter);
            drained_flag=true;
            break;
        }
    }

    tcp_socket->tcpClose();

    std::cout << "queue is empty:"<< wqueue->isEmpty() <<"\n";
    std::cout << "sender counter:" <<  +loop_counter << "\n";
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