#ifndef KVWRITE_H
#define KVWRITE_H

#include <string>
#include <cstdint>
#include <time.h>
#include "Socket.h"
#include "KVFileIO.h"
#include "KVTuple.h"
#include "ConcurrentQueue.h"
#include "spdlog/spdlog.h"
#include "StageTracker.h"

class KVWriter {
public:
    KVWriter(int id, int connected_file_guiders, std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, std::shared_ptr<StageTracker> tracker, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd);
    ~KVWriter();
    KVWriter(KVWriter&& other);
    KVWriter& operator= (KVWriter&& other);
    KVWriter(const KVWriter& other) = delete;
    KVWriter& operator= (const KVWriter& other)=delete;

    
    void startWriterThread();
	void submitWrite(const KVTuple& kvr);
    void setKeyIndex(int index);
    //void setNetworkConfig(std::string ip, std::string port);

    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

private:
    void bulkWritingKV();
    void sendingKV();

    std::unique_ptr<KVFileIO> kv_io;
    std::shared_ptr<ConcurrentQueue> WriteQueue;
    std::shared_ptr<StageTracker> write_tracker;
    int num_thread;
    int num_file_guider;

    int wid;
    /*uint32_t num_iter;
    uint32_t num_host;
    int64_t total_expected_bytes;*/
    uint32_t header_offset=10;
    uint32_t record_size=100;
    //uint32_t key_size=10;
    //uint32_t value_size=90;
    int node_status; // 0 = uninitialized, 1=read, 2=recv
    int key_index;
    std::shared_ptr<spdlog::logger> _logger;
    std::thread m_WriterThread;

    //Network Config
    uint64_t Retry=5;
    uint64_t retryDelayInMicros=1000;
    std::string server_IP;
    std::string server_port;
    std::unique_ptr<Socket> tcp_socket;
};


#endif
