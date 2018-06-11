#ifndef KVREAD_H
#define KVREAD_H

#include <string>
#include <cstdint>
#include <time.h>
#include <vector>
#include "Socket.h"
#include "KVFileIO.h"
#include "KVTuple.h"
#include "ConcurrentQueue.h"
#include "spdlog/spdlog.h"

class KVReader {
public:
    KVReader(std::unique_ptr<KVFileIO> io, std::shared_ptr<ConcurrentQueue> queue, uint32_t num, std::unique_ptr<Socket> sock_ptr, int rcv_or_snd);
    ~KVReader();
    KVReader(KVReader&& other);
    KVReader& operator= (KVReader&& other);
    KVReader(const KVReader& other) = delete;
    KVReader& operator= (const KVReader& other)=delete;

    void threadJoin();
    //void setNetworkConfig(std::string ip, std::string port);
    //void setRecvConfig(uint32_t num, uint32_t iter);
    void submitKVRead();

    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

private:

    void readingKV();
    void receiveKV();

    //KVFileIO kv_io;
    //ConcurrentQueue& ReadQueue;
    std::unique_ptr<KVFileIO> kv_io;
    std::shared_ptr<ConcurrentQueue> ReadQueue;

    uint32_t num_record;
    uint32_t num_iter;
    uint32_t num_host;
    int64_t total_expected_bytes;
    int record_size=100;
    uint32_t key_size=10;
    uint32_t value_size=90;
    int node_status; // 0 = uninitialized, 1=read, 2=recv
    std::thread m_ReaderThread;
    std::shared_ptr<spdlog::logger> _logger;

    //Network Config
    uint64_t timeoutInMicros= 1000*1000*60;
    int backlogSize=10;    
    std::unique_ptr<Socket> tcp_socket;
    std::string server_IP="127.0.0.1";
    std::string server_port="80";
};

#endif