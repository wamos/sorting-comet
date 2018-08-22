#ifndef CONCUR_QUEUE_H
#define CONCUR_QUEUE_H

#include <deque>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <cstdint>
#include <atomic>
#include "KVTuple.h"
#include "spdlog/spdlog.h"

#include <vector> // for bulk_push and bulk_pop
//TODO use template!!!
//template <typename T>

class ConcurrentQueue{
    public:
    ConcurrentQueue();
    ~ConcurrentQueue();    
    ConcurrentQueue(uint64_t qid, uint64_t qsize);
    ConcurrentQueue(ConcurrentQueue&& other);
    ConcurrentQueue& operator= (ConcurrentQueue&& other);
    // No copy or copy assignment constructor
    ConcurrentQueue(const ConcurrentQueue& other) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue& other) = delete;

    KVTuple pop();
    void pop(KVTuple& item);
    void push(const KVTuple& item);
    void push(KVTuple&& item); //Move Push, not tested

    bool spin_pop(KVTuple& item);
    bool spin_push(const KVTuple& item);
    bool bulk_push(std::vector<KVTuple>& kv_list);
    bool bulk_pop(std::vector<KVTuple>& kv_list, size_t bulk_size);
    bool spin_push(KVTuple&& item); 
    void clear();

    bool isEmpty() const;
    uint64_t getSize() const;
    uint64_t getPushedTimes() const;

    private:
    std::mutex mutex_;
    std::condition_variable cond_;
    std::deque<KVTuple> queue_;
    std::atomic_flag spinlock_;
    std::shared_ptr<spdlog::logger> logger_;

    uint32_t queue_id;
    uint64_t queue_size; //predeclared queue size
    uint64_t total_bytes;
    uint64_t push_times;
    uint64_t pop_times;
};

#endif
