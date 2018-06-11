#ifndef CONCUR_QUEUE_H
#define CONCUR_QUEUE_H

#include <deque>
#include <mutex>
#include <condition_variable>
#include <cstdint>
#include "KVTuple.h"

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

    //TODO: inline these methods?
    KVTuple pop();
    void pop(KVTuple& item);
    void push(const KVTuple& item);
    void push(KVTuple&& item);
    void clear();

    bool isEmpty() const;
    uint64_t getSize() const;
    uint64_t getPushedBytes() const;

    private:
    std::mutex mutex_;
    std::condition_variable cond_;
    std::deque<KVTuple> queue_;

    uint32_t queue_id;
    uint64_t queue_size; //predeclared queue size
    uint64_t total_bytes;
};

#endif
