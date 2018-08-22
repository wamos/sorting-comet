#include <iostream>
#include <thread>
#include <cstring>
#include <functional>

#include "BoundedQueue.h"
#include "KVTuple.h"
#include "ConcurrentQueue.h"
#include "spdlog/spdlog.h"

#define COUNT 100000
//define COUNT 1000

inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}


template<typename T> 
void consumer_func(T* queue){
    size_t count = COUNT;
    struct timespec ts1,ts2, ts3;
    uint64_t read_start, read_end, gateway_end;
    std::shared_ptr<spdlog::logger> _logger = spdlog::get("logger");
    while (count > 0) {
        KVTuple kvr;
        read_start = getNanoSecond(ts1);
        bool good_dequeue = queue->dequeue(kvr);
        read_end = getNanoSecond(ts2);
        _logger->info("pop_time {0:d}", read_end-read_start);
        if (good_dequeue) {
            --count;
        }
    }
}

template<typename T> 
void bounded_producer_func(T* queue){
    size_t count = COUNT;
    struct timespec ts1,ts2, ts3;
    uint64_t read_start, read_end, gateway_end;
    std::shared_ptr<spdlog::logger> _logger = spdlog::get("logger");
    while (count > 0) {
        //TupleBuffer* kvr = new TupleBuffer();
		KVTuple kvr;
		kvr.initRecord(100);
        read_start = getNanoSecond(ts1);
        bool good_enqueue = queue->enqueue(kvr);
        read_end = getNanoSecond(ts2);
        _logger->info("push_time {0:d}", read_end-read_start);
        if (good_enqueue) {
            --count;
        }
    }
}


template<typename T> 
long double run_test( T producer_func, T consumer_func){
    typedef std::chrono::high_resolution_clock clock_t;
    typedef std::chrono::time_point<clock_t> time_t;
    time_t start;
    time_t end;

    start = clock_t::now();
    std::thread producer(producer_func);
    std::thread consumer(consumer_func);
    //TODO add more threads: 4 threads for producer, 4 threads for consumer

    producer.join();
    consumer.join();
    end = clock_t::now();

    std::cout << "thread joins\n";

    return
        (end - start).count()
        * ((double) std::chrono::high_resolution_clock::period::num
           / std::chrono::high_resolution_clock::period::den);
}

int main(){
    {
        auto cas_logger = spdlog::basic_logger_mt("logger", "/tmp/CAS.test");
        typedef mpmc_bounded_queue_t<KVTuple> queue_t;
        queue_t queue(65536);
        long double seconds = run_test(std::bind(&bounded_producer_func<queue_t>, &queue),
                                       std::bind(&consumer_func<queue_t>, &queue));
        std::cout << "CAS queue completed "
                  << COUNT
                  << " iterations in "
                  << seconds
                  << " seconds. \n";
    }
    /*{
        auto spin_logger = spdlog::basic_logger_mt("logger", "/tmp/spinlcok.test");
        ConcurrentQueue queue(0,65536);
        long double seconds = run_test(std::bind(&bounded_producer_func<ConcurrentQueue>, &queue),
                                       std::bind(&consumer_func<ConcurrentQueue>, &queue));
        std::cout << "spinlock dequeue completed "
                  << COUNT
                  << " iterations in "
                  << seconds
                  << " seconds. \n";
    }*/


    return 0;
}
