#ifndef MEMPOOL_H
#define MEMPOOL_H

#include <memory>
#include <mutex>
#include <cstring>
#include <iostream>
#include <vector>
#include <condition_variable>
#include "KVTuple.h"

//template <class char>
class MemoryPool {
public:
	MemoryPool(size_t size, size_t step) 
	: kv_ptr(nullptr)
	, chunk_size(step)
	, max_size_(size*step)
	, head_(0)
    , tail_(0)
    , full_(false){
    	std::cout << "mp default ctor" << "\n";
    	std::cout << "alloc mem" << "\n";
    	kv_ptr = (char*) malloc(size*step*sizeof(char));
    	// memset(kv_ptr, 0, size*sizeof(char) ); // the memory is clear
    	/// the pool is filled up
    	head_ = (max_size_ - 1)*chunk_size;
	}

	~MemoryPool(){
		std::cout << "mp dtor" << "\n";
		std::cout << "free mem" << "\n";
		free(kv_ptr);
	}

	MemoryPool(MemoryPool&& other) noexcept 
	: kv_ptr(nullptr)
	, max_size_(0)
	, head_(0)
    , tail_(0)
    , chunk_size(other.chunk_size)
    , full_(false){
    	//std::cout << "mp move ctor" << "\n";
    	//logger_ = spdlog::get("queue_logger");	
    	*this = std::move(other);
	}

	MemoryPool& operator= (MemoryPool&& other) noexcept{
		//std::cout << "mp move assign" << "\n";
		if(this!=&other){ // prevent self-move
			kv_ptr = other.kv_ptr;
			head_ = other.head_;
			tail_ = other.tail_;
			full_ = other.full_;

			other.kv_ptr=nullptr;
			other.full_=false;
			other.head_=0;
			other.tail_=0;
		}

		return *this;
	}

	bool releaseMemBlock(){ //push
		//std::cout << "mp rel, qpush" << "\n";
		std::unique_lock<std::mutex> lock(mutex_);
		// don't want to clear the memory here 
		// memset ( &kv_ptr[head_], 0, chunk_size*sizeof(char));
		// it may cause errors if the memeory relase order is not the same as memory acquire order
		head_ = (head_ + chunk_size) % max_size_;

		if(head_ == tail_)
			full_ = true;
		else
			full_ = false;

		lock.unlock();

		return true;
	}

	char* acquireMemBlock(){ //pop
		//std::cout << "mp acq, qpop" << "\n";
		std::unique_lock<std::mutex> lock(mutex_);

		//expand the pool if the pool is 3/4 full
		if( tail_%max_size_ >= 3*max_size_/4){
			std::cout << "mp expand!!!!!!!" << "\n";
			kv_ptr = (char*) realloc(kv_ptr, max_size_*2);
			max_size_=max_size_*2;
		}

		char* mem = &kv_ptr[tail_];
		full_ = false;
		tail_ = (tail_ + chunk_size) % max_size_;

		lock.unlock();

		return mem;
	}

	void reset(){
		std::unique_lock<std::mutex> lock(mutex_);
		head_ = tail_;
		full_ = false;
	}

	bool isEmpty() const{
		//if head and tail are equal, we are empty
		return (!full_ && (head_ == tail_));
	}

	bool full() const{
		//If tail is ahead the head by 1, we are full
		return full_;
	}

	size_t capacity() const{
		return max_size_;
	}

	size_t getSize() const{
		size_t size = max_size_;

		if(!full_){
			if(head_ >= tail_){
				size = head_ - tail_;
			}
			else{
				size = max_size_ + head_ - tail_;
			}
		}

		return size;
	}

private:
	std::mutex mutex_;
	std::condition_variable cond_;
	char* kv_ptr;
	size_t head_ = 0;
	size_t tail_ = 0;
	const size_t chunk_size;
	size_t max_size_;
	bool full_ = false;
};

#endif