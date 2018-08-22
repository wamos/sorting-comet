#ifndef CIRQ_H
#define CIRQ_H

#include <memory>
#include <mutex>
#include <iostream>
#include <vector>
#include <condition_variable>
#include "KVTuple.h"

//https://github.com/Quuxplusone/ring_view
//https://github.com/WG21-SG14/SG14/blob/master/SG14/ring.h
//https://github.com/martinmoene/ring-span-lite

//Like: https://embeddedartistry.com/blog/2017/4/6/circular-buffers-in-cc#
//template <class T>
class CircularQueue {
public:
	CircularQueue(size_t size) 
	: kv_ptr(nullptr)
	, max_size_(size)
	, head_(0)
    , tail_(0)
    , pop_times(0)
    , push_times(0)
    , full_(false){
    	std::cout << "cq default ctor" << "\n";
    	kv_ptr = new KVTuple[size];
    	/*for(int i = 0; i < size; i++){
    		kv_ptr[i].initRecord(10,10,90);
    	}*/

    	//Use placement-new to reduce memory alloc overhead
    	//kv_ptr = (KVTuple*)new[size*sizeof(KVTuple));
    	//for(int i = 0; i < size; i++){
    	//	new (&kv_ptr[i]) KVTuple(10,10,90);
    	//}
    	//If placement_params are provided, they are passed to the allocation function as additional arguments. 
    	// Such allocation functions are known as "placement new", after the standard allocation function void* operator new(std::size_t, void*), which simply returns its second argument unchanged. 
    	//This is used to construct objects in allocated storage:
		//char* ptr = new char[sizeof(T)]; // allocate memory
		//T* tptr = new(ptr) T;            // construct in allocated storage ("place")
		//tptr->~T();                      // destruct
		//delete[] ptr;                    // deallocate memory
	}

	~CircularQueue(){
		std::cout << "cq dtor" << "\n";
		delete [] kv_ptr;
	}

	CircularQueue(CircularQueue&& other) noexcept
	: kv_ptr(nullptr)
	, max_size_(0)
	, head_(0)
    , tail_(0)
    , full_(false){
    	//std::cout << "cq move ctor" << "\n";
    	//logger_ = spdlog::get("queue_logger");	
    	*this = std::move(other);
	}

	CircularQueue& operator= (CircularQueue&& other) noexcept{
		//std::cout << "cq move assign" << "\n";
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

	bool bulk_push(std::vector<KVTuple>& kv_list);
    bool bulk_pop(std::vector<KVTuple>& kv_list, size_t bulk_size);

	bool spin_push(KVTuple&& item) noexcept{
		//std::cout << "cq push" << "\n";
		int spin_counter=0;
		bool return_flag=false;
		// like condition variable wait though
		while(getSize() >= max_size_){
			spin_counter++;
			if(spin_counter > 1000){ //TODO: a more reasonalbe value to abort the spin?
				//change from 1000000
				return_flag = true;
				break;
			}
			continue;
		}
		if(return_flag)
			return false;
		//cond_.wait(lock, [this](){return !full_ ;});

		std::unique_lock<std::mutex> lock(mutex_);

		kv_ptr[head_] = std::move(item);

		if(full_){
			tail_ = (tail_ + 1) % max_size_;
		}

		head_ = (head_ + 1) % max_size_;
		push_times++;

		if(head_ == tail_)
			full_ = true;
		else
			full_ = false;

		lock.unlock();
		//cond_.notify_one();
		return true;

	}

	//TODO, it needs a machism to backoff as well
	bool spin_pop(KVTuple& item) noexcept{
		//std::cout << "cq move pop" << "\n";
		int spin_counter=0;
		bool return_flag=false;
		// like condition variable wait though
		while(getSize() <= 0){
			spin_counter++;
			if(spin_counter > 1000){ //TODO: a more reasonalbe value to abort the spin?
				//change from 1000000
				return_flag = true;
				break;
			}
			continue;
		}
		if(return_flag)
			return false;
		//cond_.wait(lock, [this](){return !isEmpty();});

		std::unique_lock<std::mutex> lock(mutex_);
		item = std::move(kv_ptr[tail_]);
		full_ = false;
		tail_ = (tail_ + 1) % max_size_;
		pop_times++;

		lock.unlock();
		//cond_.notify_one();

		return true;
	}

	/*bool spin_pop(KVTuple& item){
		std::cout << "cq copy pop" << "\n";
		std::unique_lock<std::mutex> lock(mutex_);
		cond_.wait(lock, [this](){return !isEmpty();});

		item = kv_ptr[tail_];
		full_ = false;
		tail_ = (tail_ + 1) % max_size_;

		cond_.notify_one();
		return true;

		//return val;
	}*/

	void reset(){
		std::unique_lock<std::mutex> lock(mutex_);
		head_ = tail_;
		full_ = false;
	}

	bool isEmpty() const{
		//if head and tail are equal, we are empty
		if(push_times-pop_times==0){
			return true;
		}
		else{
			return false;
		}
		//return (!full_ && (head_ == tail_));
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
	//std::unique_ptr<KVTuple[]> buf_;
	KVTuple* kv_ptr;
	size_t head_ = 0;
	size_t tail_ = 0;
	uint64_t pop_times=0;
	uint64_t push_times=0;
	const size_t max_size_;
	bool full_ = 0;
};

#endif