//#include <iostream>
#include <utility>
#include <iostream>
#include "ConcurrentQueue.h"

ConcurrentQueue::ConcurrentQueue(){	
}

ConcurrentQueue::ConcurrentQueue(uint64_t id, uint64_t qsize)
	:queue_id(id),
    queue_size(qsize),
    total_bytes(0){
    std::cout << "cq ctor" << "\n";
}

ConcurrentQueue::~ConcurrentQueue(){
	std::cout << "cq dtor" << "\n";
	queue_.clear();
}

ConcurrentQueue::ConcurrentQueue(ConcurrentQueue&& other)
	:queue_(std::move(other).queue_),
	queue_id(0),
    queue_size(0),
    total_bytes(0){
    std::cout << "cq move ctor" << "\n";	
    *this = std::move(other);
}

ConcurrentQueue& ConcurrentQueue::operator= (ConcurrentQueue&& other){
	std::cout << "cq move assign" << "\n";
	if(this!=&other){ // prevent self-move
		queue_ = std::move(other.queue_);
		queue_id= other.queue_id;
		queue_size = other.queue_size;
		total_bytes = other.total_bytes;

		other.queue_.clear();
		other.queue_id=0;
		other.queue_size=0;
		other.total_bytes=0;
	}
	return *this;
}

void ConcurrentQueue::clear(){
	queue_.clear();
}

KVTuple ConcurrentQueue::pop(){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() > 0;});
	KVTuple item = queue_.front();
	queue_.pop_front();
	locker.unlock();
	cond_.notify_one();
	return item;
}

void ConcurrentQueue::pop(KVTuple& item){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() > 0;});
	item = queue_.front();
	queue_.pop_front();
	locker.unlock();
	cond_.notify_one();
}

//condtion variable wait for

void ConcurrentQueue::push(const KVTuple& item){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() < queue_size;});
	queue_.push_back(item);
	total_bytes += item.getSize();
	locker.unlock();
	cond_.notify_one();
}

void ConcurrentQueue::push(KVTuple&& item){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() < queue_size;});
	queue_.push_back(item);
	total_bytes += item.getSize();
	locker.unlock();
	cond_.notify_one();
}

bool ConcurrentQueue::isEmpty() const{
	return queue_.empty();
}

uint64_t ConcurrentQueue::getSize() const{
	return queue_.size();
}

uint64_t ConcurrentQueue::getPushedBytes() const{
	return total_bytes;
}




