//#include <iostream>
#include <utility>
#include <iostream>
#include "ConcurrentQueue.h"

ConcurrentQueue::ConcurrentQueue(){	
}

ConcurrentQueue::ConcurrentQueue(uint64_t id, uint64_t qsize)
	:queue_id(id),
    queue_size(qsize),
    push_times(0),
    pop_times(0),
    total_bytes(0),
    spinlock_(ATOMIC_FLAG_INIT){
    std::cout << "cq ctor" << "\n";
    logger_ = spdlog::get("queue_logger");
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
    logger_ = spdlog::get("queue_logger");	
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
	//logger_->info("pop_qsize {0:d}",queue_.size());
	cond_.notify_one();
}

//condtion variable wait for

void ConcurrentQueue::push(const KVTuple& item){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() < queue_size;});
	queue_.push_back(item);
	//total_bytes += item.getSize();
	locker.unlock();
	//logger_->info("push_qsize {0:d}",queue_.size());
	cond_.notify_one();
}

void ConcurrentQueue::push(KVTuple&& item){
	std::unique_lock<std::mutex> locker(mutex_);
	cond_.wait(locker, [this](){return queue_.size() < queue_size;});
	queue_.push_back(item);
	//total_bytes += item.getSize();
	//logger_->info("move push, qsize {0:d}",queue_.size());
	locker.unlock();
	cond_.notify_one();
}

//TODO: imporve performance, 
//ref https://github.com/facebook/folly/blob/master/folly/SpinLock.h
//ref https://github.com/cyfdecyf/spinlock
bool ConcurrentQueue::spin_pop(KVTuple& item){
	int spin_counter=0;
	bool return_flag=false;
	// like condition variable wait though
	while(queue_.size() <= 0){
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

	while (spinlock_.test_and_set(std::memory_order_acquire)) //acquire lock
		; // yo, let's spin
	//we can access resource here
	item = queue_.front(); 
	queue_.pop_front();
	pop_times++;
	spinlock_.clear(std::memory_order_release);//release lock
	logger_->info("spin_pop {0:d} {1:d}",queue_id, queue_.size());
	//std::cout << "pop:" << queue_id <<","<< queue_.size() << "\n";
	return true;
}

bool ConcurrentQueue::spin_push(const KVTuple& item){
	// like condition variable wait
	int spin_counter=0;
	bool return_flag=false;
	while(queue_.size() >= queue_size){
		spin_counter++;
		if(spin_counter > 1000){ //change from 1000000
			return_flag = true;
			break;
		}
		continue;
	}
	if(return_flag)
		return false;

	while (spinlock_.test_and_set(std::memory_order_acquire)) //acquire lock
		; // yo, let's spin
	//we can access resource here
	queue_.push_back(item);
	//total_bytes += item.getSize();
	push_times++;
	spinlock_.clear(std::memory_order_release);//release lock
	logger_->info("spin_push {0:d},{1:d}", queue_id, queue_.size());
	//std::cout << "push:" << queue_id <<","<< queue_.size() << "\n";
	return true;
}

bool ConcurrentQueue::bulk_push(std::vector<KVTuple>& kv_list){
	int spin_counter=0;
	bool return_flag=false;
	while(queue_.size() >= queue_size){
		spin_counter++;
		if(spin_counter > 1000){  //change from 1000000
			return_flag = true;
			break;
		}
		continue;
	}
	if(return_flag)
		return false;

	while (spinlock_.test_and_set(std::memory_order_acquire)) //acquire lock
		; // yo, let's spin
	//we can access resource here
	//queue_.insert(queue_.begin(), kv_list.begin(), kv_list.end());
	//push_times = push_times+kv_list.size();
	int bulk_size = kv_list.size();
	for(int i=0; i < bulk_size; i++){
		//queue_.push_back(std::move(kv_list[i]));
		KVTuple item = kv_list[i];
		queue_.push_back(std::move(item));
		//total_bytes += item.getSize();
		push_times++;
	}
	spinlock_.clear(std::memory_order_release);//release lock
	logger_->info("bulk_push {0:d},{1:d},{2:d}", queue_id, queue_.size(),bulk_size);
	//std::cout << "push:" << queue_id <<","<< queue_.size() << "\n";
	return true;
}

bool ConcurrentQueue::bulk_pop(std::vector<KVTuple>& kv_list, size_t bulk_size){
	int spin_counter=0;
	bool return_flag=false;
	// like condition variable wait though
	while(queue_.size() <= 0){
		spin_counter++;
		if(spin_counter > 1000){ //TODO: a more reasonalbe value to abort the spin?
			return_flag = true;
			break;
		}
		continue;
	}
	if(return_flag)
		return false;

	while (spinlock_.test_and_set(std::memory_order_acquire)) //acquire lock
		; // yo, let's spin
	//we can access resource here
	if( queue_.size() < bulk_size){
		bulk_size = queue_.size(); 
	} 
		//return false;-> this causes the program idling since there's no 
		//enough kv pairs to make up a bulk.

	for(size_t i=0; i < bulk_size; i++){
		KVTuple item = queue_.front(); 
		//total_bytes += item.getSize();
		kv_list.push_back(std::move(item));
		queue_.pop_front();
		pop_times++;
	}
	spinlock_.clear(std::memory_order_release);//release lock
	//logger_->info("{0:d}", queue_.size());
	logger_->info("bulk_pop {0:d},{1:d},{2:d}", queue_id, queue_.size(), bulk_size);
	//std::cout << "pop:" << queue_id <<","<< queue_.size() << "\n";
	return true;
}




bool ConcurrentQueue::isEmpty() const{
	if(push_times-pop_times==0)
		return true;
	else
		return false;
	//return queue_.empty();
}

uint64_t ConcurrentQueue::getSize() const{
	return push_times-pop_times;
	//return queue_.size();
}

uint64_t ConcurrentQueue::getPushedTimes() const{
	return push_times;
}




