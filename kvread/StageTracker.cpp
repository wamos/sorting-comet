#include "StageTracker.h"
#include <iostream>

StageTracker::StageTracker(int connected_thread_num, int input_files_per_thread)
	:max_file_counter(connected_thread_num*input_files_per_thread)
	,spinlock_(ATOMIC_FLAG_INIT)
	,file_counter(0)
	,copy_file_counter(0)
	,logger_(nullptr){
		logger_ = spdlog::get("ctrl_logger");
}

StageTracker::StageTracker(StageTracker&& other)
	:spinlock_(ATOMIC_FLAG_INIT)
	,logger_(nullptr){
		*this = std::move(other);
}

StageTracker::~StageTracker(){}

StageTracker& StageTracker::operator= (StageTracker&& other){
	if(this!=&other){
		logger_ = spdlog::get("ctrl_logger");
		max_file_counter = other.max_file_counter;
		file_counter = other.file_counter;
		copy_file_counter = other.copy_file_counter;

		other.max_file_counter = 0;
		other.file_counter = 0;
		other.copy_file_counter = 0;
	}
	return *this;
}


// The following simple Spinlock can be buggy here if too many concurrent read happen
// TODO implement a RWSpinLock
int StageTracker::getReadCount(){
	return copy_file_counter;
}

int StageTracker::getGuideCount(){
	return copy_guide_counter;
}

//TODO: spdlog seg fault when spinlocks work?
bool StageTracker::read_increment(){
	std::cout<< "read_increment called!!!" <<"\n";
	if(file_counter >= max_file_counter){
		//logger_->info("file counter error, counter > max");
		return false;	
	}
	while (spinlock_.test_and_set(std::memory_order_acquire))
		;
	file_counter=file_counter+1;
	copy_file_counter = file_counter;
	spinlock_.clear(std::memory_order_release);//release lock
	//logger_->info("file counter 1");
	return true;
}

bool StageTracker::guide_increment(){
	if(guide_counter >= max_file_counter){
		//logger_->info("file counter error, counter > max");
		return false;	
	}
	while (spinlock_.test_and_set(std::memory_order_acquire))
		;
	guide_counter = guide_counter+1;
	copy_guide_counter = guide_counter;
	spinlock_.clear(std::memory_order_release);//release lock
	//logger_->info("file counter 1");
	return true;
}