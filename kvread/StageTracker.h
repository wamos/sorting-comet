#ifndef STAGE_TRACKER_H
#define STAGE_TRACKER_H

#include <thread>
#include <atomic>
#include "spdlog/spdlog.h"

class StageTracker {
public:
	StageTracker(int connected_thread_num, int input_files_per_thread);
	~StageTracker();
	StageTracker(StageTracker&& other);
    StageTracker& operator= (StageTracker&& other);
    // No copy constructor or copy assignment operator
    StageTracker(const StageTracker& other) = delete;
    StageTracker& operator=(const StageTracker& other) = delete;

	// The following simple Spinlock can be buggy here if too many concurrent read happen
	// TODO implement a RWSpinLock
	int getReadCount();
	int getGuideCount();

	bool read_increment();
	bool guide_increment();


private:
	int max_file_counter;
	int file_counter;
	int copy_file_counter;
	int guide_counter;
	int copy_guide_counter;	
	std::atomic_flag spinlock_;
	std::shared_ptr<spdlog::logger> logger_;
};

#endif