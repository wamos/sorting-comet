#include <thread>
#include <chrono>
#include <iostream>
#include <utility>
//#include "json.hpp"
#include "utility.h"
/*---------------*/
//#include "ConcurrentQueue.h"
#include "CircularQueue.h"
#include "MemoryPool.h"
#include "StageTracker.h"
#include "KVTuple.h"
#include "KVSink.h"
#include "KVSource.h"
//#include "KVGuide.h"
#include "spdlog/spdlog.h"

#define SPDLOG_TRACE_ON
#define SPDLOG_DEBUG_ON

inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}


int main(int argc, char* argv[]) {

	size_t record_size= 100;
	uint32_t key_size=10;
	uint32_t val_size=99980; //uint32_t val_size=90;
	uint32_t header_size=10;
	size_t bulk_size=100;
	int num_src_thread=1; //default as 1
	int num_sink_thread=1; //default as 1
	int num_file_guider=0; //default as 0
	int duration = 60;

	//std::string filepath= "/oasis/scratch/comet/stingw/temp_project/";
	std::string filepath= "/tmp/";
	std::string datapath;
	std::string slurm_topology_addr;

	utility::parse_cli_arguments(argc, argv,
		utility::cli_argument_pack()
		.arg(duration, "-t", "how long should the code run")
        .arg(num_src_thread, "-src", "the number of source")
        .arg(num_sink_thread, "-sink", "the number of sink")
        .arg(num_file_guider, "-fg", "the number of file guider")
        .arg(bulk_size, "-bs", "the size of a bulk")
        .arg(datapath, "-d", "SSD or Data Oasis")
        //.arg(slurm_topology_addr, "-host", "hostname from env var")
        //.positional_arg(inputFileName, "input_filename", "input file name")
	);

	/*size_t queue_size = 100;
	KVTuple kvr2;
	

	CircularQueue cq(queue_size);
	MemoryPool mpool(queue_size*2, size);

	for(int i=0;i<queue_size;i++){
		KVTuple kvr(header_size, key_size, val_size, mpool.acquireMemBlock());
		cq.spin_push(std::move(kvr));
	}
	for(int i=0;i<queue_size;i++){
		cq.spin_pop(kvr2);
		mpool.releaseMemBlock();
	}*/

	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];
	time (&rawtime);
	timeinfo = localtime(&rawtime);

	strftime(buffer,sizeof(buffer),"%Y:%m:%d:%H:%M",timeinfo);
	std::string time_str(buffer);
	std::cout << "The Time is:"+time_str <<"\n";

	auto console = spdlog::stdout_color_mt("console");
	spdlog::set_pattern("%v");
	console->info("spdlog console logger starts");

	auto queue_logger = spdlog::basic_logger_mt("queue_logger", filepath+"queue_"+time_str+"_th_"+std::to_string(num_sink_thread)+"."+datapath);
	if(num_file_guider>0){
		auto g_logger = spdlog::basic_logger_mt("guide_logger", filepath+"guide_"+time_str+"_th_"+std::to_string(num_sink_thread)+"."+datapath);
	}
	else{
		std::cout << "No Guider!!!!!!!"<<"\n";
	}
	if(num_sink_thread>0)
		auto sink_logger = spdlog::basic_logger_mt("sink_logger", filepath+"sink_"+time_str+"_th_"+std::to_string(num_sink_thread)+"."+datapath);
	if(num_src_thread>0)
		auto src_logger = spdlog::basic_logger_mt("source_logger", filepath+"src_"+time_str+"_th_"+std::to_string(num_sink_thread)+"."+datapath);

	size_t queue_size = 1000;
	size_t size = header_size+key_size+val_size;
	std::shared_ptr<CircularQueue> GuideQueue( new CircularQueue(queue_size) );
	std::shared_ptr<StageTracker> tracker( new StageTracker(num_src_thread, 1));
	//std::shared_ptr<MemoryPool> mpool( new MemoryPool(queue_size*2, size));

	//auto ptr =mpool.get();
	//MemoryPool* ptr = new MemoryPool(queue_size*2, size);
	//KVTuple kvr(header_size, key_size, val_size, ptr->acquireMemBlock());
	//ptr->releaseMemBlock();

    KVSource source_gen(0, key_size, val_size, GuideQueue, tracker, nullptr);
    source_gen.setHeaderSize(10);
	KVSink sink(0, GuideQueue, tracker, nullptr, 1);

	source_gen.startSource();
	sink.startMultiSink();

	std::this_thread::sleep_for(std::chrono::seconds(duration));

	source_gen.stopSource();

	return 0;
}
