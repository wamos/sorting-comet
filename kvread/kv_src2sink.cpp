#include <thread>
#include <chrono>
#include <iostream>
#include <utility>
//#include "json.hpp"
#include "utility.h"
/*---------------*/
#include "ConcurrentQueue.h"
#include "StageTracker.h"
#include "KVTuple.h"
#include "KVSink.h"
#include "KVSource.h"
#include "KVGuide.h"
#include "spdlog/spdlog.h"

#define SPDLOG_TRACE_ON
#define SPDLOG_DEBUG_ON

inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}


int main(int argc, char* argv[]) {

	size_t record_size= 100;
	size_t key_size=10;
	size_t val_size=90;
	size_t bulk_size=100;
	int num_src_thread=1; //default as 1
	int num_sink_thread=1; //default as 1
	int num_file_guider=0; //default as 0
	int duration = 60;

	std::string filepath= "/oasis/scratch/comet/stingw/temp_project/";
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

	/*std::string::size_type n;
	n = slurm_topology_addr.rfind(".");
	std::string hostname =slurm_topology_addr.substr(n+1);
	std::cout<<hostname<<"\n";*/

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

	uint64_t queue_size = 1000;
	std::shared_ptr<ConcurrentQueue> GuideQueue( new ConcurrentQueue(0,queue_size) );
	std::shared_ptr<StageTracker> tracker( new StageTracker(num_src_thread, 1));

	KVGuide file_guider(0, GuideQueue, num_src_thread, tracker);
    file_guider.genSplitPoints(num_sink_thread);
    file_guider.setBulkSize(1000);

    std::vector<KVSource> src_list;
    for(int i=0;i < num_src_thread;i++){
    	KVSource source_gen(0, key_size, val_size, GuideQueue, tracker);
    	source_gen.setBulkSize(1000);
    	src_list.push_back(std::move(source_gen));
	}	
    //KVSink sink(0, GuideQueue, tracker, 1); // for 0 guider
    //sink.setBulkSize(100);

	std::vector<KVSink> sink_list;
    for(int i=0;i < num_sink_thread;i++){
		std::cout << "sink_" <<i<<":"<<"\n";
		if(num_file_guider>0){
			std::shared_ptr<ConcurrentQueue> SQueue( new ConcurrentQueue(i+1,queue_size) );
			file_guider.addWriteQueue(SQueue);
			KVSink sink(i, SQueue, tracker, num_file_guider);
			sink.setBulkSize(1000);
			sink_list.push_back(std::move(sink));
		}
		else{
			KVSink sink(i, GuideQueue, tracker, 1);
			sink.setBulkSize(1000);
			sink_list.push_back(std::move(sink));
		}
		std::cout << "sink push_back\n";
	}

	for(int i=0;i < num_src_thread;i++){
		src_list[i].startSource();
	}
	//source_gen.startSource();

	if(num_file_guider>0){
		file_guider.startGuide();
	}

	for(int i=0;i < num_sink_thread;i++){
		sink_list[i].startMultiSink();
	}

	std::this_thread::sleep_for(std::chrono::seconds(duration));

	for(int i=0;i < num_src_thread;i++){
		src_list[i].stopSource();
	}

	//source_gen.stopSource();

	if(num_file_guider==0){
		std::cout << "No Guider!!!!!!!"<<"\n";
	}
	uint64_t qcount = GuideQueue->getPushedTimes()*100;
	std::cout<< "q counter:" << +qcount << "\n";

	return 0;
}