#include <thread>
#include <iostream>
#include <fstream>
#include <utility>
#include "json.hpp"
#include "utility.h"
/*---------------*/
#include "ConcurrentQueue.h"
#include "KVTuple.h"
#include "KVReader.h"
#include "KVSink.h"
#include "Socket.h"
#include "FileOpCounter.h"
#include "spdlog/spdlog.h"

#define SPDLOG_TRACE_ON
#define SPDLOG_DEBUG_ON

using json = nlohmann::json;

inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}


int main(int argc, char* argv[]) {

	//struct timespec ts1,ts2;
	//uint64_t all_start, all_end;
	//all_start = getNanoSecond(ts1);
	//std::string inputFilePrefix[4] = {"test100", "test100_0", "test100_1", "test100_2"};
	//std::string inputFilePrefix[4] = {"100mb", "100mb_0", "100mb_1", "100mb_2"};
	size_t record_size= 0;
	size_t bulk_size=0;
	int output_files_per_thread=0;
	int input_files_per_thread=0;
	int num_read_thread=0;
	int num_write_thread=0;
	int node_status=0;
	uint64_t queue_size=0;
	std::string filepath; //= "/oasis/scratch/comet/stingw/temp_project/";
	std::string shared_config;
	std::string host_config;
	std::string slurm_topology_addr;

	utility::parse_cli_arguments(argc, argv,
		utility::cli_argument_pack()
        //.arg(Kbytes, "-b", "\t buffer size in KB")
        //.arg(sort_type, "-s", "\t type of sort")
		//.arg(machine_name, "-m", "the machine name")
        .arg(node_status, "-n", "the node is either sender or receiver")
        .arg(shared_config, "-sc", "shared config file name")
        .arg(host_config, "-hc", "per host config file name")
        .arg(slurm_topology_addr, "-host", "hostname from env var")
        //.positional_arg(inputFileName, "input_filename", "input file name")
        );
	std::string::size_type n;
	n = slurm_topology_addr.rfind(".");
	std::string hostname =slurm_topology_addr.substr(n+1);
	std::cout<<hostname<<"\n";

	json shared_json, host_json;
	shared_config = "../config/"+shared_config;
	host_config   = "../config/"+host_config;
	std::ifstream sharedConfigStream(shared_config.c_str(), std::ios::in | std::ios::binary);
	std::ifstream hostConfigStream(host_config.c_str(), std::ios::in | std::ios::binary);
	sharedConfigStream >> shared_json;
	hostConfigStream >> host_json;
	std::string::size_type sz;

	std::cout << "shared setting:"<< "\n";
	for (json::iterator it = shared_json.begin(); it != shared_json.end(); ++it) {
		std::cout << it.key() << ":";
		if(it.key() == "record_size"){
			std::string rs = it.value();
			record_size = (size_t) std::stoul(rs, &sz);
			std::cout << record_size << "\n";
		}
		else if (it.key() == "bulk_size"){
			std::string bs = it.value();
			bulk_size = (size_t) std::stoul(bs, &sz);
			std::cout << bulk_size << "\n";
		}
		else if (it.key() == "read_threads"){
			std::string rt = it.value();
			num_read_thread = std::stoi(rt, &sz);
			std::cout << num_read_thread << "\n";
		}
		else if (it.key() == "write_threads"){
			std::string wt = it.value();
			num_write_thread = std::stoi(wt, &sz);
			std::cout << num_write_thread << "\n";
		}
		else if (it.key() == "input_files_per_thread"){
			std::string ifpt = it.value();
			input_files_per_thread = std::stoi(ifpt, &sz);
			std::cout << input_files_per_thread << "\n";
		}
		else if (it.key() == "output_files_per_thread"){
			std::string ofpt = it.value();
			output_files_per_thread = std::stoi(ofpt, &sz);
			std::cout << output_files_per_thread << "\n";
		}
		else if(it.key()== "filepath"){
			filepath = it.value();
			std::cout << filepath << "\n";
		}
		else{
			std::cout << it.value() << ",";
			std::cerr << "error parsing json" << "\n";
		}

	}

	std::cout << "host setting:"<< "\n";
	std::vector<std::string> input_prefix_list;
	std::vector<std::string> output_prefix_list;
	for (json::iterator it = host_json.begin(); it != host_json.end(); ++it) {
		json json_obj = *it;
		std::string input_prefix;
		std::string output_prefix;
		for (json::iterator itr = json_obj.begin(); itr != json_obj.end(); ++itr) {
                //std::cout << itr.key()<<":"<< itr.value()<< "\n";
                if(itr.key() == "input_prefix"){
                    input_prefix = itr.value();
                    input_prefix_list.push_back(input_prefix);
                    std::cout << input_prefix<<"\n";
                }
                else if(itr.key() == "output_prefix"){
                    output_prefix = itr.value();
                    output_prefix_list.push_back(output_prefix);
                    std::cout << output_prefix<<"\n";
                }
                else{
					std::cerr << "error parsing json" << "\n";
				}
		}
	}


	uint32_t bytes = record_size;
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];
	time (&rawtime);
	timeinfo = localtime(&rawtime);

	strftime(buffer,sizeof(buffer),"%Y:%m:%d:%H:%M",timeinfo);
	std::string time_str(buffer);
	std::cout << "The Time is:"+time_str <<"\n";

	auto console = spdlog::stdout_color_mt("console");
	//spdlog::set_pattern("[%H:%M:%S %F] [thread %t] %v");
	//spdlog::set_pattern("[tid %t] %v");
	//spdlog::set_pattern("%t %v");
	spdlog::set_pattern("%v");
	console->info("spdlog console logger starts");
	//auto ar_logger = spdlog::basic_logger_mt("kvreader_logger", filepath+"read_"+time_str+"_kv_"+std::to_string(bytes)+".test");
	auto ar_logger = spdlog::basic_logger_mt("kvreader_logger", filepath+"read_"+time_str+"_th_"+std::to_string(num_read_thread)+".test");
	//auto queue_logger = spdlog::basic_logger_mt("queue_logger", filepath+"queue_"+time_str+"_th_"+std::to_string(num_read_thread)+".testing");
	//auto sink_logger = spdlog::basic_logger_mt("sink_logger", filepath+"sink_"+time_str+"_th_"+std::to_string(num_read_thread)+".testing");
	//auto ar_logger = spdlog::basic_logger_mt("kvreader_logger", filepath+"read_"+time_str+"_th_"+std::to_string(num_read_thread)+"_host_"+hostname+".test");
	//ar_logger->info("switch.switch.node:"+slurm_topology_addr);
	//ar_logger->info("reader_logger_starts");
	//queue_logger->info("queue_logger_starts");
	//sink_logger->info("sink_logger_starts");
	//auto aw_logger = spdlog::basic_logger_mt("async_writer_logger", "/tmp/write_"+time_str+"_buffer_"+std::to_string(Kbytes)+".test");
	//aw_logger->info("async_writer logger starts");
	//auto sock_logger = spdlog::basic_logger_mt("socket_logger", "/tmp/sock_"+time_str+"_buffer_"+std::to_string(Kbytes)+".test");
	//sock_logger->info("socket logger starts");
	
	//Test reading threads with a single sink
	queue_size = 64; //48000000
	std::shared_ptr<ConcurrentQueue> KVQueue( new ConcurrentQueue(0,queue_size) );
	//ar_logger->info("queue capacity {0:d}", queue_size);
	//ar_logger->info("cq is created");
	//std::cout << "cq is created" << "\n";

	std::shared_ptr<FileOpCounter> filecount( new FileOpCounter(num_read_thread, input_files_per_thread));

	uint32_t total_iters=0;
	std::vector<KVReader> reader_list;
	for(int i=0;i < num_read_thread ;i++){
		std::cout << "\n";
		std::unique_ptr<Socket> r_sock (nullptr);
    	std::cout << "reader_" <<i<<":"<<"\n";

		std::unique_ptr<KVFileIO> file_io (new KVFileIO(record_size, bulk_size, filepath));
		file_io->openInputFiles(input_prefix_list[i], input_files_per_thread);
		/*for(int file_index = 0; file_index< input_files_per_thread; file_index++){
        	uint32_t iters =  (uint32_t) kv_io->genReadIters(file_index);
        	total_iters = total_iters + iters;
        }*/

		KVReader reader(std::move(file_io), KVQueue, filecount, std::move(r_sock), node_status);
		std::cout << "reader push_back\n";
		reader_list.push_back(std::move(reader));
	}

	for(int i=0;i < num_read_thread;i++){
		reader_list[i].submitKVRead();
	}

	KVSink sink0(KVQueue, filecount, num_read_thread, 0);
	//KVSink sink1(KVQueue, filecount, num_read_thread, 1);
	//sink0.startSink();
	sink0.startMultiSink();
	//sink1.startMultiSink();
	//reader_list.clear();


	//Test KVFileIO alone
	/*uint32_t loop_counter=0;
	std::cout << "read loop counter:" <<  +loop_counter << "\n"; 
	for(int i=0;i < num_read_thread ;i++){
		std::unique_ptr<KVFileIO> file_io (new KVFileIO(record_size, filepath));	
		file_io->openInputFiles(input_prefix_list[i], input_files_per_thread);
		int file_num = file_io->getInputFileNum();
    	for(int file_index = 0; file_index< file_num; file_index++){
        	uint32_t iters =  (uint32_t) file_io->genReadIters(file_index);
        	std::cout << "read iters:" << iters << "," << std::this_thread::get_id() << "\n";
        	for(uint32_t j = 0; i< iters; j++){
        		std::cout << loop_counter << "\n";       
            	KVTuple kvr;
            	//kvr.initRecord(record_size);
            	file_io->readTuple(kvr,file_index);
            	loop_counter++;
        	}
        	std::cout << "read loop counter:" <<  +loop_counter << "\n";       
    	}
    }*/	
	/*console->info("spdlog console logger ends");*/
    //all_end = getNanoSecond(ts2);
    //uint64_t alltime =all_end -all_start;	
    //std::cout << "overall running time"<< +alltime << "\n";
    //read_logger->info("overall running time {0:d}", all_end - all_start);

	return 0;
}
