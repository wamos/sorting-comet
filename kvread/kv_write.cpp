#include <thread>
#include <iostream>
#include <fstream>
#include <tuple>
#include <utility>
#include "json.hpp"
#include "utility.h"
/*---------------*/
#include "ConcurrentQueue.h"
#include "KVTuple.h"
#include "KVReader.h"
#include "KVWriter.h"
#include "KVSink.h"
#include "Socket.h"
#include "FileGuider.h"
#include "spdlog/spdlog.h"

#define SPDLOG_TRACE_ON
#define SPDLOG_DEBUG_ON

using json = nlohmann::json;

int main(int argc, char* argv[]) {

	//std::string inputFilePrefix[4] = {"test0_100mb", "test1_100mb", "test2_100mb", "test3_100mb"};
	//std::string inputFilePrefix[4] = {"test100", "test100_0", "test100_1", "test100_2"};
	//std::string inputFilePrefix[4] = {"100mb", "100mb_0", "100mb_1", "100mb_2"};
	//std::string outputFilePrefix("kv");
	size_t record_size= 0;
	int output_files_per_thread=0;
	int input_files_per_thread=0;
	int num_read_thread=0;
	int num_write_thread=0;
	int node_status=0;
	std::string filepath; //= "/oasis/scratch/comet/stingw/temp_project/";
	std::string shared_config;
	std::string host_config;

	utility::parse_cli_arguments(argc, argv,
		utility::cli_argument_pack()
        //.arg(Kbytes, "-b", "\t buffer size in KB")
        //.arg(sort_type, "-s", "\t type of sort")
		//.arg(machine_name, "-m", "the machine name")
        .arg(node_status, "-n", "the node is either sender or receiver")
        .arg(shared_config, "-scf", "shared config file name")
        .arg(host_config, "-hcf", "per host config file name")
        //.positional_arg(inputFileName, "input_filename", "input file name")
        );

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
	spdlog::set_pattern("[%H:%M:%S %F] [thread %t] %v");
	console->info("spdlog console logger starts");
	auto ar_logger = spdlog::basic_logger_mt("kvreader_logger", filepath+"read_"+time_str+"_kv_"+std::to_string(bytes)+".test");
	ar_logger->info("reader logger starts");
	//auto aw_logger = spdlog::basic_logger_mt("async_writer_logger", "/tmp/write_"+time_str+"_buffer_"+std::to_string(Kbytes)+".test");
	//aw_logger->info("async_writer logger starts");
	//auto sock_logger = spdlog::basic_logger_mt("socket_logger", "/tmp/sock_"+time_str+"_buffer_"+std::to_string(Kbytes)+".test");
	//sock_logger->info("socket logger starts");
	

	std::shared_ptr<ConcurrentQueue> GuideQueue( new ConcurrentQueue(0,400) );
	std::cout << "cq is created" << "\n";

	// Test KVReader-Q-KVWriter -> PASS
	/*std::unique_ptr<Socket> r_sock (nullptr);
	std::unique_ptr<KVFileIO> infile_io (new KVFileIO(record_size, filepath));
	infile_io->openInputFiles(input_prefix_list[0], input_files_per_thread);
	KVReader reader(std::move(infile_io), GuideQueue, std::move(r_sock), node_status);

	node_status=2;
	std::unique_ptr<Socket> w_sock (nullptr);
	std::unique_ptr<KVFileIO> outfile_io (new KVFileIO(record_size, filepath));
	outfile_io->openOutputFiles(output_prefix_list[0], output_files_per_thread);
	KVWriter writer(std::move(outfile_io), GuideQueue, std::move(w_sock), node_status);
	writer.setKeyIndex(0);

	reader.submitKVRead();
	writer.startWriterThread();*/

	KVGuide file_guider(GuideQueue, num_read_thread);
	//file_guider.genSplitPoints(num_write_thread*output_files_per_thread);
	file_guider.genSplitPoints(2);

	std::vector<KVReader> reader_list;
	//std::vector<KVWriter> writer_list;

	for(int i=0;i < num_read_thread ;i++){
		std::cout << "\n";
		std::unique_ptr<Socket> r_sock (nullptr);
    	std::cout << "reader_" <<i<<":"<<"\n";

		std::unique_ptr<KVFileIO> file_io (new KVFileIO(record_size, filepath));
		file_io->openInputFiles(input_prefix_list[i], input_files_per_thread);

		KVReader reader(std::move(file_io), GuideQueue, std::move(r_sock), node_status);
		std::cout << "reader push_back\n";
		reader_list.push_back(std::move(reader));
	}

	std::shared_ptr<ConcurrentQueue> SQueue0( new ConcurrentQueue(0,4096) );
	std::shared_ptr<ConcurrentQueue> SQueue1( new ConcurrentQueue(0,4096) );
	file_guider.addWriteQueue(SQueue0);
	file_guider.addWriteQueue(SQueue1);

	KVSink sinker0(SQueue0, 1, 0);
	KVSink sinker1(SQueue1, 1, 1); 

	for(int i=0;i < num_read_thread;i++){
		reader_list[i].submitKVRead();
	}

	file_guider.startGuide();
	sinker0.startSink();
	sinker1.startSink();

	/*node_status = 2;
	for(uint32_t i=0;i < num_write_thread ;i++){
		std::cout << "\n";
		std::unique_ptr<Socket> w_sock (nullptr);
    	std::cout << "reader_" <<i<<":"<<"\n";
    	// Guider needs to have pointrt to write queues
    	std::shared_ptr<ConcurrentQueue> WriteQueue( new ConcurrentQueue(0,4096) );
    	file_guider.addWriteQueue(WriteQueue);

		std::unique_ptr<KVFileIO> file_io (new KVFileIO(record_size, filepath));
		file_io->openOutputFiles(output_prefix_list[i], output_files_per_thread);
		//KVWriter writer(std::move(file_io), WriteQueue, std::move(w_sock), node_status);
		//writer.setKeyIndex(i);
		std::cout << "writer push_back\n";
		//writer_list.push_back(std::move(writer));	
	}*/

	/*for(int i=0;i < num_read_thread;i++){
		reader_list[i].submitKVRead();
	}*/

	/*for(int i=0;i < num_write_thread; i++){
		writer_list[i].startWriterThread();
	}*/


	/*console->info("spdlog console logger ends");*/
	return 0;
}
