#include <thread>
#include <iostream>
#include <utility>
#include "ConcurrentQueue.h"
#include "KVTuple.h"
#include "KVReader.h"
#include "Socket.h"
#include "spdlog/spdlog.h"

#define SPDLOG_TRACE_ON
#define SPDLOG_DEBUG_ON

class KVSink {
public:
    KVSink(std::shared_ptr<ConcurrentQueue> cq)
    :Queue(cq){};

    ~KVSink() {
    	SinkThread.join();
    	std::cout<< "Sink Thread join" <<"\n";
    	//_logger->info("kv read thread joins");
	}

	void startSink(){
		std::cout<< "Sink Thread start" <<"\n";
		 SinkThread = std::thread(&KVSink::sinkKV, this);
	}

    void threadJoin(){
   		if(SinkThread.joinable())
    		SinkThread.join();
    }

    /*inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }*/

private:
    void sinkKV(){
    	auto qptr= Queue.get();
    	while(!qptr->isEmpty()){   	
    		qptr->pop();
    	}
    }
    std::shared_ptr<ConcurrentQueue> Queue;
    std::thread SinkThread;
};

int main(int argc, char* argv[]) {

	//std::string inputFilePrefix("test100");
	std::string inputFilePrefix[4] = {"test0_100mb", "test1_100mb", "test2_100mb", "test3_100mb"};
	//std::string inputFilePrefix[4] = {"test100", "test100_0", "test100_1", "test100_2"};
	//std::string inputFilePrefix[4] = {"100mb", "100mb_0", "100mb_1", "100mb_2"};
	//std::string outputFilePrefix("kv");
	//std::string outputFilePrefix("kv");
	size_t record_size= 100;
	int output_file_num=0;
	int input_file_num=1;
	uint32_t bytes = record_size;
	std::string filepath = "/oasis/scratch/comet/stingw/temp_project/";

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

	std::shared_ptr<ConcurrentQueue> KVQueue( new ConcurrentQueue(0,4096000) );

	std::cout << "cq is created" << "\n";

	uint32_t total_num_record=400;
	/*
	for(int i=0; i < input_file_num; i++){
		uint32_t num =  (uint32_t) keyValueIO.genReadIters(i);
		total_num_record = total_num_record + num;
	}*/

	int node_status=1;
	/*std::unique_ptr<Socket> r_sock (nullptr);
	KVReader reader(keyValueIO, KVQueue, total_num_record, std::move(r_sock), node_status);
	reader.submitKVRead();
	KVSink sinker(KVQueue);
	sinker.startSink();*/


	std::vector<KVReader> reader_list;
	for(int i=0;i <4;i++){
		std::cout << "\n";
    	std::unique_ptr<Socket> r_sock (nullptr);
    	std::cout << "reader_" <<i<<":"<<"\n";
    	std::unique_ptr<KVFileIO> kvio (new KVFileIO(record_size, input_file_num, output_file_num));
		kvio->openInputFiles(inputFilePrefix[i]);
		KVReader reader(std::move(kvio), KVQueue, total_num_record, std::move(r_sock), node_status);
		//reader.submitKVRead();
		std::cout << "reader push_back\n";
		reader_list.push_back(std::move(reader));
	}
	for(int i=0;i <4;i++){
		reader_list[i].submitKVRead();
	}

	KVSink sinker(KVQueue);
	sinker.startSink();

	console->info("spdlog console logger ends");
	return 0;
}
