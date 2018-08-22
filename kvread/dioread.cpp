#include <errno.h>
#include <time.h>
//#include <cstdio>
#include <string>
//for raw open and read
#include <unistd.h>
#include <fcntl.h>
//------//
#include "spdlog/spdlog.h"

const int BUF_SIZE = 100;
std::shared_ptr<spdlog::logger> read_logger;

inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
}

// wrap up the C read syscall: ssize_t read(int fd, void *buf, size_t count);
/*inline ssize_t readFile(int fd, char* buffer, uint64_t buf_size){
	ssize_t totalBytes = 0;
  	ssize_t read_size  = buf_size;
  	char* readBuffer;
  	ssize_t numBytes;
  	struct timespec ts1,ts2;
  	uint64_t read_start, read_end, read_all;
	while(read_size > 0){
    	readBuffer = (char*) (buffer + totalBytes);
    	read_start = getNanoSecond(ts1);
    	numBytes = read(fd, readBuffer, read_size);
    	read_end = getNanoSecond(ts2);

    	if(numBytes > 0){
      		totalBytes = totalBytes + numBytes;
      		read_size = read_size - numBytes;
      		read_all = read_all + (read_end - read_start);      		
    	}    
  	}
  	if(totalBytes > 0){
    //_logger->info("sock_return");
  		read_logger->info("{0:d}", read_all);
    	return totalBytes;
  	}
  	else{
    //_logger->info("sock_stall");
  		return 0;
  	}
}*/

size_t genReadIters(std::string filename){
	//std::cout <<"filename:"<< inputFilenames[index] << "\n";
	struct stat stat_buf;
	size_t file_size, read_iter;
	int rc = stat(filename.c_str(), &stat_buf);
	if(rc == 0){            
		file_size= (size_t) stat_buf.st_size;
		read_iter = file_size/BUF_SIZE;
		return read_iter;
	}
    else{
    	return 0;
	}
}


int main()
{
  	int ret;
  	int counter=0; 
	struct timespec ts1,ts2, ts3, ts4;
    uint64_t all_start, all_end;
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];
	all_start = getNanoSecond(ts3);

	time (&rawtime);
	timeinfo = localtime(&rawtime);
	strftime(buffer,sizeof(buffer),"%Y:%m:%d:%H:%M",timeinfo);
	std::string time_str(buffer);
	//std::cout << "The Time is:"+time_str <<"\n";

	auto console = spdlog::stdout_color_mt("console");
	//spdlog::set_pattern("[%H:%M:%S %F] [thread %t] %v");
	//console->info("program starts");

	spdlog::set_pattern("%v");
	read_logger = spdlog::basic_logger_mt("logger", "/oasis/scratch/comet/stingw/temp_project/max_read_"+time_str+"_size_"+std::to_string(BUF_SIZE)+".test");
 
 	std::string filepath = "/oasis/scratch/comet/stingw/temp_project/256gb5.input";
 	//std::string filepath = "/oasis/scratch/comet/stingw/temp_project/1200mb.input";
 	//std::string filepath = "/oasis/scratch/comet/stingw/temp_project/test100.input";
 	int file_descriptor = open(filepath.c_str(), O_RDONLY);
    //std::FILE* fp = std::fopen(filepath.c_str(), "rb");

	char *buf = new char[BUF_SIZE];

	uint32_t iter = genReadIters(filepath);
	printf("%d\n", iter);

	
	for(uint32_t i=0; i < iter; i++){
		uint64_t read_start=0, read_end=0, read_all=0;
		ssize_t totalBytes = 0;
  		ssize_t read_size  = BUF_SIZE;
  		char* readBuffer;
  		ssize_t numBytes=0;
		while(read_size > 0){
    		readBuffer = (char*) (buf + totalBytes);
    		read_start = getNanoSecond(ts1);
    		numBytes = read(file_descriptor, readBuffer, read_size);
    		read_end = getNanoSecond(ts2);
    		if(numBytes > 0){
      			totalBytes = totalBytes + numBytes;
      			read_size = read_size - numBytes;
      			read_all = read_all + (read_end - read_start);      		
    		}	    
  		}
  		if(totalBytes > 0 && totalBytes == BUF_SIZE){
  			read_logger->info("{0:d}", read_all);
  		}
  		else{
  			read_logger->info("read error");
  		}
		//read_start = getNanoSecond(ts1);
		//std::fread(buf, sizeof(char), BUF_SIZE, fp);
		//read_end = getNanoSecond(ts2);
		//read_logger->info("{0:d}", read_end - read_start);
		//if(counter%10000==0)
		//	printf("%d\n", counter);
		//counter++;
	}
 
 	delete buf;
 	close(file_descriptor);
    //std::fclose(fp);
    all_end = getNanoSecond(ts4);
    spdlog::set_pattern("[%H:%M:%S %F] [thread %t] %v");
    read_logger->info("overall running time {0:d}", all_end - all_start);
    return 0;
}
