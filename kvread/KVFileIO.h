#ifndef KV_IO_H
#define KV_IO_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <errno.h>

//This one is for fread/fopen/fwrite 
#include <cstdio>
#include <cstring>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <memory>
#include <vector>
#include <utility> 
#include "KVTuple.h"

class KVFileIO {
public:
    KVFileIO(uint32_t h_size, uint32_t k_size, uint32_t v_size, uint32_t b_size, std::string fpath);   
    ~KVFileIO();
    KVFileIO(KVFileIO&& other);
    KVFileIO& operator= (KVFileIO&& other);
    KVFileIO(KVFileIO& other)=delete;
    KVFileIO& operator=(const KVFileIO& other)=delete;

    void openInputFiles(std::string input_prefix, int file_num);
    void openOutputFiles(std::string output_prefix, int file_num);
    std::vector<KVTuple> bulkRead(int index);
    bool bulkWrite(std::vector<KVTuple> kv_list, int index);
    //int64_t bulkSend(std::vector<KVTuple> kv_list, int index);
    //std::vector<KVTuple> bulkReceive(int index);

    void readTuple(KVTuple& kvr, int index);
    void writeTuple(const KVTuple& kvr, int index);

    void closeInputFiles();
    void closeOutputFiles();
    int getInputFileNum() const;
    int getOutputFileNum() const;
    size_t getBulkSize() const;
    uint32_t getKeySize() const;
    uint32_t getValueSize() const;
    uint32_t getHeaderSize() const;
    size_t genReadIters(int index);
    inline char* getFileIOBuffer(){
        return fileio_buffer;
    }


private:
    inline uint64_t getNanoSecond(struct timespec tp){
        clock_gettime(CLOCK_MONOTONIC, &tp);
        return (1000000000) * (uint64_t)tp.tv_sec + tp.tv_nsec;
    }

    /*
    A successful return from write() does not make any guarantee that data has been committed to disk. 
    In fact, on some buggy implementations, it does not even guarantee that space has successfully been reserved for the data. 
    The only way to be sure is to call fsync(2) after you are done writing all your data.
    */
    //TODO: fsync of flush
    inline ssize_t writeFile(int fd, char* buffer, uint64_t buf_size){
        ssize_t totalBytes = 0;
        ssize_t write_size  = buf_size;
        char* writeBuffer;
        ssize_t numBytes;
        struct timespec ts1,ts2;
        //uint64_t write_start, write_end, write_all;
        while(write_size > 0){
            writeBuffer = (char*) (buffer + totalBytes);
            //write_start = getNanoSecond(ts1);
            numBytes = write(fd, writeBuffer, write_size);
            //rwrite_end = getNanoSecond(ts2);

            if(numBytes > 0){
                totalBytes = totalBytes + numBytes;
                write_size = write_size - numBytes;
                //std::cout << "ws:" << write_size << "\n";
                //write_all = write_all + (write_end - write_start);              
            }
            else{
                printf("write() error %d: %s", errno, strerror(errno)); 
                std::cout << "nb:" << numBytes << "\n";        
            }
            
        }
        if(totalBytes > 0){
            //std::cout << "w:" << totalBytes << "\n";
            //_logger->info("{0:d}", write_all);
            return totalBytes;
        }
        else{
            return 0;
        }
    }


    // wrap up the C read syscall: ssize_t read(int fd, void *buf, size_t count);
    inline ssize_t readFile(int fd, char* buffer, uint64_t buf_size){
        ssize_t totalBytes = 0;
        ssize_t read_size  = buf_size;
        char* readBuffer;
        ssize_t numBytes;
        struct timespec ts1,ts2;
        //uint64_t read_start, read_end, read_all;
        while(read_size > 0){
            readBuffer = (char*) (buffer + totalBytes);
            //read_start = getNanoSecond(ts1);
            numBytes = read(fd, readBuffer, read_size);
            //read_end = getNanoSecond(ts2);

            if(numBytes > 0){
                totalBytes = totalBytes + numBytes;
                read_size = read_size - numBytes;
                //read_all = read_all + (read_end - read_start);              
            }    
        }
        if(totalBytes > 0){
            //_logger->info("{0:d}", read_all);
            //std::cout << "r:" << totalBytes << "\n";
            return totalBytes;
        }
        else{
            return 0;
        }
    }

    //std::string inputFilename;
    std::string filepath = "/oasis/scratch/comet/stingw/temp_project/";
    std::string outputExtension = ".sort";
    std::string inputExtension = ".input";
    std::string outputPrefix;
    std::string inputPrefix;
    std::vector<std::string> inputFilenames;
    //std::vector<std::FILE*> inFileList;
    std::vector<int> inFileList;
    //std::vector<std::FILE*> outFileList;
    std::vector<int> outFileList;
    //std::ifstream inputStream;
    //std::ofstream outputStream;
    char* fileio_buffer;
    int input_file_num;
    int output_file_num;
    uint32_t header_size=10;
    uint32_t key_size=10;
    uint32_t val_size=90;
    uint32_t record_size=100;
    uint32_t bulk_size=100;
    uint64_t record_count;
    size_t file_size;
    size_t read_iter;
};



    /*void genInputStreams(std::string input_prefix, int file_num){
        input_file_num = file_num;
        inputPrefix = input_prefix;
        
        for(int i=0; i < input_file_num; i++){
            std::string FileName("/tmp/"+inputPrefix + "_" + std::to_string(i)+ inputExtension);
            inputFilenames.push_back(FileName);
            std::ifstream temp(FileName.c_str(), std::ios::out | std::ios::binary);
            inFileStreams.push_back(std::move(temp));
        }
    }
    void genOutputStreams(std::string output_prefix, int file_num){
        output_file_num = file_num;
        outputPrefix = output_prefix;

        for(int i=0; i < output_file_num; i++){
            std::string outputFileName("/tmp/"+outputPrefix + "_" + std::to_string(i)+ outputExtension);
            std::ofstream temp(outputFileName.c_str(), std::ios::out | std::ios::binary);
            outFileStreams.push_back(std::move(temp));
        }
        //std::string inputname("/tmp/"+inputFilename);
        //inputFilename = inputname;
        //std::cout <<"inputname: "<< inputname << "\n";
        //std::ifstream input(inputname.c_str(), std::ios::in | std::ios::binary);
        //inputStream = std::move(input);
    }*/

#endif //KV_IO_H
