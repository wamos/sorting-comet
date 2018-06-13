#ifndef KV_IO_H
#define KV_IO_H

#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>

#include <iostream>
//#include <fstream>
#include <cstdio>
#include <string>
#include <memory>
#include <utility> 
#include "KVTuple.h"

class KVFileIO {
public:
    KVFileIO(size_t r_size, std::string fpath)
        : record_size(r_size)
        ,record_count(0)
        ,filepath(fpath){
    } 
   
    ~KVFileIO(){
        std::cout << "kvio dtor" << "\n";
        if(inFileList.size() > 0){
            for(int i=0; i < inFileList.size(); i++){
                std::fclose(inFileList[i]);
                inFileList[i] = nullptr;
            }
            inFileList.clear();
        }   
        if(outFileList.size() > 0){
            for(int i=0; i < outFileList.size(); i++){
                std::fclose(outFileList[i]);
                outFileList[i] = nullptr;
            }
            outFileList.clear();
        }
    }  

    KVFileIO(KVFileIO&& other){
        std::cout << "kvio move ctor" << "\n";
        *this = std::move(other);
    }

    KVFileIO& operator= (KVFileIO&& other){
        std::cout << "kvio move assign" << "\n";
        if(this!=&other){ // prevent self-move
            //Move it move it
            if(input_file_num > 0){
                inFileList  = std::move(other.inFileList);
                inputPrefix = other.inputPrefix;
                inputFilenames = other.inputFilenames;
                input_file_num = other.input_file_num;
                record_count= other.record_count;
                //clean the shit
                for(int i=0; i < input_file_num; i++){
                    inFileList[i] = nullptr;
                }
                other.inFileList.clear();
                other.inputFilenames.clear();
            }

            if(output_file_num > 0){
                outFileList = std::move(other.outFileList);
                outputPrefix   = other.outputPrefix;
                output_file_num = other.output_file_num;
                //clean the shit
                for(int i=0; i < output_file_num; i++){
                    outFileList[i] = nullptr;
                }
                other.outFileList.clear();
            }

        }
        return *this;
    }

    KVFileIO(KVFileIO& other)=delete;
    KVFileIO& operator=(const KVFileIO& other)=delete;

    void openInputFiles(std::string input_prefix, int file_num){
        input_file_num=file_num;
        for(int i=0; i < input_file_num; i++){
            std::string FileName(filepath + input_prefix + "_" + std::to_string(i)+ inputExtension);
            std::cout  << FileName << "\n";
            inputFilenames.push_back(FileName);
            const char* filestr=FileName.c_str();
            std::FILE* fp=std::fopen(filestr,"rb");
            inFileList.push_back(fp);
        }
    }

    void openOutputFiles(std::string output_prefix, int file_num){
        output_file_num=file_num;
        for(int i=0; i < output_file_num; i++){
            std::string FileName(filepath + output_prefix + "_" + std::to_string(i)+ outputExtension);
            const char* filestr=FileName.c_str();
            std::FILE* fp=std::fopen(filestr,"wb");
            outFileList.push_back(fp);
        }
    }

    void readTuple(KVTuple& kvr, int index ) {
        char* bufptr = kvr.getBuffer();
        std::FILE* fp = inFileList[index];
        std::fread(bufptr, sizeof(char), record_size, fp);
        kvr.setTag(record_count);
        //std::cout<< "r:" << kvr.getTag() <<"\n";
        record_count++;
    }

    void writeTuple(const KVTuple& kvr, int index) {
        /*std::cout<< "w:" << kvr.getTag() <<":";
        for(uint32_t j = 0; j < 10; j++){
            uint8_t k = (uint8_t) kvr.getKey(j);
            std::cout << +k << " ";
        }
        std::cout << "\n";*/
        char* bufptr = kvr.getBuffer();
        FILE* fp = outFileList[index];
        std::fwrite(bufptr, sizeof(char), record_size, fp);
        // you need to close the ifstream/ofstream/fstream after the writers are destructed
    }

    void closeInputFiles(){
        if(input_file_num > 0){
            for(int i=0; i < input_file_num; i++){
                std::fclose(inFileList[i]);
                inFileList[i] = nullptr;
            }
            inFileList.clear();
        } 
    }

    void closeOutputFiles(){
        if(output_file_num > 0){
            for(int i=0; i < output_file_num; i++){
                std::fclose(outFileList[i]);
                outFileList[i] = nullptr;
            }
            outFileList.clear();
        }
    }

    int getInputFileNum() const{
        return input_file_num;
    }

    int getOutputFileNum() const{
        return output_file_num;
    }

    size_t getRecordSize() const {
        return record_size;
    }

    size_t genReadIters(int index){
        struct stat stat_buf;
        int rc = stat(inputFilenames[index].c_str(), &stat_buf);
        if(rc == 0){            
            file_size= (size_t) stat_buf.st_size;
            read_iter = file_size/record_size;
            return read_iter;
        }
        else{
            return 0;
        }
    }


private:
    //std::string inputFilename;
    std::string filepath = "/oasis/scratch/comet/stingw/temp_project/";
    std::string outputExtension = ".sort";
    std::string inputExtension = ".input";
    std::string outputPrefix;
    std::string inputPrefix;
    std::vector<std::string> inputFilenames;
    std::vector<std::FILE*> inFileList;
    std::vector<std::FILE*> outFileList;
    //std::ifstream inputStream;
    //std::ofstream outputStream;
    int input_file_num;
    int output_file_num;
    size_t record_size=100;
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
