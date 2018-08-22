#include "KVFileIO.h"

KVFileIO::KVFileIO(uint32_t h_size, uint32_t k_size, uint32_t v_size, uint32_t b_size, std::string fpath)
	: header_size(h_size)
    , key_size(k_size)
    , val_size(v_size)
	, bulk_size(b_size)
    , record_size(k_size+v_size)
    ,record_count(0)
	,filepath(fpath){
	fileio_buffer = new char[bulk_size*record_size];
} 

KVFileIO::~KVFileIO(){
	std::cout << "kvio dtor" << "\n";
	if(inFileList.size() > 0){
		for(int i=0; i < inFileList.size(); i++){
			close(inFileList[i]);
        }
		inFileList.clear();
	}   
	if(outFileList.size() > 0){
		for(int i=0; i < outFileList.size(); i++){
			close(outFileList[i]);
		}
		outFileList.clear();
	}
	delete fileio_buffer;
}

KVFileIO::KVFileIO(KVFileIO&& other){
	std::cout << "kvio move ctor" << "\n";
	*this = std::move(other);
}

KVFileIO& KVFileIO::operator= (KVFileIO&& other){
    std::cout << "kvio move assign" << "\n";
    if(this!=&other){ // prevent self-move
    	fileio_buffer = other.fileio_buffer;
    	other.fileio_buffer=nullptr;
        record_size = other.record_size;
        header_size = other.header_size;        
        key_size = other.key_size;
        val_size = other.val_size;
        bulk_size =other.bulk_size;
        //Move it move it
        if(input_file_num > 0){
            inFileList  = other.inFileList;
            inputPrefix = other.inputPrefix;
            inputFilenames = other.inputFilenames;
            input_file_num = other.input_file_num;
            record_count= other.record_count;
            //clean the shit
            other.inFileList.clear();
            other.inputFilenames.clear();
            other.record_count=0;
        }

        if(output_file_num > 0){
            outFileList = other.outFileList;
            outputPrefix   = other.outputPrefix;
            output_file_num = other.output_file_num;
            //clean the shit
            other.outFileList.clear();
        }

    }
    return *this;
}

std::vector<KVTuple> KVFileIO::bulkRead(int index){
    std::vector<KVTuple> kv_list;
    int fd = inFileList[index];
	ssize_t filebytes = KVFileIO::readFile(fd, fileio_buffer, bulk_size*record_size);
	size_t bufferBytes=0;
	char* reading_buffer;
	if(filebytes > 0){
		for(uint32_t i=0; i < bulk_size; i++){
			KVTuple kvr;
        	kvr.initRecord(header_size, key_size, val_size);
        	char* kv_bufptr = kvr.getBuffer(header_size);
        	reading_buffer = (char*) (fileio_buffer + bufferBytes);
        	std::copy(reading_buffer, reading_buffer + record_size, kv_bufptr); 
        	bufferBytes = bufferBytes + record_size;
			//kvr.setTag(record_count);
        	kv_list.push_back(kvr);
			record_count++;
    	}   	
	}
	return kv_list;
}

bool KVFileIO::bulkWrite(std::vector<KVTuple> kv_list, int index){
    //for loop copt KVTuple to fileio_buffer
    // then KVFileIO writeFile
    int fd = outFileList[index];
    size_t bufferBytes=0;
    char* writing_buffer;
    for(size_t i=0; i < kv_list.size(); i++){
        KVTuple kvr = kv_list[i];
        char* kv_bufptr = kvr.getBuffer(header_size);
        writing_buffer = (char*) (fileio_buffer + bufferBytes);
        std::copy(kv_bufptr, kv_bufptr + record_size, writing_buffer); 
        bufferBytes = bufferBytes + record_size;
    }

    ssize_t filebytes = KVFileIO::writeFile(fd, fileio_buffer, kv_list.size()*record_size);

    if(filebytes>0)
        return true;
    else
        return false;

}

void KVFileIO::readTuple(KVTuple& kvr, int index) {
    char* bufptr = kvr.getBuffer(header_size);
    int fd = inFileList[index];
    //std::fread(bufptr, sizeof(char), record_size, fp);
    read(fd,bufptr,record_size);
    //kvr.setTag(record_count);
    //std::cout<< "r:" << kvr.getTag() <<"\n";
}

void KVFileIO::writeTuple(const KVTuple& kvr, int index) {
    /*std::cout<< "w:" << kvr.getTag() <<":";
    for(uint32_t j = 0; j < 10; j++){
        uint8_t k = (uint8_t) kvr.getKey(j);
        std::cout << +k << " ";
    }
    std::cout << "\n";*/
    char* bufptr = kvr.getBuffer(header_size);
    int fd = outFileList[index];
    write(fd, bufptr, record_size);
    // you need to close the ifstream/ofstream/fstream after the writers are destructed
}


void KVFileIO::openInputFiles(std::string input_prefix, int file_num){
    input_file_num=file_num;
    for(int i=0; i < input_file_num; i++){
        std::string FileName(filepath + input_prefix + "_" + std::to_string(i)+ inputExtension);
        std::cout  << FileName << "\n";
        inputFilenames.push_back(FileName);
        const char* filestr=FileName.c_str();
        int fd = open(filestr, O_RDONLY);
        //std::FILE* fp=std::fopen(filestr,"rb");
        //std::cout << "rfd:" << fd << "\n";
        inFileList.push_back(fd);
    }
}

void KVFileIO::openOutputFiles(std::string output_prefix, int file_num){
    output_file_num=file_num;
    for(int i=0; i < output_file_num; i++){
        std::string FileName(filepath + output_prefix + "_" + std::to_string(i)+ outputExtension);
        const char* filestr=FileName.c_str();
        //std::FILE* fp=std::fopen(filestr,"wb");
        std::cout << "open outfile:" << FileName <<"\n";
        int fd = open(filestr, O_WRONLY | O_CREAT); // you need to create it!!!
        //printf("open() error %d: %s", errno, strerror(errno)); 
        //std::cout << "wfd:" << fd << "\n";
        outFileList.push_back(fd);
    }
}

void KVFileIO::closeInputFiles(){
    if(input_file_num > 0){
        for(int i=0; i < input_file_num; i++){
            close(inFileList[i]);
        }
        inFileList.clear();
    } 
}

void KVFileIO::closeOutputFiles(){
    if(output_file_num > 0){
        for(int i=0; i < output_file_num; i++){
            close(outFileList[i]);
        }
        outFileList.clear();
    }
}

int KVFileIO::getInputFileNum() const{
    return input_file_num;
}

int KVFileIO::getOutputFileNum() const{
    return output_file_num;
}

uint32_t KVFileIO::getKeySize() const {
    return key_size;
}

uint32_t KVFileIO::getValueSize() const {
    return val_size;
}

uint32_t KVFileIO::getHeaderSize() const {
    return header_size;
}

size_t KVFileIO::getBulkSize() const {
    return (size_t) bulk_size;
}

size_t KVFileIO::genReadIters(int index){
    //std::cout <<"filename:"<< inputFilenames[index] << "\n";
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


