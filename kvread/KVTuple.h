#ifndef KV_TUPLE_H
#define KV_TUPLE_H

#include <iostream>
#include <cstdint>
#include <cstring>

class KVTuple{
	public:
	KVTuple();
	KVTuple(uint32_t header_size, uint32_t key_size, uint32_t val_size) ;
	KVTuple(uint32_t header_size, uint32_t key_size, uint32_t val_size, char* mem);
    KVTuple(const KVTuple& other);	
    KVTuple(KVTuple&& other) noexcept;
	~KVTuple();
    KVTuple& operator= (const KVTuple& other);
	KVTuple& operator= (KVTuple&& other) noexcept;

	//void initRecord(size_t _size);
	inline void initRecord(uint32_t header_size, uint32_t key_size, uint32_t val_size){
		//std::cout << "kvtuple init" << "\n";
		size = header_size + key_size + val_size;
		header = new char[size];
		//buffer = header + header_size;
		uint16_t isLast=0; // since optional is not using now, we just copt uint16_t instead of uint8_t
	    std::copy((char*)&isLast,(char*)&isLast + sizeof(uint16_t), header);
	    std::copy((char*)&key_size,(char*)&key_size + sizeof(uint32_t), header+2);
	    std::copy((char*)&val_size,(char*)&val_size + sizeof(uint32_t), header+6);
		//std::cout << "init record size:"<< size <<"\n";
	}

	inline char* getBuffer(int offset) const{
		// if header_size =10 then the offset should be 10
		return header+offset;
	}
	/* TODO : add header_size to KVFileIO and KVWriter
	[stingw@comet-ln2 kvread]$ grep -rn "getBuffer(" ./*.cpp
	./KVWriter.cpp:221:    char* kv_bufptr = kvr.getBuffer();
	*/

	inline char* getHeader() const{
		return header;
	}
	
	inline char getKey(int index, int offset) const{
		// if header_size =10 then the offset should be 10
		return header[index+offset];
	}

	inline char getValue(int index, int offset) const{
		// if header_size =10 and key_length =10 then the offset should be 20
		return header[index+offset];
	}

	inline void setKeyWithIndex(char k, int index, int offset){
		// if header_size =10 then the offset should be 10
		header[index+offset] = k; 
	}

	inline void setValueWithIndex(char v, int index, int offset){
		// if header_size =10 and key_length =10 then the offset should be 20
		header[index+offset] = v;
	}

	inline bool isLastKV(){
		if( header[0] == 1)
			return true;
		else
			return false;
	}

	uint32_t getKeySize() const{
		uint32_t key_length=0;
	    for(int i=0;i<4;i++){
	        //std::cout << +(uint8_t)header[i+6] << ",";
	        int shift=8*i;
	        uint32_t number = (uint8_t)header[i+2] << shift;
	        //std::cout << +number << "\n";
	        key_length = number + key_length;
	    }
	    return key_length;
	}

	uint32_t getValueSize() const{
		uint32_t val_length=0;
	    for(int i=0;i<4;i++){
	        //std::cout << +(uint8_t)header[i+6] << ",";
	        int shift=8*i;
	        uint32_t number = (uint8_t)header[i+6] << shift;
	        //std::cout << +number << "\n";
	        val_length = number + val_length;
	    }
	    return val_length;
	}

  	//[ uint8_t isLast, uint8_t optional, uint32_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
	private:
	char* header; 
	/*
	the header contains
	uint16_t islast        // 2 bytes
	//it could become [uint8_t islast, uint8_t optional] in the future
  	uint32_t key_length;   // 4 bytes = 10
	uint32_t val_length;   // 4 bytes = 90
  	*/
	//char* buffer;
	uint32_t size;
};

#endif //KV_RECORD_H


/*	
//uint64_t tag;  // 8 byetes
//bool isLast; // 1 byte
//static void destroyKV(const KVTuple& kvr) {
	char* buf = kvr.getBuffer();
	delete [] buf;
}*/
