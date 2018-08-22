#include <algorithm>
#include "KVTuple.h"

// Ref to get all c'tors right
//https://docs.microsoft.com/en-us/cpp/cpp/move-constructors-and-move-assignment-operators-cpp
KVTuple::KVTuple()
: header(nullptr),
  size(0){
	//std::cout << "kvtuple empty ctor" <<"\n";
}

KVTuple::KVTuple(uint32_t header_size, uint32_t key_size, uint32_t val_size)
:size(header_size + key_size + val_size){
	//std::cout << "kvtuple param ctor" << "\n";
	header = new char[size];
	//buffer = header + header_size;
	uint16_t isLast=0; // since optional is not using now, we just copt uint16_t instead of uint8_t
    std::copy((char*)&isLast,(char*)&isLast + sizeof(uint16_t), header);
    std::copy((char*)&key_size,(char*)&key_size + sizeof(uint32_t), header+2);
    std::copy((char*)&val_size,(char*)&val_size + sizeof(uint32_t), header+6);

}

KVTuple::KVTuple(uint32_t header_size, uint32_t key_size, uint32_t val_size, char* mem)
:size(header_size + key_size + val_size){
	//std::cout << "kvtuple pre-alloc ctor" << "\n";
	header = mem;
	uint16_t isLast=0; // since optional is not using now, we just copt uint16_t instead of uint8_t
    std::copy((char*)&isLast,(char*)&isLast + sizeof(uint16_t), header);
    std::copy((char*)&key_size,(char*)&key_size + sizeof(uint32_t), header+2);
    std::copy((char*)&val_size,(char*)&val_size + sizeof(uint32_t), header+6);

}

// Copy constructor
KVTuple::KVTuple(const KVTuple& other)
	: size(other.size){
	//std::cout << "kvtuple copy ctor" << "\n";
	//maybe we should avoid C style memcpy in C++11 code
	//std::memcpy(Buffer, other.Buffer, size);
	header = new char[size];
	//buffer = header + 10; // hard-coded size 10
	std::copy(other.header, other.header+ size, header);
}

KVTuple::~KVTuple(){
	//std::cout << "kv dtor" << "\n";
	//for mempool 
	//header = nullptr; 
	//for regular dtor
	if (header!= nullptr){ 
		//std::cout << "mem del" << "\n";
		delete [] header;
	}
}

// Copy assignment operator
KVTuple& KVTuple::operator= (const KVTuple& other){
	//std::cout << "kvtuple copy assign" << "\n";
	if (this != &other){
		delete[] header;
		size = other.size;
		header = new char[size]; 
		//buffer = header + 10;
		std::copy(other.header, other.header + size, header); 
		//std::memcpy(buffer, other.buffer, size);
	}
	return *this;
}

// move constructor
KVTuple::KVTuple(KVTuple&& other) noexcept
   : header(nullptr),
   size(0){
   //std::cout << "kvtuple move ctor" << "\n";
   *this = std::move(other);
}

// move assignment
KVTuple& KVTuple::operator= (KVTuple&& other) noexcept{
	//std::cout << "kvtuple move assign" << "\n";
	if(this!=&other){ // prevent self-move
		header= other.header;
		size=other.size;
		//clear the pointer and initailized all values
		other.header = nullptr;
		other.size = 0;
	}
	return *this;
}
