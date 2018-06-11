//#include <stdint.h>
//#include <stdlib.h>
//#include <unistd.h>
#include <iostream>
#include <cstdint>
//#include <cstring>
#include <algorithm>
#include "KVTuple.h"
// Ref to get all c'tors right
//https://docs.microsoft.com/en-us/cpp/cpp/move-constructors-and-move-assignment-operators-cpp


KVTuple::KVTuple(){	
	//std::cout << "KV default constructor" <<"\n";
}


void KVTuple::initRecord(size_t _size){
	//std::cout << "kvtuple ctor" << "\n";
	buffer = new char[_size];
	size=_size;
	//std::cout << "init record size:"<< size <<"\n";
}

// Copy constructor
KVTuple::KVTuple(const KVTuple& other)
	: tag(other.tag)
	, size(other.size){
	//std::cout << "KV copy constructor" <<"\n";
	//tag=other.tag;
	//size=other.size;
	//std::cout << "copy record size:"<< size <<"\n";
	//maybe we should avoid C style memcpy in C++11 code
	//std::memcpy(Buffer, other.Buffer, size);
	buffer = new char[size];
	//std::cout << "kvtuple copy ctor" << "\n";
	std::copy(other.buffer, other.buffer+ size, buffer);
}

KVTuple::~KVTuple(){
	//std::cout << "KV destructor tag:"; 
	//std::cout << tag <<"\n";
	//std::cout << "kvtuple dtor" << "\n";
	if (buffer!= nullptr){
		delete [] buffer;
	}
}

// Copy assignment operator
KVTuple& KVTuple::operator= (const KVTuple& other){
	//std::cout << "KV copy assignment" <<"\n";
	//std::cout << "other.tag:" << other.getTag() <<"\n";
	//std::cout << "other.size:" <<  other.getSize() <<"\n";
	//std::cout << "kvtuple copy assign" << "\n";
	if (this != &other){
		delete[] buffer;
		size=other.size;
		tag=other.tag;
		buffer = new char[size];
		std::copy(other.buffer, other.buffer + size, buffer); 
		//std::memcpy(buffer, other.buffer, size);
	}
	return *this;
}

// move constructor
KVTuple::KVTuple(KVTuple&& other)
   : buffer(nullptr)  
   , tag(0)
   , size(0){
   //std::cout << "kvtuple move ctor" << "\n";
   *this = std::move(other);
}

// move assignment
KVTuple& KVTuple::operator= (KVTuple&& other){
	//std::cout << "kvtuple move assign" << "\n";
	if(this!=&other){ // prevent self-move
		buffer= other.buffer;
		tag=other.tag;
		size=other.size;
		//clear the pointer and initailized all values
		other.buffer = nullptr;
		other.tag = 0;
		other.size = 0;
	}
	return *this;
}
