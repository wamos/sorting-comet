#ifndef KV_TUPLE_H
#define KV_TUPLE_H

//#include <stdint.h>
#include <cstdint>
//#include <stdlib.h>

class KVTuple{
	public:
	KVTuple();	
    KVTuple(const KVTuple& other);	
    KVTuple(KVTuple&& other);
	~KVTuple();
    KVTuple& operator= (const KVTuple& other);
	KVTuple& operator= (KVTuple&& other);

	void initRecord(size_t _size);
	
	inline char getKey(int index) const{
		return buffer[index];
	}

	inline char getValue(int index) const{
		return buffer[index+10];
	}

	inline char* getBuffer() const{
		return buffer;
	}

	inline void setKeyWithIndex(char k, int index){
		buffer[index] = k;
	}

	inline void setValueWithIndex(char v, int index){
		buffer[index+10] = v;
	}

	inline void setBuffer(char* buf){
		buffer=buf;
	}

	/*inline void markLast(){
		isLast=true;
	}

	inline bool checkLast() const{
		return isLast;
	}*/

	inline void setTag(int t){
		tag=t;
	}
	inline int getTag() const{
		return tag;
	}

	inline int getSize() const{
		return size;
	}

	inline void setSize(int _size){
		size = _size;
	}

	static void destroyKV(const KVTuple& kvr) {
		char* buf = kvr.getBuffer();
		delete [] buf;
	}

  	//[ uint64_t keySize, uint32_t valueSize, uint8_t* key, uint8_t* value ]
	private:
	char* buffer;
  	uint64_t tag;
  	size_t size;
  	//bool isLast;
  	
  	//int keySize=10;
};

#endif //KV_RECORD_H
