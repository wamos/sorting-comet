 	cmake_minimum_required(VERSION 3.0.0 FATAL_ERROR)
	project(cool_sorting CXX)
    	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread -lrt") #-fno-omit-frame-pointer -fsanitize=address")
	#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread -g -lrt")
	#set(LINKER_FLAGS "-fsanitize=address")
	
	add_executable(kvread kv_read.cpp KVTuple.cpp Socket.cpp KVReader.cpp ConcurrentQueue.cpp)
	#add_executable(kvio kv_sender.cpp KVTuple.cpp Socket.cpp AsyncKVReader.cpp AsyncKVWriter.cpp)
    	target_include_directories(kvread PRIVATE spdlog)

