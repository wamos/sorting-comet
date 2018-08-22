#include <iostream>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include "KVTuple.h"
#include "CircularQueue.h"

int main()
{
    uint32_t key_size=10;
    uint32_t val_size=99980;
    uint32_t header_size=10;
    CircularQueue cq(10);
    int push_counter = 0;
    int pop_counter = 0;

    for(int i=0;i <5;i++){
        KVTuple kvr(header_size, key_size, val_size);
        kvr.setKeyWithIndex(1, 0, header_size);
        bool goodpush = cq.spin_push(std::move(kvr));
        if(goodpush){
            push_counter+=1;
        }
        std::cout<< "end push loop" << "\n";
    }

    for(int i=0;i<5;i++){
        KVTuple kvr2;
        bool goodpop = cq.spin_pop(kvr2);
        if(goodpop){
            pop_counter+=1;
        }
        std::cout<< "end pop loop" << "\n";
    }

    std::cout << "push_counter:" << push_counter << "\n";
    std::cout << "pop_counter:" << pop_counter << "\n";

    return 0;
}
