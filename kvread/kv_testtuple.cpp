#include <iostream>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>

int main()
{
    /*char hexBuffer[110]={0};*/
    char* header = new char[110];
    char* buffer =header+10;
    char* copyBuffer = new char[100];
    /*uint32_t n=4294967295;*/
    uint32_t header_size=100;
    uint32_t key_size=10;
    uint32_t val_size=10000; //=2^17+2^8+255, 0 2 1 255
    uint16_t isLast=1;
    //uint8_t opt=0;
    //printf("%d\n", sizeof(size_t));

    /*memcpy((char*)hexBuffer,(char*)&n,sizeof(int));*/
    //std::copy_backward((char*)&isLast,(char*)&isLast + sizeof(uint16_t), header+2);
    //std::copy_backward((char*)&key_size,(char*)&key_size + sizeof(uint32_t), header+6);
    //std::copy_backward((char*)&val_size,(char*)&val_size + sizeof(uint32_t), header+10);
    //std::copy((char*)&isLast,(char*)&isLast + sizeof(uint16_t), header);
    //std::copy((char*)&key_size,(char*)&key_size + sizeof(uint32_t), header+2);
    //std::copy((char*)&val_size,(char*)&val_size + sizeof(uint32_t), header+6);
    std::memcpy(header,(char*)&isLast, sizeof(uint16_t));
    std::memcpy(header+2,(char*)&key_size, sizeof(uint32_t));
    std::memcpy(header+6,(char*)&val_size, sizeof(uint32_t));

    //-> HEX:1 0 10 0 0 0 90 0 0 0
    printf("HEX:");
    for(int i=0;i<10;i++){
        //printf("%" PRIu8 " ",hexBuffer[i]);
        std::cout << +(uint8_t)header[i] << " "; //char is signed on x86!
    }
    printf("\n");


    uint32_t temp=0;
    for(int i=0;i<4;i++){
        //std::cout << +(uint8_t)header[i+6] << ","; //char is signed on x86!
        int shift=8*i;
        uint32_t number = (uint8_t)header[i+6] << shift;
        //std::cout << +number << "\n";
        temp = number+temp;
    }
    std::cout << "temp:" << +temp << "\n";
    printf("\n");

    std::memset(buffer, 1, 100);
    std::copy(buffer, buffer+100, copyBuffer);

    printf("copy:");
    for(int i=0;i<100;i++){
        //printf("%" PRIu8 " ",copyBuffer[i]);
        std::cout << +(uint8_t)copyBuffer[i] << " ";
    }
    printf("\n");
    /*printf("%02X ",hexBuffer[i]);*/

    printf("\n");
}
