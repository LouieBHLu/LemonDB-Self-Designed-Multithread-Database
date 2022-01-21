//
// Created by lu, 2021/11/3
//

#include "thread.h"
static long thread_num;

void setThreadNum(long n){
    thread_num=n;
}

long getThreadNum(){
    return thread_num;
}