// main.cpp
#include "ThreadPool.h"
#include <iostream>
#include <chrono>

int main() {
    // 创建包含4个工作线程的线程池
    ThreadPool pool(4);
    
    // 添加8个任务
    for(int i = 0; i < 8; ++i) {
        pool.enqueue([i] {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::cout << "Task " << i << " executed by thread " 
                      << std::this_thread::get_id() << std::endl;
        });
    }

    // 主线程等待用户输入，保持程序运行
    std::cout << "All tasks submitted. Press Enter to exit..." << std::endl;
    std::cin.get();

    return 0;
}