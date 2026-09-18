#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>

class FileIO {
public:
    FileIO(const std::string& filename) : fd_(-1), filename_(filename) {}

    // 打开文件
    bool Open(int flags, mode_t mode = 0666) {
        fd_ = open(filename_.c_str(), flags, mode);
        return (fd_ != -1);
    }

    // 关闭文件
    void Close() {
        if (fd_ != -1) {
            close(fd_);
            fd_ = -1;
        }
    }

    // 写入数据
    ssize_t Write(const void* data, size_t size) {
        if (fd_ == -1) {
            return -1; // 文件未打开
        }
        return write(fd_, data, size);
    }

    void Fsync() {
        fsync(fd_);
    }

    // 读取数据
    ssize_t Read(void* data, size_t size) {
        if (fd_ == -1) {
            return -1; // 文件未打开
        }
        return read(fd_, data, size);
    }

    // 检查文件是否打开
    bool IsOpen() const {
        return (fd_ != -1);
    }

    // 获取文件描述符
    int GetFileDescriptor() const {
        return fd_;
    }

    // 获取文件名
    std::string GetFileName() const {
        return filename_;
    }

    ~FileIO() {
        // 刷新文件数据到磁盘
        Close(); // 析构时自动关闭文件
    }

private:
    int fd_; // 文件描述符
    std::string filename_;
};