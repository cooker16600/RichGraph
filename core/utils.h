#pragma once

#include <sstream>
#include <sys/stat.h>
#include <vector>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <map>
#include <sys/time.h>
#include <sys/mman.h>

#ifdef _WIN32
#include <direct.h>
#include <stdio.h>
#include <io.h>
#include <windows.h>
#endif
#if defined(__linux__) || defined(__MINGW32__) || defined(__APPLE__)
#include <dirent.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#endif

namespace utils{

    // addr是指向内存区域起始地址的指针。
    // length是内存区域的长度，以字节为单位。
    // advice是一个整数参数，用于指定对内存区域的建议。常用的建议值包括：
    //     MADV_NORMAL：对内存区域没有特殊的建议。
    //     MADV_RANDOM：表明内存区域将被以随机的方式访问。
    //     MADV_SEQUENTIAL：表明内存区域将被以顺序的方式访问。
    //     MADV_WILLNEED：表明内存区域将很快被使用，内核应尽快预读该区域的内容到内存。
    //     MADV_DONTNEED：表明内存区域的内容不再需要，内核可以释放该区域的内存资源。
    static inline void use_madvise(void* addr, int offset, int length,
                                   bool use_byte=false,   // madvice使用byte还是page为粒度
                                   int advice=MADV_NORMAL) {
      if (use_byte == true) {
        // 手动触发预读操作
        madvise(static_cast<char*>(addr) + offset,
                length,
                MADV_WILLNEED);   // 对pagerank友好
      } else {
        // 页面对齐
        size_t fi = offset - offset % 4096;
        size_t to = offset + length;
        size_t ti = to + (4096 - to % 4096);
		madvise(static_cast<char*>(addr) + fi, ti - fi, MADV_WILLNEED);
      }
    }

    /**
     * Check whether directory exists
     * @param path directory to be checked.
     * @return ture if directory exists, false otherwise.
     */
    static inline bool dirExists(std::string path){
        struct stat st;
        int ret = stat(path.c_str(), &st);
        return ret == 0 && st.st_mode & S_IFDIR;
    }

    /**
     * list all filename in a directory
     * @param path directory path.
     * @param ret all files name in directory.
     * @return files number.
     */
    #if defined(_WIN32) && !defined(__MINGW32__) 
    static inline int scanDir(std::string path, std::vector<std::string> &ret){
        std::string extendPath;
        if(path[path.size() - 1] == '/'){
            extendPath = path + "*";
        }
        else{
            extendPath = path + "/*";
        }
        WIN32_FIND_DATAA fd;
        HANDLE h = FindFirstFileA(extendPath.c_str(), &fd);
        if(h == INVALID_HANDLE_VALUE){
            return 0;
        }
        while(true){
            std::string ss(fd.cFileName);
            if(ss[0] != '.'){
                ret.push_back(ss);
            }
            if(FindNextFile(h, &fd) ==false){
                break;
            }
        }
        FindClose(h);
        return ret.size();
    }
    #endif
    #if defined(__linux__) || defined(__MINGW32__) || defined(__APPLE__)
    static inline int scanDir(std::string path, std::vector<std::string> &ret){
        DIR *dir;
        struct dirent *rent;
        dir = opendir(path.c_str());
        char s[100];
        while((rent = readdir(dir))){
            strcpy(s,rent->d_name);
            if (s[0] != '.'){
                ret.push_back(s);
            }   
        }
        closedir(dir);
        return ret.size();
    }
    #endif

    /**
     * Create directory
     * @param path directory to be created.
     * @return 0 if directory is created successfully, -1 otherwise.
     */
    static inline int _mkdir(const char *path){
        #ifdef _WIN32
            return ::_mkdir(path);
        #else
            return ::mkdir(path, 0775);
        #endif
    }

    /**
     * Create directory recursively
     * @param path directory to be created.
     * @return 0 if directory is created successfully, -1 otherwise.
     */
    static inline int mkdir(const std::string &path){
        std::string currentPath = "";
        std::string dirName;
        std::stringstream ss(path);

        while (std::getline(ss, dirName, '/')){
            if (dirName.size() == 0) { // root path
              currentPath += "/";
              continue;
            }
            currentPath += dirName;
            if (!dirExists(currentPath) && _mkdir(currentPath.c_str()) != 0){
                return -1;
            }
            currentPath += "/";
        }
        return 0;
    }

    /**
     * Delete a empty directory
     * @param path directory to be deleted.
     * @return 0 if delete successfully, -1 otherwise.
     */
    static inline int rmdir(const char *path){
        #ifdef _WIN32
            return ::_rmdir(path);
        #else
            return std::filesystem::remove_all(path); // delete non-empty directory
        #endif
    }

    /**
     * Delete a file
     * @param path file to be deleted.
     * @return 0 if delete successfully, -1 otherwise.
     */
    static inline int rmfile(const char *path){
        #ifdef _WIN32
            return ::_unlink(path);
        #else
            return ::unlink(path);
        #endif
    }

    static inline uint64_t EncodeIndex(uint16_t levelID, uint32_t fileID, uint32_t offset) {
        uint64_t encoded = 0;
        encoded |= static_cast<uint64_t>(levelID & 0xF) << 60;
        encoded |= static_cast<uint64_t>(fileID & 0x0FFFFFFF) << 32;
        encoded |= static_cast<uint64_t>(offset & 0xFFFFFFFF);
        return encoded;
    }

    static inline void DecodeIndex(uint64_t encoded, uint16_t& levelID, uint32_t& fileID, uint32_t& offset) {
        levelID = static_cast<uint16_t>((encoded >> 60) & 0xF);
        fileID = static_cast<uint32_t>((encoded >> 32) & 0x0FFFFFFF);
        offset = static_cast<uint32_t>(encoded & 0xFFFFFFFF);
    }

    class SmapsParser {
    public:
        static std::unordered_map<std::string, unsigned long> getMemorySizes(const std::string& pid) {
            std::string smapsPath = "/proc/" + pid + "/smaps";
            std::unordered_map<std::string, unsigned long> memorySizes;

            std::ifstream smapsFile(smapsPath);
            std::string line;
            std::string currentCategory;

            if (smapsFile.is_open()) {
                size_t line_num = 0;
                while (std::getline(smapsFile, line)) {
                    line_num++;
                    if (line.compare(0, 5, "Size:") == 0) {
                        memorySizes["Size"] += getSizeInKb(line);
                    } else if (line.compare(0, 4, "Rss:") == 0) {
                        memorySizes["Rss"] += getSizeInKb(line);
                    } else if (line.compare(0, 13, "Shared_Clean:") == 0) {
                        memorySizes["Shared_Clean"] += getSizeInKb(line);
                    } else if (line.compare(0, 13, "Shared_Dirty:") == 0) {
                        memorySizes["Shared_Dirty"] += getSizeInKb(line);
                    } else if (line.compare(0, 14, "Private_Clean:") == 0) {
                        memorySizes["Private_Clean"] += getSizeInKb(line);
                    } else if (line.compare(0, 14, "Private_Dirty:") == 0) {
                        memorySizes["Private_Dirty"] += getSizeInKb(line);
                    } else if (line.compare(0, 5, "Swap:") == 0) {
                        memorySizes["Swap"] += getSizeInKb(line);
                    } else if (line.compare(0, 4, "Pss:") == 0) {
                        memorySizes["Pss"] += getSizeInKb(line);
                    }
                }
                printf("file line_num=%ld\n", line_num);
                smapsFile.close();
            } else {
                printf("error: Failed to open smaps file.\n");
            }
            return memorySizes;
        }

    private:
        static unsigned long getSizeInKb(const std::string& line) {
            std::string sizeStr = line.substr(line.find_first_of("0123456789"));
            return std::stoul(sizeStr);
        }
    };

    static inline double get_memory_usage() {
        std::ifstream statusFile("/proc/self/status");
        std::string line;

        while (std::getline(statusFile, line)) {
            if (line.compare(0, 6, "VmRSS:") == 0) {
                // 提取 RES 值
                unsigned long resMemory = std::stoul(line.substr(6));
                double res_mem = double(resMemory)/(1024*1024); // GB
#ifndef TEST_MEM_TIME_QPS
              printf("[RES %f GB ]\n", res_mem);
#endif
                return res_mem;
            }
        }
        fprintf(stderr, "not found, RES 0 GB \n");
        return 0;
    }

    // read io info
    static inline void get_io_info(std::string info="") {
        static std::map<std::string, long long> previousDataMap;
        std::string pid = std::to_string(getpid());
        std::string ioStatsFilePath = "/proc/" + pid + "/io";

        std::ifstream file(ioStatsFilePath);
        std::map<std::string, long long> currentDataMap;

        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                size_t delimiterPos = line.find(':');
                if (delimiterPos != std::string::npos) {
                    std::string key = line.substr(0, delimiterPos);
                    std::string valueString = line.substr(delimiterPos + 1);
                    long long value = std::stoull(valueString);
                    currentDataMap[key] = value;
                }
            }

            file.close();

            // print diff value
            for (const auto& entry : currentDataMap) {
                printf(" %s%s(GB): %.2f\n", info.c_str(), 
                        entry.first.c_str(), 
                        entry.second / 1024.0 / 1024 / 1024);
                if (previousDataMap.count(entry.first) > 0) {
                long long diff 
                    = entry.second - previousDataMap[entry.first];
                printf(" @%s%s(GB): %.2f\n", info.c_str(), entry.first.c_str(), 
                        diff / 1024.0 / 1024 / 1024);
                } else {
                printf(" @%s%s(GB): %.2f\n", info.c_str(), 
                        entry.first.c_str(), 
                        entry.second / 1024.0 / 1024 / 1024);
                }
            }

            // 保存当前统计值作为上一次的统计值
            previousDataMap = currentDataMap;
        } else {
            std::cerr << "Failed to open file: " << ioStatsFilePath << std::endl;
        }
    }
    
    inline double GetCurrentTime() {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return tv.tv_sec + tv.tv_usec / 1000000.0;
    }

}
