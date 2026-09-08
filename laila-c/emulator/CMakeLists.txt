# ESP-IDF project that boots laila-C on an emulated ESP32 (QEMU).
# Build/run:  . $IDF_PATH/export.sh && idf.py set-target esp32 && idf.py build qemu
cmake_minimum_required(VERSION 3.16)
include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(laila_boot)
