#!/bin/bash
g++ hook.cpp -o hook.so -I/usr/local/cuda/include -lcuda --shared -fPIC
