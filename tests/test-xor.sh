#!/bin/bash

tests/ch-placement-test xor 256 1 100 3
if [ $? -ne 0 ]; then
    exit 1
fi

tests/ch-placement-test xor 256 16 100 3
if [ $? -ne 0 ]; then
    exit 1
fi
