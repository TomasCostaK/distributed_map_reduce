#!/bin/bash

python3 coordinator.py -f lusiadas.txt &
coordinator_pid=$! 

sleep 3

python3 worker.py

kill $coordinator_pid
