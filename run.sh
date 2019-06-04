#!/bin/bash

python3 coordinator.py -f lusiadas.txt &
coordinator_pid=$! 

sleep 3

python3 worker.py
worker_pid=$!

kill $worker_pid
kill $coordinator_pid
