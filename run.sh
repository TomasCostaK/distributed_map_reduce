#!/bin/bash

# python3 coordinator.py -f $1 &
# coordinator_pid=$! 

# sleep 1

python3 worker.py &
worker1_pid=$!

python3 worker.py &
worker2_pid=$!

python3 worker.py &
worker3_pid=$!

python3 worker.py
worker4_pid=$!

# kill $worker_pid
# kill $coordinator_pid
