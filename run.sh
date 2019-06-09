#!/bin/bash

# python3 coordinator.py -f $1 &
# coordinator_pid=$! 

# sleep 1

python3 worker.py --id 1 &
worker1_pid=$!

python3 worker.py --id 2 &
worker2_pid=$!

python3 worker.py --id 3 &
worker3_pid=$!

python3 worker.py --id 4 
worker4_pid=$!

# kill $coordinator_pid

echo 'DIFF'
diff output.csv $1.csv | grep -e --- | wc -l
