#!/usr/bin/env bash
if [ $# -eq 0 ]; then
    echo 'Illegal input, you can choose:'
    echo '  [--coordinator] launch a coordinator'
    echo '  [--server1] launch a server with port 9000'
    echo '  [--server2] launch a server with port 9001'
    echo '  [--client1] launch a client with username 1'
    echo '  [--client2] launch a client with username 2'
    exit
fi

arg0=$1

if [ "$arg0" = "--coordinator" ]; then
    ./coordinator -p 3010
fi

if [ "$arg0" = "--server1" ]; then
    ./tsd -c 1 -s 1 -h 127.0.0.1 -k 3010 -p 9000
fi

if [ "$arg0" = "--server2" ]; then
    ./tsd -c 2 -s 2 -h 127.0.0.1 -k 3010 -p 9001
fi

if [ "$arg0" = "--client1" ]; then
    ./tsc -h localhost -k 3010 -u 1
fi

if [ "$arg0" = "--client2" ]; then
    ./tsc -h localhost -k 3010 -u 2
fi