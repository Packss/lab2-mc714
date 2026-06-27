#! /bin/bash
if [[ "$HOSTNAME" == "node-1" ]]; then
  python3 node.py 1 5000 '{"2": ["10.128.0.3", 5000], "3": ["10.128.0.4", 5000]}'
elif [[ "$HOSTNAME" == "node-2" ]]; then
  python3 node.py 2 5000 '{"1": ["10.128.0.2", 5000], "3": ["10.128.0.4", 5000]}'
elif [[ "$HOSTNAME" == "node-3" ]]; then
  python3 node.py 3 5000 '{"1": ["10.128.0.2", 5000], "2": ["10.128.0.3", 5000]}'
else
  echo "host nao conhecido"
fi
