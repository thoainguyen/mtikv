# Use goreman to run `go get github.com/mattn/goreman`

# PASS
# mtikv1: ./mtikv --raft-id 1,1 --data-dir mtikv1 --raft-group 1,2 --peers http://127.0.0.1:12379,http://127.0.0.1:12389;http://127.0.0.1:12359,http://127.0.0.1:12369 --host :12345
# mtikv2: ./mtikv --raft-id 2,2 --data-dir mtikv2 --raft-group 2,1 --peers http://127.0.0.1:12359,http://127.0.0.1:12369;http://127.0.0.1:12379,http://127.0.0.1:12389 --host :12346

# PASS
# mtikv1: ./mtikv --raft-id 1 --data-dir mtikv1 --raft-group 1 --peers http://127.0.0.1:12379,http://127.0.0.1:12389,http://127.0.0.1:12399 --host :12345
# mtikv2: ./mtikv --raft-id 2 --data-dir mtikv2 --raft-group 1 --peers http://127.0.0.1:12379,http://127.0.0.1:12389,http://127.0.0.1:12399 --host :12346
# mtikv3: ./mtikv --raft-id 3 --data-dir mtikv3 --raft-group 1 --peers http://127.0.0.1:12379,http://127.0.0.1:12389,http://127.0.0.1:12399 --host :12347

# PASS
# mtikv1: ./mtikv --raft-id 1   --data-dir mtikv1 --raft-group 1   --peers http://127.0.0.1:12379,http://127.0.0.1:12389 --host :12345
# mtikv2: ./mtikv --raft-id 2,1 --data-dir mtikv2 --raft-group 1,2 --peers http://127.0.0.1:12379,http://127.0.0.1:12389;http://127.0.0.1:12399,http://127.0.0.1:12369 --host :12346
# mtikv3: ./mtikv --raft-id 2,1 --data-dir mtikv3 --raft-group 2,3 --peers http://127.0.0.1:12399,http://127.0.0.1:12369;http://127.0.0.1:12359,http://127.0.0.1:12349 --host :12347
# mtikv4: ./mtikv --raft-id 2   --data-dir mtikv4 --raft-group 3   --peers http://127.0.0.1:12359,http://127.0.0.1:12349 --host :12348

# PASS
mtikv1: ./mtikv --raft-id 1,1    --data-dir mtikv1  --raft-group 1,3     --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:14379,http://127.0.0.1:24394,http://127.0.0.1:34379 --host :12381
mtikv2: ./mtikv --raft-id 2,1,2  --data-dir mtikv2  --raft-group 1,2,3   --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579;http://127.0.0.1:14379,http://127.0.0.1:24394,http://127.0.0.1:34379 --host :22383
mtikv3: ./mtikv --raft-id 2,3    --data-dir mtikv3  --raft-group 2,3     --peers http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579;http://127.0.0.1:14379,http://127.0.0.1:24394,http://127.0.0.1:34379 --host :32380
mtikv4: ./mtikv --raft-id 3,3    --data-dir mtikv4  --raft-group 1,2     --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579 --host :42380
