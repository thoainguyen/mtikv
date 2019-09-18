# Use goreman to run `go get github.com/mattn/goreman`

# mtikv1: ./mtikv --id 1,1    --dir mtikv1  --cluster 1,3     --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:14379,http://127.0.0.1:24379,http://127.0.0.1:34379 --port 12380
# mtikv2: ./mtikv --id 2,1,2  --dir mtikv2  --cluster 1,2,3   --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579;http://127.0.0.1:14379,http://127.0.0.1:24379,http://127.0.0.1:34379 --port 22383
# mtikv3: ./mtikv --id 2,3    --dir mtikv3  --cluster 2,3     --peers http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579;http://127.0.0.1:14379,http://127.0.0.1:24379,http://127.0.0.1:34379 --port 32380
# mtikv4: ./mtikv --id 3,3    --dir mtikv4  --cluster 1,2     --peers http://127.0.0.1:12389,http://127.0.0.1:22579,http://127.0.0.1:32389;http://127.0.0.1:13379,http://127.0.0.1:23579,http://127.0.0.1:33579 --port 42380

mtikv1: ./mtikv --id 1 --dir mtikv1 --cluster 1 --peers http://127.0.0.1:12379,http://127.0.0.1:52379,http://127.0.0.1:32379 --port 12345
mtikv2: ./mtikv --id 2 --dir mtikv2 --cluster 1 --peers http://127.0.0.1:12379,http://127.0.0.1:52379,http://127.0.0.1:32379 --port 12345
mtikv3: ./mtikv --id 3 --dir mtikv3 --cluster 1 --peers http://127.0.0.1:12379,http://127.0.0.1:52379,http://127.0.0.1:32379 --port 12345