file='/media/user/data4/hjz/raft/src/kvraft/20241102_114154/TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B_321.log'
awk '/{Server 0} commit log/ || /{Server 0} Commit snapshot/ || /StartKVServer\[0\]/ {print}' "${file}" > server-0.log
awk '/{Server 1} commit log/ || /{Server 1} Commit snapshot/ || /StartKVServer\[1\]/ {print}' "${file}" > server-1.log
awk '/{Server 2} commit log/ || /{Server 2} Commit snapshot/ || /StartKVServer\[2\]/ {print}' "${file}" > server-2.log
awk '/{Server 3} commit log/ || /{Server 3} Commit snapshot/ || /StartKVServer\[3\]/ {print}' "${file}" > server-3.log
awk '/{Server 4} commit log/ || /{Server 4} Commit snapshot/ || /StartKVServer\[4\]/ {print}' "${file}" > server-4.log
awk '/{Server 5} commit log/ || /{Server 5} Commit snapshot/ || /StartKVServer\[5\]/ {print}' "${file}" > server-5.log
awk '/{Server 6} commit log/ || /{Server 6} Commit snapshot/ || /StartKVServer\[6\]/ {print}' "${file}" > server-6.log