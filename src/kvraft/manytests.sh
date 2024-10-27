#!/bin/bash

n=$1
threads=$2

folder_name=$(date +"%Y%m%d%H%M%S")
mkdir -p "$folder_name"

for ((j = 0; j < threads; j++)); do
  (
    for ((k = 0; k < n; k++)); do
        # 执行 go test 并将输出重定向到文件
        (go_test_output_file="$folder_name/output_$i.txt"
        go test -run 3A > "$go_test_output_file"

        # 检查 go test 是否成功（假设成功时无输出）
        if [ -s "$go_test_output_file" ]; then
            echo "Test $i failed. Output saved in $go_test_output_file"
        else
            echo "Test $i succeeded. Removing output file."
            rm "$go_test_output_file"
        fi)
    done
  ) &
done

