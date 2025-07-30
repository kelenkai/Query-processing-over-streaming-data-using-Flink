#!/bin/bash

# TPC-H Query 3 快速并行度性能测试脚本
# 简化版本 - 每个并行度只运行一次

echo "=== TPC-H Query 3 快速并行度性能测试 ==="
echo "测试时间: $(date)"
echo ""

# 编译项目
echo "正在编译项目..."
mvn clean compile -q
if [ $? -ne 0 ]; then
    echo "编译失败!"
    exit 1
fi
echo "编译完成!"
echo ""

# 存储结果
declare -a RESULTS=()

echo "开始性能测试..."
echo "并行度\t执行时间(ms)\t结果"
echo "------\t-----------\t----"

# 测试不同的并行度
for parallelism in {1..8}; do
    echo -n "$parallelism\t"
    
    # 运行测试并捕获输出
    output=$(java -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
             org.example.TPCHQuery3_StreamProcessor $parallelism 2>&1)
    
    # 提取执行时间
    execution_time=$(echo "$output" | grep "总执行时间" | grep -o '[0-9]\+' | tail -1)
    
    if [ -n "$execution_time" ]; then
        echo -n "$execution_time\t\t"
        RESULTS+=("$parallelism:$execution_time")
        
        # 提取第一名结果作为验证
        first_result=$(echo "$output" | grep -A1 "TPC-H Query 3 最终结果" | grep "^1" | head -1)
        if [ -n "$first_result" ]; then
            echo "✓ 成功"
        else
            echo "⚠ 无结果"
        fi
    else
        echo "失败\t\t✗ 错误"
    fi
    
    # 短暂等待
    sleep 2
done

echo ""
echo "=== 性能分析 ==="

# 找出最快和最慢的
best_time=999999999
worst_time=0
best_parallel=1
worst_parallel=1

for result in "${RESULTS[@]}"; do
    parallel=$(echo $result | cut -d: -f1)
    time=$(echo $result | cut -d: -f2)
    
    if [ $time -lt $best_time ]; then
        best_time=$time
        best_parallel=$parallel
    fi
    
    if [ $time -gt $worst_time ]; then
        worst_time=$time
        worst_parallel=$parallel
    fi
done

echo "最快配置: 并行度 $best_parallel (${best_time} ms)"
echo "最慢配置: 并行度 $worst_parallel (${worst_time} ms)"

if [ $worst_time -gt 0 ]; then
    speedup=$(echo "scale=2; $worst_time / $best_time" | bc -l)
    echo "性能提升: ${speedup}x"
fi

echo ""
echo "建议: 使用并行度 $best_parallel 获得最佳性能" 