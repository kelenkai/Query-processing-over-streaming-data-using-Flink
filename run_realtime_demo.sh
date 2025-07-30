#!/bin/bash

echo "=== TPC-H Query 3 实时TopN演示 ==="
echo "1. 编译Java程序..."

# 编译程序
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "❌ 编译失败，请检查代码错误"
    exit 1
fi

echo "✅ 编译成功"

# 检查streamdata.csv是否存在
if [ ! -f "streamdata.csv" ]; then
    echo "⚠️  警告: streamdata.csv 文件不存在"
    echo "请确保数据文件存在，否则程序会启动失败"
fi

echo ""
echo "🚀 启动实时流处理系统..."
echo "📊 WebSocket服务器将在端口 8080 启动"
echo "🌐 请在浏览器中打开 simple_index.html 查看实时结果"
echo ""
echo "💡 使用说明:"
echo "   1. 运行此脚本启动后端流处理"
echo "   2. 在浏览器中打开 simple_index.html"
echo "   3. 观察实时TopN结果更新"
echo "   4. 按 Ctrl+C 停止程序"
echo ""

# 设置并行度，默认为4
PARALLELISM=${1:-2}
echo "📈 使用并行度: $PARALLELISM"
echo ""

# 运行程序
mvn exec:java -Dexec.mainClass="org.example.TPCHQuery3_StreamProcessor" -Dexec.args="$PARALLELISM" -q 