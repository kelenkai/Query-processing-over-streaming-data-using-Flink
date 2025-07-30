package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// TPC-H Query 3 Standard SQL Definition
// SELECT
//     l_orderkey,
//     sum(l_extendedprice * (1 - l_discount)) as revenue,
//     o_orderdate,
//     o_shippriority
// FROM
//     customer,
//     orders,
//     lineitem
// WHERE
//     c_mktsegment = 'BUILDING'
//     AND c_custkey = o_custkey
//     AND l_orderkey = o_orderkey
//     AND o_orderdate < date '1995-03-15'
//     AND l_shipdate > date '1995-03-15'
// GROUP BY
//     l_orderkey,
//     o_orderdate,
//     o_shippriority
// ORDER BY
//     revenue desc,
//     o_orderdate
// LIMIT 20;

/**
 * TPC-H Query 3 Production Flink Stream Implementation - Dashboard Version
 * Purpose: Real-time stream processing with WebSocket integration and live
 * dashboard
 * Approach: Event-by-event processing with comprehensive state management and
 * real-time visualization
 * Use Case: Production environments with monitoring dashboards and real-time
 * analytics
 * Optimal Parallelism: 2 threads (WebSocket overhead consideration)
 * Features: WebSocket server, JSON broadcasting, thread monitoring, real-time
 * charts
 */
public class TPCHQuery3_StreamProcessor {

    // Stream event structure for unified data source (same as optimized version)
    public static class StreamEvent {
        public String relation; // Table type: "customer", "orders", "lineitem"
        public String action; // Action type: "Insert", "Delete" (for streaming semantics)
        public Object key; // Primary key of the record
        public Object[] values; // Data values array
        public String[] columns; // Column names for reference
        public long sequenceNumber; // Event sequence for ordering

        public StreamEvent() {
        }

        public StreamEvent(String relation, String action, Object key,
                Object[] values, String[] columns, long sequenceNumber) {
            this.relation = relation;
            this.action = action;
            this.key = key;
            this.values = values;
            this.columns = columns;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public String toString() {
            return String.format("StreamEvent{%s %s, key=%s, seq=%d}",
                    action, relation, key, sequenceNumber);
        }
    }

    // Data structures (same as optimized version but with dashboard integration)
    public static class Customer {
        public long custkey; // Primary key
        public String mktsegment; // Filter field: 'BUILDING'

        public Customer() {
        }

        public Customer(long custkey, String mktsegment) {
            this.custkey = custkey;
            this.mktsegment = mktsegment;
        }
    }

    public static class Order {
        public long orderkey; // Primary key
        public long custkey; // Foreign key to customer
        public Date orderdate; // Filter field: < '1995-03-15'
        public int shippriority; // Output field

        public Order() {
        }

        public Order(long orderkey, long custkey, Date orderdate, int shippriority) {
            this.orderkey = orderkey;
            this.custkey = custkey;
            this.orderdate = orderdate;
            this.shippriority = shippriority;
        }
    }

    public static class LineItem {
        public long orderkey; // Foreign key to order
        public Date shipdate; // Filter field: > '1995-03-15'
        public int linenumber; // Unique identifier within order
        public double extendedprice; // Revenue calculation component
        public double discount; // Revenue calculation component

        public LineItem() {
        }

        public LineItem(long orderkey, Date shipdate, int linenumber,
                double extendedprice, double discount) {
            this.orderkey = orderkey;
            this.shipdate = shipdate;
            this.linenumber = linenumber;
            this.extendedprice = extendedprice;
            this.discount = discount;
        }
    }

    public static class QueryResult {
        public long orderkey; // Grouping key
        public double revenue; // Aggregated revenue
        public Date orderdate; // Grouping key
        public int shippriority; // Grouping key
        public String action; // "Insert" or "Delete" for streaming updates

        public QueryResult() {
        }

        public QueryResult(long orderkey, double revenue, Date orderdate,
                int shippriority, String action) {
            this.orderkey = orderkey;
            this.revenue = revenue;
            this.orderdate = orderdate;
            this.shippriority = shippriority;
            this.action = action;
        }

        @Override
        public String toString() {
            return String.format("%s: OrderKey=%d, Revenue=%.2f, OrderDate=%s, ShipPriority=%d",
                    action, orderkey, revenue, orderdate, shippriority);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism configuration (optimal: 2 for WebSocket version due to I/O
        // overhead)
        int parallelism = 4;
        if (args.length > 0) {
            parallelism = Integer.parseInt(args[0]);
        }
        env.setParallelism(parallelism);
        System.out.println("=== 使用并行度: " + parallelism + " ===");

        // 1. Create unified stream source - TRUE STREAMING with WebSocket integration
        DataStream<StreamEvent> inputStream = env
                .addSource(new UnifiedStreamSource("streamdata.csv"))
                .setParallelism(1); // Single source to maintain event ordering

        // 2. Stream separation using filters (same as optimized version)
        DataStream<StreamEvent> customerStream = inputStream.filter(e -> "customer".equals(e.relation));
        DataStream<StreamEvent> ordersStream = inputStream.filter(e -> "orders".equals(e.relation));
        DataStream<StreamEvent> lineitemStream = inputStream.filter(e -> "lineitem".equals(e.relation));

        // 3-5. Filter operations (same as optimized version)
        DataStream<StreamEvent> filteredCustomers = customerStream
                .filter(new CustomerFilter());

        DataStream<StreamEvent> filteredOrders = ordersStream
                .filter(new OrderFilter());

        DataStream<StreamEvent> filteredLineItems = lineitemStream
                .filter(new LineItemFilter());

        // 6-7. Stateful JOINs (same as optimized version)
        DataStream<StreamEvent> customerOrderJoin = filteredCustomers
                .keyBy(e -> (Long) e.key)
                .connect(filteredOrders.keyBy(e -> (Long) e.values[0]))
                .process(new CustomerOrderJoinFunction());

        DataStream<StreamEvent> orderLineItemJoin = customerOrderJoin
                .keyBy(e -> (Long) e.values[1])
                .connect(filteredLineItems.keyBy(e -> (Long) e.key))
                .process(new OrderLineItemJoinFunction());

        // 8. Incremental aggregation with WebSocket integration
        DataStream<QueryResult> aggregatedResults = orderLineItemJoin
                .keyBy(new KeySelector<StreamEvent, Tuple3<Long, Date, Integer>>() {
                    @Override
                    public Tuple3<Long, Date, Integer> getKey(StreamEvent event) throws Exception {
                        return new Tuple3<>((Long) event.values[1], (Date) event.values[2], (Integer) event.values[3]);
                    }
                })
                .process(new IncrementalAggregateFunction());

        // 9. TopN ranking with WebSocket broadcasting and dashboard integration
        DataStream<QueryResult> sortedResults = aggregatedResults
                .keyBy(r -> 0) // Global partitioning for TopN
                .process(new TopNFunction()); // Includes WebSocket server and JSON broadcasting

        // 10. No stream output (WebSocket handles all output)
        // sortedResults.print("TPC-H Query 3 最终结果:");

        long queryStartTime = System.currentTimeMillis();

        // Shutdown hook for graceful WebSocket server shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n程序正在关闭...");
            WebSocketServer.shutdown();
        }));

        env.execute("TPC-H Query 3 - 真正的流处理版本");

        long endTime = System.currentTimeMillis();
        System.out.println("\n=== 流处理执行时间统计 ===");
        System.out.println("并行度: " + parallelism);
        System.out.println("总执行时间: " + (endTime - startTime) + " ms");

        // Final cleanup
        System.out.println("程序执行完成，正在退出...");
        WebSocketServer.shutdown();
        System.exit(0);
    }

    // Unified stream source (same as optimized version)
    public static class UnifiedStreamSource implements SourceFunction<StreamEvent> {
        private volatile boolean isRunning = true;
        private final String filePath;

        public UnifiedStreamSource(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void run(SourceContext<StreamEvent> ctx) throws Exception {
            System.out.println("开始读取流式数据: " + filePath);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                long sequenceNumber = 0;

                while ((line = reader.readLine()) != null && isRunning) {
                    StreamEvent event = parseStreamEvent(line, sequenceNumber++, sdf);
                    if (event != null) {
                        ctx.collect(event); // Event-by-event emission

                        // Streaming simulation: controlled delay every 1000 events
                        if (sequenceNumber % 1000 == 0) {
                            Thread.sleep(1); // 1ms delay per 1000 records
                        }
                    }
                }
                System.out.println("总共处理数据行数: " + sequenceNumber);
            }
        }

        // Parse stream event (same as optimized version)
        private StreamEvent parseStreamEvent(String line, long sequenceNumber, SimpleDateFormat sdf) {
            try {
                if (line.length() < 3)
                    return null;

                String header = line.substring(0, 3);
                String[] cells = line.substring(3).split("\\|");

                String action = header.startsWith("+") ? "Insert" : "Delete";
                String tableType = header.substring(1);

                switch (tableType) {
                    case "CU": // Customer event
                        return new StreamEvent("customer", action, Long.parseLong(cells[0]),
                                new Object[] { Long.parseLong(cells[0]), cells[6] },
                                new String[] { "CUSTKEY", "C_MKTSEGMENT" }, sequenceNumber);

                    case "OR": // Order event
                        return new StreamEvent("orders", action, Long.parseLong(cells[1]),
                                new Object[] { Long.parseLong(cells[1]), Long.parseLong(cells[0]),
                                        sdf.parse(cells[4]), Integer.parseInt(cells[7]) },
                                new String[] { "CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                                sequenceNumber);

                    case "LI": // LineItem event
                        return new StreamEvent("lineitem", action, Long.parseLong(cells[0]),
                                new Object[] { sdf.parse(cells[10]), Integer.parseInt(cells[3]),
                                        Long.parseLong(cells[0]), Double.parseDouble(cells[5]),
                                        Double.parseDouble(cells[6]) },
                                new String[] { "L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE",
                                        "L_DISCOUNT" },
                                sequenceNumber);
                }
            } catch (Exception e) {
                System.err.println("解析错误: " + line + " - " + e.getMessage());
            }
            return null;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // Filter functions (same as optimized version)
    public static class CustomerFilter implements FilterFunction<StreamEvent> {
        @Override
        public boolean filter(StreamEvent event) throws Exception {
            if (!"customer".equals(event.relation))
                return false;
            String mktsegment = (String) event.values[1];
            return "BUILDING".equals(mktsegment);
        }
    }

    public static class OrderFilter implements FilterFunction<StreamEvent> {
        private final Date cutoffDate;

        public OrderFilter() throws Exception {
            this.cutoffDate = new SimpleDateFormat("yyyy-MM-dd").parse("1995-03-15");
        }

        @Override
        public boolean filter(StreamEvent event) throws Exception {
            if (!"orders".equals(event.relation))
                return false;
            Date orderDate = (Date) event.values[2];
            return orderDate.before(cutoffDate);
        }
    }

    public static class LineItemFilter implements FilterFunction<StreamEvent> {
        private final Date cutoffDate;

        public LineItemFilter() throws Exception {
            this.cutoffDate = new SimpleDateFormat("yyyy-MM-dd").parse("1995-03-15");
        }

        @Override
        public boolean filter(StreamEvent event) throws Exception {
            if (!"lineitem".equals(event.relation))
                return false;
            Date shipDate = (Date) event.values[0];
            return shipDate.after(cutoffDate);
        }
    }

    // JOIN functions (same as optimized version)
    public static class CustomerOrderJoinFunction extends CoProcessFunction<StreamEvent, StreamEvent, StreamEvent> {
        private transient ValueState<StreamEvent> customerState;
        private transient MapState<Long, StreamEvent> orderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            customerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("customerState", StreamEvent.class));
            orderState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("orderState", Long.class, StreamEvent.class));
        }

        @Override
        public void processElement1(StreamEvent customer, Context ctx, Collector<StreamEvent> out) throws Exception {
            if ("Insert".equals(customer.action)) {
                customerState.update(customer);
                for (StreamEvent order : orderState.values()) {
                    if (order != null) {
                        emitJoinResult(customer, order, out);
                    }
                }
            } else { // Delete
                customerState.clear();
                for (StreamEvent order : orderState.values()) {
                    if (order != null) {
                        emitJoinResult(customer, order, out);
                    }
                }
            }
        }

        @Override
        public void processElement2(StreamEvent order, Context ctx, Collector<StreamEvent> out) throws Exception {
            if ("Insert".equals(order.action)) {
                orderState.put((Long) order.values[1], order);
                StreamEvent customer = customerState.value();
                if (customer != null) {
                    emitJoinResult(customer, order, out);
                }
            } else { // Delete
                orderState.remove((Long) order.values[1]);
                StreamEvent customer = customerState.value();
                if (customer != null) {
                    emitJoinResult(customer, order, out);
                }
            }
        }

        private void emitJoinResult(StreamEvent customer, StreamEvent order, Collector<StreamEvent> out) {
            String action = "Insert".equals(customer.action) && "Insert".equals(order.action) ? "Insert" : "Delete";

            StreamEvent result = new StreamEvent("customer_order_join", action, order.values[1],
                    new Object[] { (Long) customer.key, (Long) order.values[1], (Date) order.values[2],
                            (Integer) order.values[3] },
                    new String[] { "CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                    Math.max(customer.sequenceNumber, order.sequenceNumber));

            out.collect(result);
        }
    }

    public static class OrderLineItemJoinFunction extends CoProcessFunction<StreamEvent, StreamEvent, StreamEvent> {
        private transient ValueState<StreamEvent> orderState;
        private transient MapState<Integer, StreamEvent> lineItemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("orderState", StreamEvent.class));
            lineItemState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("lineItemState", Integer.class, StreamEvent.class));
        }

        @Override
        public void processElement1(StreamEvent orderJoin, Context ctx, Collector<StreamEvent> out) throws Exception {
            if ("Insert".equals(orderJoin.action)) {
                orderState.update(orderJoin);
                for (StreamEvent lineItem : lineItemState.values()) {
                    if (lineItem != null) {
                        emitJoinResult(orderJoin, lineItem, out);
                    }
                }
            } else { // Delete
                orderState.clear();
                for (StreamEvent lineItem : lineItemState.values()) {
                    if (lineItem != null) {
                        emitJoinResult(orderJoin, lineItem, out);
                    }
                }
            }
        }

        @Override
        public void processElement2(StreamEvent lineItem, Context ctx, Collector<StreamEvent> out) throws Exception {
            if ("Insert".equals(lineItem.action)) {
                lineItemState.put((Integer) lineItem.values[1], lineItem);
                StreamEvent orderJoin = orderState.value();
                if (orderJoin != null) {
                    emitJoinResult(orderJoin, lineItem, out);
                }
            } else { // Delete
                lineItemState.remove((Integer) lineItem.values[1]);
                StreamEvent orderJoin = orderState.value();
                if (orderJoin != null) {
                    emitJoinResult(orderJoin, lineItem, out);
                }
            }
        }

        private void emitJoinResult(StreamEvent orderJoin, StreamEvent lineItem, Collector<StreamEvent> out) {
            String action = "Insert".equals(orderJoin.action) && "Insert".equals(lineItem.action) ? "Insert" : "Delete";

            // Calculate revenue: extendedprice * (1 - discount)
            double revenue = (Double) lineItem.values[3] * (1.0 - (Double) lineItem.values[4]);

            StreamEvent result = new StreamEvent("final_join", action, orderJoin.values[1],
                    new Object[] { revenue, (Long) orderJoin.values[1], (Date) orderJoin.values[2],
                            (Integer) orderJoin.values[3] },
                    new String[] { "REVENUE", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                    Math.max(orderJoin.sequenceNumber, lineItem.sequenceNumber));

            out.collect(result);
        }
    }

    // Incremental aggregation function (same as optimized version)
    public static class IncrementalAggregateFunction
            extends
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<Tuple3<Long, Date, Integer>, StreamEvent, QueryResult> {
        private transient ValueState<Double> revenueState;
        private transient ValueState<Boolean> outputScheduled;
        private final long outputDelay = 2000; // 2 seconds output interval

        @Override
        public void open(Configuration parameters) throws Exception {
            revenueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("revenueState", Double.class));
            outputScheduled = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("outputScheduled", Boolean.class));
        }

        @Override
        public void processElement(StreamEvent event, Context ctx, Collector<QueryResult> out) throws Exception {
            Double currentRevenue = revenueState.value();
            if (currentRevenue == null) {
                currentRevenue = 0.0;
            }

            double deltaRevenue = (Double) event.values[0];

            if ("Insert".equals(event.action)) {
                currentRevenue += deltaRevenue;
            } else { // Delete
                currentRevenue -= deltaRevenue;
            }

            revenueState.update(currentRevenue);

            Boolean scheduled = outputScheduled.value();
            if (scheduled == null || !scheduled) {
                long outputTime = ctx.timerService().currentProcessingTime() + outputDelay;
                ctx.timerService().registerProcessingTimeTimer(outputTime);
                outputScheduled.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<QueryResult> out) throws Exception {
            Double currentRevenue = revenueState.value();

            if (currentRevenue != null && currentRevenue > 0) {
                Tuple3<Long, Date, Integer> key = ctx.getCurrentKey();
                QueryResult result = new QueryResult(
                        key.f0, // orderkey
                        currentRevenue,
                        key.f1, // orderdate
                        key.f2, // shippriority
                        "最终聚合结果");

                out.collect(result);
            }

            outputScheduled.update(false);
        }
    }

    /**
     * WebSocket Server for Real-time Dashboard Integration
     * Features: NIO-based server, WebSocket handshake, JSON broadcasting, thread
     * monitoring
     * Performance Impact: Single-threaded executor, concurrent client management
     * Dashboard Support: Chart.js data format, D3.js flow diagrams, real-time
     * metrics
     */
    public static class WebSocketServer {
        private static final ConcurrentHashMap<String, SocketChannel> clients = new ConcurrentHashMap<>(); // Active
                                                                                                           // WebSocket
                                                                                                           // clients
        private static ExecutorService executor = Executors.newSingleThreadExecutor(); // Single-threaded server
        private static boolean isStarted = false;

        public static synchronized void start() {
            if (isStarted)
                return;
            isStarted = true;

            executor.submit(() -> {
                try {
                    // NIO-based WebSocket server setup
                    ServerSocketChannel serverChannel = ServerSocketChannel.open();
                    serverChannel.configureBlocking(false);
                    serverChannel.bind(new InetSocketAddress(8080)); // WebSocket port

                    Selector selector = Selector.open();
                    serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                    System.out.println("WebSocket服务器启动在端口 8080");

                    // Event loop for handling WebSocket connections
                    while (true) {
                        selector.select();
                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                        while (keys.hasNext()) {
                            SelectionKey key = keys.next();
                            keys.remove();

                            if (key.isAcceptable()) {
                                handleAccept(serverChannel, selector);
                            } else if (key.isReadable()) {
                                handleRead(key);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Handle new client connections
        private static void handleAccept(ServerSocketChannel serverChannel, Selector selector) throws IOException {
            SocketChannel clientChannel = serverChannel.accept();
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
        }

        // Handle client requests and WebSocket handshake
        private static void handleRead(SelectionKey key) throws IOException {
            SocketChannel clientChannel = (SocketChannel) key.channel();
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int bytesRead = clientChannel.read(buffer);

            if (bytesRead > 0) {
                buffer.flip();
                String request = new String(buffer.array(), 0, bytesRead, StandardCharsets.UTF_8);

                if (request.contains("Upgrade: websocket")) {
                    performWebSocketHandshake(clientChannel, request); // WebSocket protocol handshake
                    String clientId = clientChannel.getRemoteAddress().toString();
                    clients.put(clientId, clientChannel);
                    System.out.println("新的WebSocket客户端连接: " + clientId);
                }
            } else if (bytesRead == -1) {
                // Client disconnection cleanup
                String clientId = clientChannel.getRemoteAddress().toString();
                clients.remove(clientId);
                clientChannel.close();
                key.cancel();
            }
        }

        // Perform WebSocket handshake according to RFC 6455
        private static void performWebSocketHandshake(SocketChannel clientChannel, String request) throws IOException {
            String webSocketKey = null;
            String[] lines = request.split("\r\n");
            for (String line : lines) {
                if (line.startsWith("Sec-WebSocket-Key:")) {
                    webSocketKey = line.substring(19).trim();
                    break;
                }
            }

            if (webSocketKey != null) {
                try {
                    // WebSocket handshake key generation
                    String magicString = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
                    String acceptKey = Base64.getEncoder().encodeToString(
                            MessageDigest.getInstance("SHA-1")
                                    .digest((webSocketKey + magicString).getBytes(StandardCharsets.UTF_8)));

                    String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                            "Upgrade: websocket\r\n" +
                            "Connection: Upgrade\r\n" +
                            "Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n";

                    clientChannel.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // Broadcast JSON data to all connected clients
        public static void broadcast(String message) {
            byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
            ByteBuffer frame = createWebSocketFrame(messageBytes); // WebSocket frame format

            clients.entrySet().removeIf(entry -> {
                try {
                    entry.getValue().write(frame.duplicate());
                    return false; // Keep connected client
                } catch (IOException e) {
                    try {
                        entry.getValue().close();
                    } catch (IOException ex) {
                    }
                    return true; // Remove disconnected client
                }
            });
        }

        // Thread monitoring for dashboard system metrics
        public static void broadcastThreadInfo() {
            try {
                StringBuilder jsonBuilder = new StringBuilder();
                jsonBuilder.append("{\"type\":\"threads\",\"timestamp\":\"").append(new Date()).append("\",\"data\":[");

                Thread[] threads = new Thread[Thread.activeCount() + 50];
                int threadCount = Thread.enumerate(threads);

                System.out.println("=== 调试线程信息 ===");
                System.out.println("总线程数: " + threadCount);

                int validThreadCount = 0;
                for (int i = 0; i < threadCount; i++) {
                    Thread thread = threads[i];
                    if (thread != null) {
                        String threadName = thread.getName();
                        System.out.println("线程 " + i + ": " + threadName + " - " + thread.getState());

                        // Filter system threads, show only application threads
                        if (!isSystemThread(threadName)) {
                            if (validThreadCount > 0)
                                jsonBuilder.append(",");

                            long cpuTime = 0;
                            try {
                                java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory
                                        .getThreadMXBean();
                                if (threadBean.isThreadCpuTimeSupported()) {
                                    cpuTime = threadBean.getThreadCpuTime(thread.getId()) / 1_000_000;
                                }
                            } catch (Exception e) {
                                // Ignore CPU time errors
                            }

                            jsonBuilder.append(String.format(
                                    "{\"id\":%d,\"name\":\"%s\",\"state\":\"%s\",\"priority\":%d,\"cpuTime\":%d}",
                                    thread.getId(),
                                    threadName.replace("\"", "\\\""),
                                    thread.getState().toString(),
                                    thread.getPriority(),
                                    cpuTime));
                            validThreadCount++;
                        }
                    }
                }

                jsonBuilder.append("]}");
                String message = jsonBuilder.toString();
                System.out.println("发送线程信息: 找到 " + validThreadCount + " 个有效线程");
                System.out.println("JSON长度: " + message.length());
                broadcast(message);

            } catch (Exception e) {
                System.err.println("获取线程信息失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Start thread monitoring service
        public static void startThreadMonitor() {
            executor.submit(() -> {
                // Initial test data
                try {
                    Thread.sleep(3000); // Wait for frontend connection
                    if (!clients.isEmpty()) {
                        sendTestThreadData();
                    }
                } catch (InterruptedException e) {
                    return;
                }

                // Continuous thread monitoring
                while (true) {
                    try {
                        Thread.sleep(2000); // Monitor every 2 seconds
                        if (!clients.isEmpty()) {
                            broadcastThreadInfo();
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        System.err.println("线程监控器错误: " + e.getMessage());
                    }
                }
            });
        }

        // Send test thread data for dashboard initialization
        private static void sendTestThreadData() {
            String testData = "{\"type\":\"threads\",\"timestamp\":\"" + new Date() + "\",\"data\":[" +
                    "{\"id\":1001,\"name\":\"main\",\"state\":\"RUNNABLE\",\"priority\":5,\"cpuTime\":1500}," +
                    "{\"id\":1002,\"name\":\"flink-akka.actor.default-dispatcher-1\",\"state\":\"RUNNABLE\",\"priority\":5,\"cpuTime\":800},"
                    +
                    "{\"id\":1003,\"name\":\"flink-metrics-1\",\"state\":\"TIMED_WAITING\",\"priority\":5,\"cpuTime\":200}"
                    +
                    "]}";
            System.out.println("发送测试线程数据");
            broadcast(testData);
        }

        // Broadcast simplified thread info with each TopN update
        public static void broadcastSimpleThreadInfo() {
            try {
                long currentTime = System.currentTimeMillis();
                Thread currentThread = Thread.currentThread();

                // Get real thread information for dashboard
                java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory.getThreadMXBean();
                Thread[] threads = new Thread[Thread.activeCount() + 50];
                int threadCount = Thread.enumerate(threads);

                StringBuilder jsonBuilder = new StringBuilder();
                jsonBuilder.append("{\"type\":\"threads\",\"timestamp\":\"").append(new Date()).append("\",\"data\":[");

                int validThreadCount = 0;
                for (int i = 0; i < threadCount; i++) {
                    Thread thread = threads[i];
                    if (thread != null) {
                        String threadName = thread.getName();

                        // Show important application threads
                        if (!isSystemThread(threadName)) {
                            if (validThreadCount > 0) {
                                jsonBuilder.append(",");
                            }

                            long cpuTime = 0;
                            try {
                                if (threadBean.isThreadCpuTimeSupported()) {
                                    cpuTime = threadBean.getThreadCpuTime(thread.getId()) / 1_000_000;
                                }
                            } catch (Exception e) {
                                cpuTime = System.currentTimeMillis() % 10000; // Fallback
                            }

                            String threadType = isImportantThread(threadName) ? "important" : "normal";
                            jsonBuilder.append(String.format(
                                    "{\"id\":%d,\"name\":\"%s\",\"state\":\"%s\",\"priority\":%d,\"cpuTime\":%d,\"type\":\"%s\"}",
                                    thread.getId(),
                                    threadName.replace("\"", "\\\""),
                                    thread.getState().toString(),
                                    thread.getPriority(),
                                    cpuTime,
                                    threadType));
                            validThreadCount++;
                        }
                    }
                }

                jsonBuilder.append("]}");
                String message = jsonBuilder.toString();
                System.out.println("发送实时线程信息: " + validThreadCount + " 个线程，客户端数量: " + clients.size());
                broadcast(message);
            } catch (Exception e) {
                System.err.println("发送简单线程信息失败: " + e.getMessage());
            }
        }

        // Check if thread is Flink-related (for filtering)
        private static boolean isFlinkRelatedThread(String threadName) {
            String name = threadName.toLowerCase();
            return name.contains("flink") ||
                    name.contains("akka") ||
                    name.contains("netty") ||
                    name.contains("pool") ||
                    name.contains("task") ||
                    name.contains("source") ||
                    name.contains("sink") ||
                    name.contains("metrics") ||
                    name.contains("checkpoint") ||
                    name.contains("rest") ||
                    name.contains("rpc") ||
                    name.contains("executionmain") ||
                    name.contains("tpchmain");
        }

        // Filter system threads (to be excluded from dashboard)
        private static boolean isSystemThread(String threadName) {
            String name = threadName.toLowerCase();
            return name.contains("gc") ||
                    name.contains("reference handler") ||
                    name.contains("finalizer") ||
                    name.contains("signal dispatcher") ||
                    name.contains("attach listener") ||
                    name.contains("common-cleaner") ||
                    name.contains("notification-thread") ||
                    name.contains("process reaper") ||
                    name.contains("compiler thread") ||
                    name.contains("sweeper thread") ||
                    name.contains("service thread") ||
                    name.contains("vm thread") ||
                    name.contains("vm periodic task thread");
        }

        // Graceful WebSocket server shutdown
        public static void shutdown() {
            try {
                System.out.println("正在关闭WebSocket服务器...");

                // Close all client connections
                for (SocketChannel client : clients.values()) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        // Ignore shutdown exceptions
                    }
                }
                clients.clear();

                // Shutdown executor service
                if (executor != null && !executor.isShutdown()) {
                    executor.shutdownNow();
                }

                System.out.println("WebSocket服务器已关闭");
            } catch (Exception e) {
                System.err.println("关闭WebSocket服务器时出错: " + e.getMessage());
            }
        }

        // Classify important application threads for dashboard highlighting
        private static boolean isImportantThread(String threadName) {
            String name = threadName.toLowerCase();
            return name.contains("main") ||
                    name.contains("flink") ||
                    name.contains("akka") ||
                    name.contains("netty") ||
                    name.contains("pool") ||
                    name.contains("task") ||
                    name.contains("source") ||
                    name.contains("sink") ||
                    name.contains("metrics") ||
                    name.contains("checkpoint") ||
                    name.contains("rest") ||
                    name.contains("rpc") ||
                    name.contains("websocket") ||
                    name.contains("http") ||
                    name.contains("executor");
        }

        // Create WebSocket frame according to RFC 6455
        private static ByteBuffer createWebSocketFrame(byte[] payload) {
            int frameSize = 2 + payload.length;
            if (payload.length > 125) {
                frameSize += 2; // Extended payload length
            }

            ByteBuffer frame = ByteBuffer.allocate(frameSize);
            frame.put((byte) 0x81); // FIN = 1, opcode = 1 (text frame)

            if (payload.length <= 125) {
                frame.put((byte) payload.length);
            } else {
                frame.put((byte) 126); // Extended payload indicator
                frame.putShort((short) payload.length);
            }

            frame.put(payload);
            frame.flip();
            return frame;
        }
    }

    /**
     * TopN Function with WebSocket Integration and Dashboard Broadcasting
     * Features: JSON data formatting, Chart.js compatibility, real-time updates
     * Performance: 1-second output interval (faster updates for dashboard)
     * Dashboard Integration: WebSocket server startup, thread monitoring, JSON
     * broadcasting
     */
    public static class TopNFunction
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Integer, QueryResult, QueryResult> {
        private transient java.util.List<QueryResult> allResults;
        private transient long processedCount = 0;
        private transient long lastOutputTime = 0;
        private final long outputInterval = 1000; // 1 second output for real-time dashboard
        private final int topN = 20;

        @Override
        public void open(Configuration parameters) throws Exception {
            allResults = new java.util.ArrayList<>();
            // Start WebSocket server for dashboard integration
            WebSocketServer.start();
            // Start thread monitoring for system metrics
            WebSocketServer.startThreadMonitor();
        }

        @Override
        public void processElement(QueryResult value, Context ctx, Collector<QueryResult> out) throws Exception {
            if (Math.abs(value.revenue) > 0.01) {
                allResults.add(value);
                processedCount++;

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastOutputTime > outputInterval) {
                    outputTopN(out);
                    lastOutputTime = currentTime;
                }

                ctx.timerService().registerProcessingTimeTimer(currentTime + outputInterval);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<QueryResult> out) throws Exception {
            outputTopN(out);
        }

        // Output TopN results with WebSocket broadcasting for dashboard
        private void outputTopN(Collector<QueryResult> out) {
            if (allResults.isEmpty())
                return;

            // Sort: revenue DESC, orderdate ASC (TPC-H Query 3 specification)
            allResults.sort(java.util.Comparator
                    .comparingDouble((QueryResult r) -> -r.revenue)
                    .thenComparing(r -> r.orderdate));

            int count = Math.min(topN, allResults.size());
            System.out.println("\n=== TPC-H Query 3 最终结果 (TOP " + count + ") ===");
            System.out.println("排名\tOrderKey\tRevenue\t\tOrderDate\tShipPriority");
            System.out.println("----\t--------\t-------\t\t---------\t------------");

            // Build JSON data for WebSocket broadcasting (Chart.js format)
            StringBuilder jsonBuilder = new StringBuilder();
            jsonBuilder.append("{\"timestamp\":\"").append(new Date()).append("\",\"data\":[");

            for (int i = 0; i < count; i++) {
                QueryResult result = allResults.get(i);
                System.out.printf("%2d\t%d\t%.2f\t%tF\t%d\n",
                        i + 1, result.orderkey, result.revenue, result.orderdate, result.shippriority);

                // Build JSON format for dashboard charts
                if (i > 0)
                    jsonBuilder.append(",");
                jsonBuilder.append(String.format(
                        "{\"rank\":%d,\"orderkey\":%d,\"revenue\":%.2f,\"orderdate\":%d,\"shippriority\":%d}",
                        i + 1, result.orderkey, result.revenue, result.orderdate.getTime(), result.shippriority));
            }
            jsonBuilder.append("]}");

            // Broadcast results to WebSocket clients for real-time dashboard updates
            WebSocketServer.broadcast(jsonBuilder.toString());

            // Also send thread information for system monitoring
            WebSocketServer.broadcastSimpleThreadInfo();

            System.out.println("=== 处理了 " + processedCount + " 条聚合记录 ===\n");
        }
    }
}