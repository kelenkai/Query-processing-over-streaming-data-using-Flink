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
import java.text.SimpleDateFormat;
import java.util.Date;

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
 * TPC-H Query 3 Optimized Flink Stream Implementation - Performance Version
 * Purpose: High-performance streaming without WebSocket overhead
 * Approach: Event-by-event processing with unified stream source and stateful
 * operations
 * Use Case: Maximum throughput processing for production environments
 * Optimal Parallelism: 4 threads (vs 2 for WebSocket version)
 */
public class TPCHQuery3_StreamProcessor_no_websocket {

    // Stream event structure for unified data source
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

    // Customer data structure (minimal fields for performance)
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

    // Order data structure (minimal fields for performance)
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

    // LineItem data structure (minimal fields for performance)
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

    // Query result structure
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

        // Parallelism configuration (optimal: 4 for performance version)
        int parallelism = 4;
        if (args.length > 0) {
            parallelism = Integer.parseInt(args[0]);
        }
        env.setParallelism(parallelism);
        System.out.println("=== 使用并行度: " + parallelism + " ===");

        // 1. Create unified stream source - TRUE STREAMING: event-by-event processing
        DataStream<StreamEvent> inputStream = env
                .addSource(new UnifiedStreamSource("streamdata.csv"))
                .setParallelism(1); // Single source to maintain event ordering

        // 2. Stream separation using filters (no side outputs for performance)
        DataStream<StreamEvent> customerStream = inputStream.filter(e -> "customer".equals(e.relation));
        DataStream<StreamEvent> ordersStream = inputStream.filter(e -> "orders".equals(e.relation));
        DataStream<StreamEvent> lineitemStream = inputStream.filter(e -> "lineitem".equals(e.relation));

        // 3. Customer stream processing - filter 'BUILDING' segment
        DataStream<StreamEvent> filteredCustomers = customerStream
                .filter(new CustomerFilter());

        // 4. Orders stream processing - filter orderdate < '1995-03-15'
        DataStream<StreamEvent> filteredOrders = ordersStream
                .filter(new OrderFilter());

        // 5. LineItem stream processing - filter shipdate > '1995-03-15'
        DataStream<StreamEvent> filteredLineItems = lineitemStream
                .filter(new LineItemFilter());

        // 6. Stateful JOIN: Customer ⋈ Order (using CoProcessFunction with state)
        DataStream<StreamEvent> customerOrderJoin = filteredCustomers
                .keyBy(e -> (Long) e.key) // Key by customer key
                .connect(filteredOrders.keyBy(e -> (Long) e.values[0])) // Key by custkey in order
                .process(new CustomerOrderJoinFunction());

        // 7. Stateful JOIN: Order ⋈ LineItem (using CoProcessFunction with state)
        DataStream<StreamEvent> orderLineItemJoin = customerOrderJoin
                .keyBy(e -> (Long) e.values[1]) // Key by orderkey
                .connect(filteredLineItems.keyBy(e -> (Long) e.key)) // Key by orderkey
                .process(new OrderLineItemJoinFunction());

        // 8. Incremental aggregation with timer-based output
        DataStream<QueryResult> aggregatedResults = orderLineItemJoin
                .keyBy(new KeySelector<StreamEvent, Tuple3<Long, Date, Integer>>() {
                    @Override
                    public Tuple3<Long, Date, Integer> getKey(StreamEvent event) throws Exception {
                        return new Tuple3<>((Long) event.values[1], (Date) event.values[2], (Integer) event.values[3]);
                    }
                })
                .process(new IncrementalAggregateFunction());

        // 9. TopN ranking with global sorting - ORDER BY revenue DESC, orderdate LIMIT
        // 20
        DataStream<QueryResult> sortedResults = aggregatedResults
                .keyBy(r -> 0) // Global partitioning for TopN
                .process(new TopNFunction());

        // 10. No output to stream (console output only for performance)
        // sortedResults.print("TPC-H Query 3 最终结果:");

        long queryStartTime = System.currentTimeMillis();
        env.execute("TPC-H Query 3 - 真正的流处理版本");

        long endTime = System.currentTimeMillis();
        System.out.println("\n=== 流处理执行时间统计 ===");
        System.out.println("并行度: " + parallelism);
        System.out.println("总执行时间: " + (endTime - startTime) + " ms");
    }

    /**
     * Unified stream source - reads streamdata.csv event by event
     * Format: +CU|customer_data, +OR|order_data, +LI|lineitem_data
     * Performance: Simulates streaming latency with controlled delays
     */
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
                            Thread.sleep(1); // 1ms delay per 1000 records for realistic streaming
                        }
                    }
                }
                System.out.println("总共处理数据行数: " + sequenceNumber);
            }
        }

        // Parse unified stream event format (+CU, +OR, +LI prefixes)
        private StreamEvent parseStreamEvent(String line, long sequenceNumber, SimpleDateFormat sdf) {
            try {
                if (line.length() < 3)
                    return null;

                String header = line.substring(0, 3); // Extract event type prefix
                String[] cells = line.substring(3).split("\\|"); // Parse pipe-separated values

                String action = header.startsWith("+") ? "Insert" : "Delete";
                String tableType = header.substring(1); // CU, OR, LI

                switch (tableType) {
                    case "CU": // Customer event
                        return new StreamEvent("customer", action, Long.parseLong(cells[0]),
                                new Object[] { Long.parseLong(cells[0]), cells[6] }, // custkey, mktsegment
                                new String[] { "CUSTKEY", "C_MKTSEGMENT" }, sequenceNumber);

                    case "OR": // Order event
                        return new StreamEvent("orders", action, Long.parseLong(cells[1]),
                                new Object[] { Long.parseLong(cells[1]), Long.parseLong(cells[0]),
                                        sdf.parse(cells[4]), Integer.parseInt(cells[7]) }, // custkey, orderkey,
                                                                                           // orderdate, shippriority
                                new String[] { "CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                                sequenceNumber);

                    case "LI": // LineItem event
                        return new StreamEvent("lineitem", action, Long.parseLong(cells[0]),
                                new Object[] { sdf.parse(cells[10]), Integer.parseInt(cells[3]),
                                        Long.parseLong(cells[0]), Double.parseDouble(cells[5]),
                                        Double.parseDouble(cells[6]) }, // shipdate, linenumber, orderkey,
                                                                        // extendedprice, discount
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

    // Customer filter: market segment = 'BUILDING'
    public static class CustomerFilter implements FilterFunction<StreamEvent> {
        @Override
        public boolean filter(StreamEvent event) throws Exception {
            if (!"customer".equals(event.relation))
                return false;
            String mktsegment = (String) event.values[1];
            return "BUILDING".equals(mktsegment);
        }
    }

    // Order filter: orderdate < '1995-03-15'
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

    // LineItem filter: shipdate > '1995-03-15'
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

    /**
     * Stateful JOIN: Customer ⋈ Order
     * State management: Maintains customer and order states for delayed arrivals
     * Performance: Handles late arrival of either customer or order events
     */
    public static class CustomerOrderJoinFunction extends CoProcessFunction<StreamEvent, StreamEvent, StreamEvent> {
        private transient ValueState<StreamEvent> customerState; // Customer state storage
        private transient MapState<Long, StreamEvent> orderState; // Order state storage (multiple orders per customer)

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
                // Process all waiting orders for this customer
                for (StreamEvent order : orderState.values()) {
                    if (order != null) {
                        emitJoinResult(customer, order, out);
                    }
                }
            } else { // Delete action
                customerState.clear();
                // Emit delete results for all related orders
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
                orderState.put((Long) order.values[1], order); // Store by orderkey
                StreamEvent customer = customerState.value();
                if (customer != null) {
                    emitJoinResult(customer, order, out);
                }
            } else { // Delete action
                orderState.remove((Long) order.values[1]);
                StreamEvent customer = customerState.value();
                if (customer != null) {
                    emitJoinResult(customer, order, out);
                }
            }
        }

        // Emit JOIN result with action propagation
        private void emitJoinResult(StreamEvent customer, StreamEvent order, Collector<StreamEvent> out) {
            String action = "Insert".equals(customer.action) && "Insert".equals(order.action) ? "Insert" : "Delete";

            StreamEvent result = new StreamEvent("customer_order_join", action, order.values[1],
                    new Object[] { (Long) customer.key, (Long) order.values[1], (Date) order.values[2],
                            (Integer) order.values[3] }, // custkey, orderkey, orderdate, shippriority
                    new String[] { "CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                    Math.max(customer.sequenceNumber, order.sequenceNumber));

            out.collect(result);
        }
    }

    /**
     * Stateful JOIN: Order ⋈ LineItem
     * State management: Maintains order and lineitem states for delayed arrivals
     * Performance: Handles multiple lineitems per order efficiently
     */
    public static class OrderLineItemJoinFunction extends CoProcessFunction<StreamEvent, StreamEvent, StreamEvent> {
        private transient ValueState<StreamEvent> orderState; // Order state storage
        private transient MapState<Integer, StreamEvent> lineItemState; // LineItem state storage (multiple per order)

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
                // Process all waiting lineitems for this order
                for (StreamEvent lineItem : lineItemState.values()) {
                    if (lineItem != null) {
                        emitJoinResult(orderJoin, lineItem, out);
                    }
                }
            } else { // Delete action
                orderState.clear();
                // Emit delete results for all related lineitems
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
                lineItemState.put((Integer) lineItem.values[1], lineItem); // Store by linenumber
                StreamEvent orderJoin = orderState.value();
                if (orderJoin != null) {
                    emitJoinResult(orderJoin, lineItem, out);
                }
            } else { // Delete action
                lineItemState.remove((Integer) lineItem.values[1]);
                StreamEvent orderJoin = orderState.value();
                if (orderJoin != null) {
                    emitJoinResult(orderJoin, lineItem, out);
                }
            }
        }

        // Emit JOIN result with revenue calculation
        private void emitJoinResult(StreamEvent orderJoin, StreamEvent lineItem, Collector<StreamEvent> out) {
            String action = "Insert".equals(orderJoin.action) && "Insert".equals(lineItem.action) ? "Insert" : "Delete";

            // Calculate revenue: extendedprice * (1 - discount)
            double revenue = (Double) lineItem.values[3] * (1.0 - (Double) lineItem.values[4]);

            StreamEvent result = new StreamEvent("final_join", action, orderJoin.values[1],
                    new Object[] { revenue, (Long) orderJoin.values[1], (Date) orderJoin.values[2],
                            (Integer) orderJoin.values[3] }, // revenue, orderkey, orderdate, shippriority
                    new String[] { "REVENUE", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY" },
                    Math.max(orderJoin.sequenceNumber, lineItem.sequenceNumber));

            out.collect(result);
        }
    }

    /**
     * Incremental aggregation function - SUM(revenue) GROUP BY (orderkey,
     * orderdate, shippriority)
     * Performance: Timer-based output every 2 seconds (faster than WebSocket
     * version)
     * State: Maintains running sum for each group
     */
    public static class IncrementalAggregateFunction
            extends
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<Tuple3<Long, Date, Integer>, StreamEvent, QueryResult> {
        private transient ValueState<Double> revenueState; // Running revenue sum
        private transient ValueState<Boolean> outputScheduled; // Timer scheduling flag
        private final long outputDelay = 2000; // 2 seconds output interval (faster for performance)

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

            double deltaRevenue = (Double) event.values[0]; // Revenue from JOIN result

            // Incremental aggregation: add for Insert, subtract for Delete
            if ("Insert".equals(event.action)) {
                currentRevenue += deltaRevenue;
            } else { // Delete
                currentRevenue -= deltaRevenue;
            }

            revenueState.update(currentRevenue);

            // Schedule timer for output if not already scheduled
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

            // Only output positive revenue results (filter empty aggregations)
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

            // Reset timer scheduling flag
            outputScheduled.update(false);
        }
    }

    /**
     * TopN function - ORDER BY revenue DESC, orderdate ASC LIMIT 20
     * Performance: Console output only (no WebSocket overhead)
     * Optimization: Faster output interval (3 seconds)
     */
    public static class TopNFunction
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Integer, QueryResult, QueryResult> {
        private transient java.util.List<QueryResult> allResults; // All results storage
        private transient long processedCount = 0;
        private transient long lastOutputTime = 0;
        private final long outputInterval = 3000; // 3 seconds output interval
        private final int topN = 20; // LIMIT 20

        @Override
        public void open(Configuration parameters) throws Exception {
            allResults = new java.util.ArrayList<>();
        }

        @Override
        public void processElement(QueryResult value, Context ctx, Collector<QueryResult> out) throws Exception {
            // Only keep positive revenue records
            if (Math.abs(value.revenue) > 0.01) {
                allResults.add(value);
                processedCount++;

                long currentTime = System.currentTimeMillis();
                if (currentTime - lastOutputTime > outputInterval) {
                    outputTopN(out);
                    lastOutputTime = currentTime;
                }

                // Register timer for final output
                ctx.timerService().registerProcessingTimeTimer(currentTime + outputInterval);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<QueryResult> out) throws Exception {
            outputTopN(out);
        }

        // Output TopN results to console (no WebSocket - performance optimized)
        private void outputTopN(Collector<QueryResult> out) {
            if (allResults.isEmpty())
                return;

            // Sort: revenue DESC, orderdate ASC (TPC-H Query 3 specification)
            allResults.sort(java.util.Comparator
                    .comparingDouble((QueryResult r) -> -r.revenue)
                    .thenComparing(r -> r.orderdate));

            // Output top-N results (LIMIT 20)
            int count = Math.min(topN, allResults.size());
            System.out.println("\n=== TPC-H Query 3 最终结果 (TOP " + count + ") ===");
            System.out.println("排名\tOrderKey\tRevenue\t\tOrderDate\tShipPriority");
            System.out.println("----\t--------\t-------\t\t---------\t------------");
            for (int i = 0; i < count; i++) {
                QueryResult result = allResults.get(i);
                System.out.printf("%2d\t%d\t%.2f\t%tF\t%d\n",
                        i + 1, result.orderkey, result.revenue, result.orderdate, result.shippriority);
                // No stream output for performance (console only)
                // out.collect(result);
            }
            System.out.println("=== 处理了 " + processedCount + " 条聚合记录 ===\n");
        }
    }
}