package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

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
 * TPC-H Query 3 Flink Batch Implementation - DataStream Learning Version
 * Purpose: Learn DataStream API with familiar batch semantics
 * (pseudo-streaming)
 * Approach: Uses DataStream API but processes entire files like batch mode
 * Use Case: Transition from SQL to DataStream API understanding
 */
public class TPCHQuery3_BatchProcessor {

        // Data structure definitions for TPC-H tables
        public static class Customer {
                public int custkey; // Primary key for customer table
                public String name;
                public String address;
                public int nationkey;
                public String phone;
                public double acctbal;
                public String mktsegment; // Filter field: must be 'BUILDING'
                public String comment;

                public Customer() {
                }

                public Customer(int custkey, String name, String address, int nationkey,
                                String phone, double acctbal, String mktsegment, String comment) {
                        this.custkey = custkey;
                        this.name = name;
                        this.address = address;
                        this.nationkey = nationkey;
                        this.phone = phone;
                        this.acctbal = acctbal;
                        this.mktsegment = mktsegment;
                        this.comment = comment;
                }
        }

        public static class Order {
                public int orderkey; // Primary key, JOIN key with lineitem
                public int custkey; // Foreign key, JOIN key with customer
                public String orderstatus;
                public double totalprice;
                public Date orderdate; // Filter field: must be < '1995-03-15'
                public String orderpriority;
                public String clerk;
                public int shippriority; // Output field
                public String comment;

                public Order() {
                }
        }

        public static class LineItem {
                public int orderkey; // Foreign key, JOIN key with orders
                public int partkey;
                public int suppkey;
                public int linenumber;
                public double quantity;
                public double extendedprice; // Revenue calculation: price * (1 - discount)
                public double discount; // Revenue calculation component
                public double tax;
                public String returnflag;
                public String linestatus;
                public Date shipdate; // Filter field: must be > '1995-03-15'
                public Date commitdate;
                public Date receiptdate;
                public String shipinstruct;
                public String shipmode;
                public String comment;

                public LineItem() {
                }
        }

        // Query result structure matching TPC-H Query 3 output
        public static class QueryResult {
                public int orderkey; // Grouping key
                public double revenue; // Aggregated: SUM(extendedprice * (1 - discount))
                public Date orderdate; // Grouping key
                public int shippriority; // Grouping key

                public QueryResult() {
                }

                public QueryResult(int orderkey, double revenue, Date orderdate, int shippriority) {
                        this.orderkey = orderkey;
                        this.revenue = revenue;
                        this.orderdate = orderdate;
                        this.shippriority = shippriority;
                }

                @Override
                public String toString() {
                        return String.format("OrderKey: %d, Revenue: %.2f, OrderDate: %s, ShipPriority: %d",
                                        orderkey, revenue, orderdate, shippriority);
                }
        }

        public static void main(String[] args) throws Exception {
                // Performance measurement start
                long startTime = System.currentTimeMillis();

                // 1. Create Flink StreamExecutionEnvironment (though processing is batch-like)
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // Parallelism configuration from command line (default: 8)
                int parallelism = 8;
                if (args.length > 0) {
                        parallelism = Integer.parseInt(args[0]);
                }
                env.setParallelism(parallelism);
                System.out.println("=== 使用并行度: " + parallelism + " ===");

                // 2. Create data sources - BATCH-LIKE: reads entire files at startup
                // Parallelism=1 to avoid duplicate reading across multiple instances
                DataStream<Customer> customerStream = env.addSource(new CustomerSource()).setParallelism(1);
                DataStream<Order> orderStream = env.addSource(new OrderSource()).setParallelism(1);
                DataStream<LineItem> lineItemStream = env.addSource(new LineItemSource()).setParallelism(1);

                // 3. Filter customers with 'BUILDING' market segment
                DataStream<Customer> buildingCustomers = customerStream
                                .filter((FilterFunction<Customer>) customer -> "BUILDING".equals(customer.mktsegment));

                // 4. Filter orders with orderdate < '1995-03-15'
                DataStream<Order> filteredOrders = orderStream
                                .filter((FilterFunction<Order>) order -> order.orderdate
                                                .before(new SimpleDateFormat("yyyy-MM-dd").parse("1995-03-15")));

                // 5. Filter lineitem with shipdate > '1995-03-15'
                DataStream<LineItem> filteredLineItems = lineItemStream
                                .filter((FilterFunction<LineItem>) lineItem -> lineItem.shipdate
                                                .after(new SimpleDateFormat("yyyy-MM-dd").parse("1995-03-15")));

                // 6. Distributed JOIN: customer ⋈ order (using stateful CoProcessFunction)
                DataStream<Tuple3<Integer, Date, Integer>> customerOrderJoin = buildingCustomers
                                .keyBy(c -> c.custkey) // Key by customer key
                                .connect(filteredOrders.keyBy(o -> o.custkey)) // Key by customer key
                                .process(new org.apache.flink.streaming.api.functions.co.CoProcessFunction<Customer, Order, Tuple3<Integer, Date, Integer>>() {
                                        // State to store order and customer data for JOIN
                                        private transient org.apache.flink.api.common.state.ValueState<Order> orderState;
                                        private transient org.apache.flink.api.common.state.ValueState<Customer> customerState;

                                        @Override
                                        public void open(org.apache.flink.configuration.Configuration parameters) {
                                                // Initialize state descriptors for JOIN operation
                                                org.apache.flink.api.common.state.ValueStateDescriptor<Order> orderDescriptor = new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                                                "orderState", Order.class);
                                                orderState = getRuntimeContext().getState(orderDescriptor);
                                                org.apache.flink.api.common.state.ValueStateDescriptor<Customer> customerDescriptor = new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                                                "customerState", Customer.class);
                                                customerState = getRuntimeContext().getState(customerDescriptor);
                                        }

                                        @Override
                                        public void processElement1(Customer customer, Context ctx,
                                                        org.apache.flink.util.Collector<Tuple3<Integer, Date, Integer>> out)
                                                        throws Exception {
                                                customerState.update(customer);
                                                Order order = orderState.value();
                                                if (order != null) {
                                                        // Emit JOIN result when both customer and order are available
                                                        out.collect(new Tuple3<>(order.orderkey, order.orderdate,
                                                                        order.shippriority));
                                                }
                                        }

                                        @Override
                                        public void processElement2(Order order, Context ctx,
                                                        org.apache.flink.util.Collector<Tuple3<Integer, Date, Integer>> out)
                                                        throws Exception {
                                                orderState.update(order);
                                                Customer customer = customerState.value();
                                                if (customer != null) {
                                                        // Emit JOIN result when both customer and order are available
                                                        out.collect(new Tuple3<>(order.orderkey, order.orderdate,
                                                                        order.shippriority));
                                                }
                                        }
                                });

                // 7. Distributed JOIN: order ⋈ lineitem (using stateful CoProcessFunction)
                DataStream<QueryResult> orderLineItemJoin = customerOrderJoin
                                .keyBy(t -> t.f0) // Key by order key
                                .connect(filteredLineItems.keyBy(l -> l.orderkey)) // Key by order key
                                .process(new org.apache.flink.streaming.api.functions.co.CoProcessFunction<Tuple3<Integer, Date, Integer>, LineItem, QueryResult>() {
                                        // State management for JOIN operation
                                        private transient org.apache.flink.api.common.state.ValueState<Tuple3<Integer, Date, Integer>> orderState;
                                        private transient org.apache.flink.api.common.state.MapState<Integer, LineItem> lineItemState;

                                        @Override
                                        public void open(org.apache.flink.configuration.Configuration parameters) {
                                                // State for order information
                                                org.apache.flink.api.common.state.ValueStateDescriptor<Tuple3<Integer, Date, Integer>> orderDescriptor = new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                                                "orderState",
                                                                org.apache.flink.api.common.typeinfo.Types.TUPLE(
                                                                                org.apache.flink.api.common.typeinfo.Types.INT,
                                                                                org.apache.flink.api.common.typeinfo.Types.SQL_DATE,
                                                                                org.apache.flink.api.common.typeinfo.Types.INT));
                                                orderState = getRuntimeContext().getState(orderDescriptor);

                                                // State for lineitem information (multiple lineitems per order)
                                                org.apache.flink.api.common.state.MapStateDescriptor<Integer, LineItem> lineItemDescriptor = new org.apache.flink.api.common.state.MapStateDescriptor<>(
                                                                "lineItemState", Integer.class, LineItem.class);
                                                lineItemState = getRuntimeContext().getMapState(lineItemDescriptor);
                                        }

                                        @Override
                                        public void processElement1(Tuple3<Integer, Date, Integer> orderInfo,
                                                        Context ctx, org.apache.flink.util.Collector<QueryResult> out)
                                                        throws Exception {
                                                orderState.update(orderInfo);
                                                // Process all waiting lineitems for this order
                                                for (LineItem lineItem : lineItemState.values()) {
                                                        double revenue = lineItem.extendedprice
                                                                        * (1 - lineItem.discount); // Revenue
                                                                                                   // calculation
                                                        out.collect(new QueryResult(orderInfo.f0, revenue, orderInfo.f1,
                                                                        orderInfo.f2));
                                                }
                                        }

                                        @Override
                                        public void processElement2(LineItem lineItem, Context ctx,
                                                        org.apache.flink.util.Collector<QueryResult> out)
                                                        throws Exception {
                                                Tuple3<Integer, Date, Integer> orderInfo = orderState.value();
                                                if (orderInfo != null) {
                                                        // Calculate revenue and emit result
                                                        double revenue = lineItem.extendedprice
                                                                        * (1 - lineItem.discount);
                                                        out.collect(new QueryResult(orderInfo.f0, revenue, orderInfo.f1,
                                                                        orderInfo.f2));
                                                } else {
                                                        // Store lineitem for later processing when order arrives
                                                        lineItemState.put(lineItem.orderkey, lineItem);
                                                }
                                        }
                                });

                // 8. Distributed aggregation: GROUP BY (orderkey, orderdate, shippriority)
                DataStream<QueryResult> aggregatedResults = orderLineItemJoin
                                .keyBy(new KeySelector<QueryResult, Tuple3<Integer, Date, Integer>>() {
                                        @Override
                                        public Tuple3<Integer, Date, Integer> getKey(QueryResult result) {
                                                return new Tuple3<>(result.orderkey, result.orderdate,
                                                                result.shippriority);
                                        }
                                })
                                .reduce((value1, value2) -> new QueryResult( // SUM aggregation
                                                value1.orderkey,
                                                value1.revenue + value2.revenue, // Revenue summation
                                                value1.orderdate,
                                                value1.shippriority));

                // 9. TopN implementation: collect all results and sort (ORDER BY revenue DESC,
                // orderdate)
                DataStream<QueryResult> sortedResults = aggregatedResults
                                .keyBy(r -> 0) // Send all data to single partition for global sorting
                                .process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction<Integer, QueryResult, QueryResult>() {
                                        private transient java.util.List<QueryResult> allResults;
                                        private transient long processedCount = 0;
                                        private transient long lastOutputTime = 0;
                                        private final long outputInterval = 3000; // Output every 3 seconds
                                        private final int topN = 20; // LIMIT 20

                                        @Override
                                        public void open(org.apache.flink.configuration.Configuration parameters) {
                                                allResults = new java.util.ArrayList<>();
                                        }

                                        @Override
                                        public void processElement(QueryResult value, Context ctx,
                                                        org.apache.flink.util.Collector<QueryResult> out)
                                                        throws Exception {
                                                allResults.add(value);
                                                processedCount++;

                                                long currentTime = System.currentTimeMillis();
                                                if (currentTime - lastOutputTime > outputInterval) {
                                                        outputTopN(out);
                                                        lastOutputTime = currentTime;
                                                }

                                                // Register timer for final output
                                                ctx.timerService().registerProcessingTimeTimer(
                                                                currentTime + outputInterval);
                                        }

                                        @Override
                                        public void onTimer(long timestamp, OnTimerContext ctx,
                                                        org.apache.flink.util.Collector<QueryResult> out)
                                                        throws Exception {
                                                outputTopN(out);
                                        }

                                        private void outputTopN(org.apache.flink.util.Collector<QueryResult> out) {
                                                if (allResults.isEmpty())
                                                        return;

                                                // Sort: revenue DESC, orderdate ASC (TPC-H Query 3 specification)
                                                allResults.sort(java.util.Comparator
                                                                .comparingDouble((QueryResult r) -> -r.revenue)
                                                                .thenComparing(r -> r.orderdate));

                                                // Output top-N results (LIMIT 20)
                                                int count = Math.min(topN, allResults.size());
                                                for (int i = 0; i < count; i++) {
                                                        out.collect(allResults.get(i));
                                                }

                                                System.out.println("=== 当前TopN结果 (处理了 " + processedCount + " 条记录) ===");
                                        }
                                });

                // 11. Print final TopN results
                sortedResults.print("最终TopN:");

                // Record query start time
                long queryStartTime = System.currentTimeMillis();

                // 12. Execute Flink job (blocking call)
                env.execute("TPC-H Query 3 with 分布式 TopN");

                // Calculate and display performance metrics
                long endTime = System.currentTimeMillis();
                long setupTime = queryStartTime - startTime;
                long queryTime = endTime - queryStartTime;
                long totalTime = endTime - startTime;

                System.out.println("\n=== DataStream API 执行时间统计 ===");
                System.out.println("并行度: " + parallelism);
                System.out.println("环境设置时间: " + setupTime + " ms");
                System.out.println("查询执行时间: " + queryTime + " ms");
                System.out.println("总执行时间: " + totalTime + " ms");
                System.out.println("总执行时间: " + (totalTime / 1000.0) + " 秒");
        }

        // CSV Data Source Implementations - BATCH-LIKE: read entire files at startup

        /**
         * Customer data source - reads entire customer.csv file
         * Input: 150,000 customer records
         */
        public static class CustomerSource implements SourceFunction<Customer> {
                private volatile boolean isRunning = true;

                @Override
                public void run(SourceContext<Customer> ctx) throws Exception {
                        System.out.println("开始读取客户数据...");
                        try (BufferedReader reader = new BufferedReader(new FileReader("customer.csv"))) {
                                String line;
                                boolean isFirstLine = true;
                                int count = 0;
                                while ((line = reader.readLine()) != null && isRunning) {
                                        if (isFirstLine) {
                                                isFirstLine = false;
                                                System.out.println("客户表头行: " + line);
                                                continue; // Skip header row
                                        }

                                        String[] fields = parseCSVLine(line);
                                        if (fields.length >= 8) {
                                                try {
                                                        Customer customer = new Customer(
                                                                        Integer.parseInt(fields[0].trim()), // custkey
                                                                        fields[1].trim(), // name
                                                                        fields[2].trim(), // address
                                                                        Integer.parseInt(fields[3].trim()), // nationkey
                                                                        fields[4].trim(), // phone
                                                                        Double.parseDouble(fields[5].trim()), // acctbal
                                                                        fields[6].trim(), // mktsegment
                                                                        fields[7].trim()); // comment
                                                        ctx.collect(customer);
                                                        count++;
                                                        if (count <= 3) {
                                                                System.out.println("读取客户: " + customer.custkey + " - "
                                                                                + customer.mktsegment);
                                                        }
                                                } catch (NumberFormatException e) {
                                                        System.err.println("跳过无效行: " + line);
                                                        System.err.println("错误: " + e.getMessage());
                                                }
                                        }
                                }
                                System.out.println("总共读取客户数: " + count);
                        }
                }

                // CSV parsing utility
                private String[] parseCSVLine(String line) {
                        java.util.List<String> fields = new java.util.ArrayList<>();
                        boolean inQuotes = false;
                        StringBuilder field = new StringBuilder();

                        for (int i = 0; i < line.length(); i++) {
                                char c = line.charAt(i);
                                if (c == '"') {
                                        inQuotes = !inQuotes;
                                } else if (c == ',' && !inQuotes) {
                                        fields.add(field.toString());
                                        field = new StringBuilder();
                                } else {
                                        field.append(c);
                                }
                        }
                        fields.add(field.toString());
                        return fields.toArray(new String[0]);
                }

                @Override
                public void cancel() {
                        isRunning = false;
                }
        }

        /**
         * Orders data source - reads entire orders.csv file
         * Input: 1,500,000 order records
         */
        public static class OrderSource implements SourceFunction<Order> {
                private volatile boolean isRunning = true;

                @Override
                public void run(SourceContext<Order> ctx) throws Exception {
                        System.out.println("开始读取订单数据...");
                        try (BufferedReader reader = new BufferedReader(new FileReader("orders.csv"))) {
                                String line;
                                boolean isFirstLine = true;
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // Date parser
                                int count = 0;
                                while ((line = reader.readLine()) != null && isRunning) {
                                        if (isFirstLine) {
                                                isFirstLine = false;
                                                System.out.println("订单表头行: " + line);
                                                continue; // Skip header row
                                        }

                                        String[] fields = parseCSVLine(line);
                                        if (fields.length >= 9) {
                                                try {
                                                        Order order = new Order();
                                                        order.orderkey = Integer.parseInt(fields[0].trim());
                                                        order.custkey = Integer.parseInt(fields[1].trim()); // JOIN key
                                                        order.orderstatus = fields[2].trim();
                                                        order.totalprice = Double.parseDouble(fields[3].trim());
                                                        order.orderdate = sdf.parse(fields[4].trim()); // Filter field
                                                        order.orderpriority = fields[5].trim();
                                                        order.clerk = fields[6].trim();
                                                        order.shippriority = Integer.parseInt(fields[7].trim());
                                                        order.comment = fields[8].trim();
                                                        ctx.collect(order);
                                                        count++;
                                                        if (count <= 3) {
                                                                System.out.println("读取订单: " + order.orderkey + " - "
                                                                                + sdf.format(order.orderdate));
                                                        }
                                                } catch (Exception e) {
                                                        System.err.println("跳过无效订单行: " + line);
                                                        System.err.println("错误: " + e.getMessage());
                                                }
                                        }
                                }
                                System.out.println("总共读取订单数: " + count);
                        }
                }

                // CSV parsing utility (same as CustomerSource)
                private String[] parseCSVLine(String line) {
                        java.util.List<String> fields = new java.util.ArrayList<>();
                        boolean inQuotes = false;
                        StringBuilder field = new StringBuilder();

                        for (int i = 0; i < line.length(); i++) {
                                char c = line.charAt(i);
                                if (c == '"') {
                                        inQuotes = !inQuotes;
                                } else if (c == ',' && !inQuotes) {
                                        fields.add(field.toString());
                                        field = new StringBuilder();
                                } else {
                                        field.append(c);
                                }
                        }
                        fields.add(field.toString());
                        return fields.toArray(new String[0]);
                }

                @Override
                public void cancel() {
                        isRunning = false;
                }
        }

        /**
         * LineItem data source - reads entire lineitem.csv file
         * Input: 6,001,215 lineitem records (largest table)
         */
        public static class LineItemSource implements SourceFunction<LineItem> {
                private volatile boolean isRunning = true;

                @Override
                public void run(SourceContext<LineItem> ctx) throws Exception {
                        System.out.println("开始读取行项目数据...");
                        try (BufferedReader reader = new BufferedReader(new FileReader("lineitem.csv"))) {
                                String line;
                                boolean isFirstLine = true;
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // Date parser
                                int count = 0;
                                while ((line = reader.readLine()) != null && isRunning) {
                                        if (isFirstLine) {
                                                isFirstLine = false;
                                                System.out.println("行项目表头行: " + line);
                                                continue; // Skip header row
                                        }

                                        String[] fields = parseCSVLine(line);
                                        if (fields.length >= 16) {
                                                try {
                                                        LineItem lineItem = new LineItem();
                                                        lineItem.orderkey = Integer.parseInt(fields[0].trim()); // JOIN
                                                                                                                // key
                                                        lineItem.partkey = Integer.parseInt(fields[1].trim());
                                                        lineItem.suppkey = Integer.parseInt(fields[2].trim());
                                                        lineItem.linenumber = Integer.parseInt(fields[3].trim());
                                                        lineItem.quantity = Double.parseDouble(fields[4].trim());
                                                        lineItem.extendedprice = Double.parseDouble(fields[5].trim()); // Revenue
                                                                                                                       // calc
                                                        lineItem.discount = Double.parseDouble(fields[6].trim()); // Revenue
                                                                                                                  // calc
                                                        lineItem.tax = Double.parseDouble(fields[7].trim());
                                                        lineItem.returnflag = fields[8].trim();
                                                        lineItem.linestatus = fields[9].trim();
                                                        lineItem.shipdate = sdf.parse(fields[10].trim()); // Filter
                                                                                                          // field
                                                        lineItem.commitdate = sdf.parse(fields[11].trim());
                                                        lineItem.receiptdate = sdf.parse(fields[12].trim());
                                                        lineItem.shipinstruct = fields[13].trim();
                                                        lineItem.shipmode = fields[14].trim();
                                                        lineItem.comment = fields[15].trim();
                                                        ctx.collect(lineItem);
                                                        count++;
                                                        if (count <= 3) {
                                                                System.out.println("读取行项目: " + lineItem.orderkey + " - "
                                                                                + sdf.format(lineItem.shipdate));
                                                        }
                                                } catch (Exception e) {
                                                        System.err.println("跳过无效行项目行: " + line);
                                                        System.err.println("错误: " + e.getMessage());
                                                }
                                        }
                                }
                                System.out.println("总共读取行项目数: " + count);
                        }
                }

                // CSV parsing utility (same as other sources)
                private String[] parseCSVLine(String line) {
                        java.util.List<String> fields = new java.util.ArrayList<>();
                        boolean inQuotes = false;
                        StringBuilder field = new StringBuilder();

                        for (int i = 0; i < line.length(); i++) {
                                char c = line.charAt(i);
                                if (c == '"') {
                                        inQuotes = !inQuotes;
                                } else if (c == ',' && !inQuotes) {
                                        fields.add(field.toString());
                                        field = new StringBuilder();
                                } else {
                                        field.append(c);
                                }
                        }
                        fields.add(field.toString());
                        return fields.toArray(new String[0]);
                }

                @Override
                public void cancel() {
                        isRunning = false;
                }
        }
}