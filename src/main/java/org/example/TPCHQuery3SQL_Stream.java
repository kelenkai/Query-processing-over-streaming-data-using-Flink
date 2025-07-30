package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TPC-H Query 3 SQL Stream Implementation - Experimental Version
 * Purpose: Explores SQL-based streaming capabilities and limitations
 * Use Case: Understanding Flink Table API streaming constraints for complex
 * queries
 */
public class TPCHQuery3SQL_Stream {

        public static void main(String[] args) throws Exception {
                // Record start time for performance measurement
                long startTime = System.currentTimeMillis();

                // 1. Create Flink execution environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1); // Single parallelism for simplified testing

                // 2. Create table environment in STREAMING mode - Key difference from batch
                EnvironmentSettings settings = EnvironmentSettings
                                .newInstance()
                                .inStreamingMode() // EXPERIMENTAL: SQL streaming with limited capabilities
                                .build();
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

                // 3. Create customer table (same schema as batch version)
                tableEnv.executeSql(
                                "CREATE TABLE customer (" +
                                                "  c_custkey INT," +
                                                "  c_name STRING," +
                                                "  c_address STRING," +
                                                "  c_nationkey INT," +
                                                "  c_phone STRING," +
                                                "  c_acctbal DECIMAL(10,2)," +
                                                "  c_mktsegment STRING," + // Filter: 'BUILDING' segment
                                                "  c_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'customer.csv'," +
                                                "  'format' = 'csv'," +
                                                "  'csv.ignore-parse-errors' = 'true'," +
                                                "  'csv.allow-comments' = 'true'" +
                                                ")");

                // 4. Create orders table
                tableEnv.executeSql(
                                "CREATE TABLE orders (" +
                                                "  o_orderkey INT," +
                                                "  o_custkey INT," + // JOIN key with customer
                                                "  o_orderstatus STRING," +
                                                "  o_totalprice DECIMAL(10,2)," +
                                                "  o_orderdate DATE," + // Filter: < '1995-03-15'
                                                "  o_orderpriority STRING," +
                                                "  o_clerk STRING," +
                                                "  o_shippriority INT," +
                                                "  o_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'orders.csv'," +
                                                "  'format' = 'csv'," +
                                                "  'csv.ignore-parse-errors' = 'true'," +
                                                "  'csv.allow-comments' = 'true'" +
                                                ")");

                // 5. Create lineitem table
                tableEnv.executeSql(
                                "CREATE TABLE lineitem (" +
                                                "  l_orderkey INT," + // JOIN key with orders
                                                "  l_partkey INT," +
                                                "  l_suppkey INT," +
                                                "  l_linenumber INT," +
                                                "  l_quantity DECIMAL(10,2)," +
                                                "  l_extendedprice DECIMAL(10,2)," + // Revenue component
                                                "  l_discount DECIMAL(10,2)," + // Revenue component
                                                "  l_tax DECIMAL(10,2)," +
                                                "  l_returnflag STRING," +
                                                "  l_linestatus STRING," +
                                                "  l_shipdate DATE," + // Filter: > '1995-03-15'
                                                "  l_commitdate DATE," +
                                                "  l_receiptdate DATE," +
                                                "  l_shipinstruct STRING," +
                                                "  l_shipmode STRING," +
                                                "  l_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'lineitem.csv'," +
                                                "  'format' = 'csv'," +
                                                "  'csv.ignore-parse-errors' = 'true'," +
                                                "  'csv.allow-comments' = 'true'" +
                                                ")");

                System.out.println("=== 开始执行 TPC-H Query 3 (SQL版本) ===");

                // 6. Execute TPC-H Query 3 in streaming mode (limited SQL streaming support)
                String query = "SELECT " +
                                "    l_orderkey, " +
                                "    SUM(l_extendedprice * (1 - l_discount)) as revenue, " + // Aggregation in streaming
                                "    o_orderdate, " +
                                "    o_shippriority " +
                                "FROM customer, orders, lineitem " +
                                "WHERE " +
                                "    c_mktsegment = 'BUILDING' " + // Static filter condition
                                "    AND c_custkey = o_custkey " + // Stream JOIN (challenging)
                                "    AND l_orderkey = o_orderkey " + // Stream JOIN (challenging)
                                "    AND o_orderdate < DATE '1995-03-15' " + // Date filter
                                "    AND l_shipdate > DATE '1995-03-15' " + // Date filter
                                "GROUP BY " +
                                "    l_orderkey, " +
                                "    o_orderdate, " +
                                "    o_shippriority " +
                                "ORDER BY revenue DESC, o_orderdate " + // ORDER BY in streaming (limited)
                                "LIMIT 20"; // LIMIT in streaming (limited)

                // Record query start time
                long queryStartTime = System.currentTimeMillis();

                Table result = tableEnv.sqlQuery(query);

                // 7. Print results (may have streaming limitations)
                System.out.println("=== 查询结果 ===");
                result.execute().print();

                // Record end time and calculate performance metrics
                long endTime = System.currentTimeMillis();

                // 8. Performance analysis for streaming SQL
                long setupTime = queryStartTime - startTime;
                long queryTime = endTime - queryStartTime;
                long totalTime = endTime - startTime;

                System.out.println("\n=== 执行时间统计 ===");
                System.out.println("环境设置时间: " + setupTime + " ms");
                System.out.println("查询执行时间: " + queryTime + " ms");
                System.out.println("总执行时间: " + totalTime + " ms");
                System.out.println("总执行时间: " + (totalTime / 1000.0) + " 秒");
        }
}
