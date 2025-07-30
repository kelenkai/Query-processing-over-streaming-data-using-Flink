package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TPC-H Query 3 SQL Batch Implementation - Reference Version
 * Purpose: Provides correct baseline results using Flink Table API in batch
 * mode
 * Use Case: Verification and performance baseline
 */
public class TPCHQuery3SQL_Batch {

        public static void main(String[] args) throws Exception {
                // Record start time for performance measurement
                long startTime = System.currentTimeMillis();

                // 1. Create Flink execution environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1); // Single parallelism for deterministic results

                // 2. Create table environment in BATCH mode - Key difference from streaming
                EnvironmentSettings settings = EnvironmentSettings
                                .newInstance()
                                .inBatchMode() // CRITICAL: Batch execution mode for complete dataset processing
                                .build();
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

                // 3. Create customer table with filesystem connector
                tableEnv.executeSql(
                                "CREATE TABLE customer (" +
                                                "  c_custkey INT," +
                                                "  c_name STRING," +
                                                "  c_address STRING," +
                                                "  c_nationkey INT," +
                                                "  c_phone STRING," +
                                                "  c_acctbal DECIMAL(10,2)," +
                                                "  c_mktsegment STRING," + // Filter condition: 'BUILDING'
                                                "  c_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'customer.csv'," + // Input: 150K records
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
                                                "  o_orderdate DATE," + // Filter condition: < '1995-03-15'
                                                "  o_orderpriority STRING," +
                                                "  o_clerk STRING," +
                                                "  o_shippriority INT," +
                                                "  o_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'orders.csv'," + // Input: 1.5M records
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
                                                "  l_extendedprice DECIMAL(10,2)," + // Revenue calculation component
                                                "  l_discount DECIMAL(10,2)," + // Revenue calculation component
                                                "  l_tax DECIMAL(10,2)," +
                                                "  l_returnflag STRING," +
                                                "  l_linestatus STRING," +
                                                "  l_shipdate DATE," + // Filter condition: > '1995-03-15'
                                                "  l_commitdate DATE," +
                                                "  l_receiptdate DATE," +
                                                "  l_shipinstruct STRING," +
                                                "  l_shipmode STRING," +
                                                "  l_comment STRING" +
                                                ") WITH (" +
                                                "  'connector' = 'filesystem'," +
                                                "  'path' = 'lineitem.csv'," + // Input: 6M records (largest table)
                                                "  'format' = 'csv'," +
                                                "  'csv.ignore-parse-errors' = 'true'," +
                                                "  'csv.allow-comments' = 'true'" +
                                                ")");

                System.out.println("=== 开始执行 TPC-H Query 3 (SQL版本) ===");

                // 6. Execute standard TPC-H Query 3 with SQL
                String query = "SELECT " +
                                "    l_orderkey, " +
                                "    SUM(l_extendedprice * (1 - l_discount)) as revenue, " + // Revenue calculation
                                "    o_orderdate, " +
                                "    o_shippriority " +
                                "FROM customer, orders, lineitem " +
                                "WHERE " +
                                "    c_mktsegment = 'BUILDING' " + // Customer filter
                                "    AND c_custkey = o_custkey " + // Customer-Order JOIN
                                "    AND l_orderkey = o_orderkey " + // Order-LineItem JOIN
                                "    AND o_orderdate < DATE '1995-03-15' " + // Order date filter
                                "    AND l_shipdate > DATE '1995-03-15' " + // LineItem date filter
                                "GROUP BY " +
                                "    l_orderkey, " +
                                "    o_orderdate, " +
                                "    o_shippriority " +
                                "ORDER BY " +
                                "    revenue DESC, " + // Primary sort: highest revenue first
                                "    o_orderdate " + // Secondary sort: earliest date first
                                "LIMIT 20"; // Top-20 results only

                // Record query start time
                long queryStartTime = System.currentTimeMillis();

                Table result = tableEnv.sqlQuery(query);

                // 7. Print results (blocking operation in batch mode)
                System.out.println("=== 查询结果 ===");
                result.execute().print();

                // Record end time and calculate execution statistics
                long endTime = System.currentTimeMillis();

                // 8. Performance metrics calculation
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
