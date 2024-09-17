package com.neo4j.demo.service;

import com.neo4j.demo.util.Measured;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.async.AsyncSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class Neo4jService {

    @Autowired
    Driver driver;

    public void checkConnection(){
        driver.verifyConnectivity();
        log.info("Connection established");
    }

    @Measured
    public void runQuery(String query, String dbName) {
        try {
            var result = driver.executableQuery(query)
                    .withConfig(QueryConfig.builder().withDatabase(dbName).build())
                    .execute();

            var records = result.records();

            records.forEach(System.out::println);

            var summary = result.summary();
            System.out.printf("The query %s returned %d records in %d ms.%n",
                    summary.query(), records.size(),
                    summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error("Something wrong with the query!");
        }
    }

    @Measured
    public void runAutoCommitMulti(String query, int runFreq, String dbName) {
        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
            for (int i=0; i<runFreq; i++) {
                session.run(query);
            }
        }
    }

    @Measured
    public void runAutoCommit(String query, String dbName) {
        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
            session.run(query);
        }
    }


    @Measured
    public void createBatch(int batchSize, String dbName) {
        // Generate a sequence of numbers
        List<Map> numbers = new ArrayList<>(batchSize);
        for (int i = 0; i< batchSize; i++) {
            numbers.add(Map.of("value", i));
        }

        driver.executableQuery("""
    UNWIND $numbers AS node
    MERGE (:Number {value: node.value})
    """)
                .withParameters(Map.of("numbers", numbers))
                .withConfig(QueryConfig.builder().withDatabase(dbName).build())
                .execute();
    }

    @Measured
    public void createBatchAuto(int batchSize, String dbName) {

        List<Map> numbers = new ArrayList<>(batchSize);
        for (int i = 0; i< batchSize; i++) {
            numbers.add(Map.of("value", i));
        }

        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
                session.run("""
                        UNWIND $numbers AS node
                        MERGE (:Number {value: node.value})
                """,Map.of("numbers", numbers));


        }
    }

    @SneakyThrows
    @Measured
    public void createBatchAutoAsync(int batchSize, String dbName) {

        List<Map> numbers = new ArrayList<>(batchSize);
        for (int i = 0; i< batchSize; i++) {
            numbers.add(Map.of("value", i));
        }

        var session = driver.session(AsyncSession.class,SessionConfig.builder().withDatabase(dbName).build());
        var query = """
               UNWIND $numbers AS node
               MERGE (:Number {value: node.value})
        """;

        var summary  = session.executeWriteAsync(tx -> tx.runAsync(query,Map.of("numbers", numbers))
                              .thenCompose(res -> res.forEachAsync(record -> {
                                 log.info(record.get(0).asString());
                              })))
                              .whenComplete((result,error) ->{
                                    session.closeAsync();
                              })
                              .toCompletableFuture();

//        System.out.println(summary.get());

    }

    @Measured
    public void deleteBatch(int batchSize, String dbName) {
        // Generate a sequence of numbers
        List<Map> numbers = new ArrayList<>(batchSize);
        for (int i = 0; i< batchSize; i++) {
            numbers.add(Map.of("value", i));
        }

        driver.executableQuery("""
    UNWIND $numbers AS node
    MATCH (a:Number {value: node.value})
    DELETE a
    """)
                .withParameters(Map.of("numbers", numbers))
                .withConfig(QueryConfig.builder().withDatabase(dbName).build())
                .execute();
    }

    @Measured
    public void deleteBatchAuto(int batchSize, String dbName) {
        // Generate a sequence of numbers
        List<Map> numbers = new ArrayList<>(batchSize);
        for (int i = 0; i< batchSize; i++) {
            numbers.add(Map.of("value", i));
        }

        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
        session.run("""
    UNWIND $numbers AS node
    MATCH (a:Number {value: node.value})
    DELETE a
    """,Map.of("numbers", numbers));
        }
    }

    @Measured
    public void deleteBulk(int batchSize, String dbName) {


        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
            session.run("""
            MATCH (a:Number )
            call { with a
            detach delete a
            } in transactions of $batch rows
            """,Map.of("batch", batchSize));
            }
    }

    @Measured
    public void deleteBulkApoc(int batchSize, String dbName) {

        try (var session = driver.session(SessionConfig.builder().withDatabase(dbName).build())) {
            session.run("""
            MATCH (a:Number )
            with collect(a) as x
            call apoc.periodic.commit( "
                UNWIND $nodes as n
                with sum(count{(n)--()}) as count_remaining, collect(n) as nn
                UNWIND nn as n
                OPTIONAL MATCH (n)-[r]-()
                with n,r,count_remaining
                LIMIT $limit
                delete r
                return count_remaining
            ", {limit:$batch, nodes:x}) yield updates, executions, runtime, batches, failedBatches, batchErrors, failedCommits, commitErrors
            UNWIND x as a
            delete a
            return updates, executions, runtime, batches
            """,Map.of("batch", batchSize));
        }
    }


    static void addPersonToOrganization(TransactionContext tx, String personName, String orgId) {
        tx.run("""
            MATCH (o:Organization {id: $orgId})
            MATCH (p:Person {name: $name})
            MERGE (p)-[:WORKS_FOR]->(o)
            """, Map.of("orgId", orgId, "name", personName)
        );
    }




}
