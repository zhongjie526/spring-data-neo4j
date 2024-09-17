package com.neo4j.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Service
public class Neo4jDBService {

    @Autowired
    Driver driver;

    public void setReadOnlyDummy(String dbName) {
        try (var session = driver.session(SessionConfig.builder().withDatabase("system").build())) {
            session.run("""
                            CALL dbms.setConfigValue('server.databases.read_only',$db_name)
                    """, Map.of("db_name", dbName));

            log.info("DB {} set to read-only",dbName);
        }
    }

    public void resetReadOnlyDummy() {
        try (var session = driver.session(SessionConfig.builder().withDatabase("system").build())) {
            session.executeWriteWithoutResult(tx->{
                resetReadOnly(tx);
            });
        }
    }

    public void setReadOnly(String dbName){
        executeWriteWithSession("system",tx-> setReadOnly(tx,dbName));
    }

    public void resetReadOnly(){
        executeWriteWithSession("system",Neo4jDBService::resetReadOnly);
    }

    public void executeWriteWithSession(String database, Consumer<TransactionContext> transactionLogic){
        try (var session = driver.session(SessionConfig.builder().withDatabase(database).build())) {
            session.executeWriteWithoutResult(transactionLogic);
        }
    }

    public static void setReadOnly(TransactionContext tx,String dbName){
        tx.run("""
                            CALL dbms.setConfigValue('server.databases.read_only',$db_name)
                    """, Map.of("db_name", dbName));
        log.info("DB {} set to read-only",dbName);
    }

    public static void resetReadOnly(TransactionContext tx){
        tx.run("""
                            CALL dbms.setConfigValue('server.databases.read_only','')
                    """);
        log.info("DB read-only reset");
    }
}
