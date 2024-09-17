package com.neo4j.demo;

import com.neo4j.demo.service.Neo4jDBService;
import com.neo4j.demo.service.Neo4jService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

	@Autowired
	Neo4jService neo4jService;
	@Autowired
	Neo4jDBService neo4jDBService;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		neo4jService.checkConnection();
//		neo4jService.runQuery("merge (:Baby {name:'Frank1'})","bryan");
//		neo4jService.runAutoCommitMulti("merge (:Baby {name:'Frank1'})",100,"bryan");

//		neo4jService.createBatch(1000,"bryan");
//		neo4jService.createBatchAuto(10000,"bryan");
//		neo4jService.deleteBulkApoc(10000,"bryan");
//		neo4jService.createBatchAutoAsync(10000,"bryan");
//		neo4jService.deleteBatch(1000,"bryan");
//		neo4jService.deleteBatchAuto(1000,"bryan");
//		neo4jService.deleteBulk(1000,"bryan");
//		neo4jService.deleteBulkApoc(10000,"bryan");
//		neo4jDBService.setReadOnly("bryan");
		neo4jDBService.resetReadOnly();

		Thread.sleep(20000);
		neo4jService.checkConnection();
	}
}
