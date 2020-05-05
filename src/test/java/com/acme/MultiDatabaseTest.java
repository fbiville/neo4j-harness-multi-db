package com.acme;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class MultiDatabaseTest {

	private static final String DATABASE_NAME = "extra";

	@Rule
	public Neo4jRule neo4jRule = new Neo4jRule();

	@Before
	public void creates_extra_db() {
		neo4jRule.databaseManagementService().createDatabase(DATABASE_NAME);
	}

	@After
	public void deletes_extra_db() {
		neo4jRule.databaseManagementService().dropDatabase(DATABASE_NAME);
	}

	@Test
	public void creates_a_node() throws Exception {
		HttpResponse<String> response = HttpClient.newBuilder()
				.build()
				.send(
						HttpRequest.newBuilder()
								.uri(transactionUri())
								.header("Accept", "application/json")
								.header("Content-Type", "application/json")
								.POST(ofString("{\"statements\": [{\"statement\": \"CREATE ()\"}]}"))
								.build(),
						HttpResponse.BodyHandlers.ofString());

		assertThat(response.statusCode(), Matchers.equalTo(200));
		GraphDatabaseService extraDatabaseService = neo4jRule.databaseManagementService().database(DATABASE_NAME);
		try (Transaction unused = extraDatabaseService.beginTx();
			 Result result = unused.execute("MATCH (n) RETURN COUNT(n) AS count");
			 ResourceIterator<Long> iterator = result.columnAs("count").map(o -> (long) o)) {

			assertThat(iterator.hasNext(), is(true));
			assertThat(iterator.next(), equalTo(1L));
			assertThat(iterator.hasNext(), is(false));
		}
	}

	private URI transactionUri() {
		return neo4jRule.httpURI().resolve(String.format("/db/%s/tx/commit", DATABASE_NAME));
	}
}
