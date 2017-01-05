package com.arshan.couchbase.client;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.arshan.couchbase.model.Rant;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.google.gson.Gson;

public class ExampleClient {

	// private Bucket bucket;

	public static void main(String[] args) {

		Logger logger = Logger.getLogger("com.couchbase.client");
		logger.setLevel(Level.FINEST);
		for (Handler h : logger.getParent().getHandlers()) {
			if (h instanceof ConsoleHandler) {
				h.setLevel(Level.FINEST);
			}
		}

		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
				// this set the IO socket timeout globally, to 45s
				.socketConnectTimeout((int) TimeUnit.SECONDS.toMillis(45))
				// this sets the connection timeout for openBucket calls
				// globally (unless a particular call provides its own timeout)
				.connectTimeout(TimeUnit.SECONDS.toMillis(60)).build();
		// Initialize the Connection
		Cluster cluster = CouchbaseCluster.create(env, "127.0.0.1");
		// System.out.println(cluster.clusterManager("Administrator",
		// "password").info().raw());

		Bucket bucket = cluster.openBucket("customer360");
		System.out.println("connected");
		Gson gson = new Gson();
		Rant rant = new Rant(UUID.randomUUID(), "ArshanM", "It's name dude", "rant");
		JsonObject abdulSuboor = JsonObject.create().fromJson(gson.toJson(rant));
		bucket.upsert(JsonDocument.create(rant.getId().toString(), abdulSuboor));
		
		bucket.bucketManager().createN1qlPrimaryIndex(true, false);
			
		resultFromSimpleQuery(bucket);

		allResultsFromBucket(bucket);
		// Just close a single bucket
		bucket.close();

		// Disconnect and close all buckets
		cluster.disconnect();
		
	}

	private static void allResultsFromBucket(Bucket bucket) {
		JsonDocument doc;
		String statement = "SELECT * FROM customer360 WHERE username = $1";
		JsonArray values = JsonArray.empty().add("ArshanM");
		N1qlParams params = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS);
		ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(statement, values, params);
		N1qlQueryResult result = bucket.query(query);
		List<N1qlQueryRow> list = result.allRows();
		System.out.println(list.size());
		if (list.size() != 0) {
			for (N1qlQueryRow n1qlQueryRow : list) {
				System.out.println(n1qlQueryRow);
			}
		}
	}
	
	private static void resultFromSimpleQuery(Bucket bucket)
	{
		SimpleN1qlQuery simpleN1q1Query = N1qlQuery.simple("SELECT * FROM customer360");		
		N1qlQueryResult result = bucket.query(simpleN1q1Query);
		List<N1qlQueryRow> list = result.allRows();
		System.out.println("Simple Query result count : "+list.size());
		if (list.size() != 0) {
			for (N1qlQueryRow n1qlQueryRow : list) {
				System.out.println(n1qlQueryRow);
			}
		}
	}

}
