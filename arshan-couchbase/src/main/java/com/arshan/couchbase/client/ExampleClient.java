package com.arshan.couchbase.client;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.arshan.couchbase.model.Rant;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.*;
import com.google.gson.Gson;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExampleClient {

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger("com.couchbase.client");
		logger.setLevel(Level.FINEST);
		for(Handler h : logger.getParent().getHandlers()) {
		    if(h instanceof ConsoleHandler){
		        h.setLevel(Level.FINEST);
		    }
		}

		
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
			    //this set the IO socket timeout globally, to 45s
			    .socketConnectTimeout((int) TimeUnit.SECONDS.toMillis(45))
			    //this sets the connection timeout for openBucket calls globally (unless a particular call provides its own timeout)
			    .connectTimeout(TimeUnit.SECONDS.toMillis(60))
			    .build();
		// Initialize the Connection
        Cluster cluster = CouchbaseCluster.create(env, "127.0.0.1");
        //System.out.println(cluster.clusterManager("Administrator", "password").info().raw());

        Bucket bucket = cluster.openBucket("customer360");
        System.out.println("connected");
        Gson gson = new Gson();
		Rant rant = new Rant(UUID.randomUUID(),"Arshan","It's name dude","rant");
		JsonObject abdulSuboor = JsonObject.create().fromJson(gson.toJson(rant));
		bucket.upsert(JsonDocument.create(rant.getId().toString(), abdulSuboor));
        
		// Just close a single bucket
		bucket.close();

		// Disconnect and close all buckets
		cluster.disconnect();

       /* // Create a JSON Document
        JsonObject abdulSuboor = JsonObject.create()
            .put("name", "AbdulSuboor")
            .put("email", "mas222r@gmail.com")
            .put("interests", JsonArray.from("Holy Grail", "African Swallows"));

        // Store the Document
        bucket.upsert(JsonDocument.create("u:mas_222r", abdulSuboor));
       
        // Load the Document and print it
        // Prints Content and Metadata of the stored Document
        System.out.println(bucket.get("u:mas_222r"));

        // Create a N1QL Primary Index (but ignore if it exists)
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(
            N1qlQuery.parameterized("SELECT name FROM default WHERE $1 IN interests",
            JsonArray.from("African Swallows"))
        );

        // Print each found Row
        for (N1qlQueryRow row : result) {
            // Prints {"name":"Arthur"}
            System.out.println(row);
        }*/
    }

	}

