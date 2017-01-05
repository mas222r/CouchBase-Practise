package com.arshan.couchbase.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.arshan.couchbase.model.Rant;
import com.couchbase.client.CouchbaseClient;
import com.google.gson.Gson;



public class ArshanCouchBaseClient {

	public static void main(String[] args){
		
		 
		
		List<URI> nodes;
		try {
			//nodes = Arrays.asList(new URI("http://localhost:8091/pools/default/buckets"));	
			//"http://127.0.0.1:8091/pools"
			nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
			CouchbaseClient client = new CouchbaseClient(nodes, "default", "password");
			Gson gson = new Gson();
			Rant rant = new Rant(UUID.randomUUID(),"Arshanm","It's name dude","rant");
			Boolean success = client.set(rant.getId().toString(), gson.toJson(rant)).get();	
			System.out.println(success.booleanValue());
			
			String json = (String)client.get("f77ccf4f-66bd-402a-9a5b-ea5bc36a11c4");
			Rant rantReturn = gson.fromJson(json, Rant.class);
			System.out.println(rantReturn.getId());
			System.out.println(rantReturn.getType());
			System.out.println(rantReturn.getRanttext());
			System.out.println(rantReturn.getUsername());
			client.shutdown();
		} catch (InterruptedException | ExecutionException | URISyntaxException | IOException e) {
			
			e.printStackTrace();
		}
	}

}
