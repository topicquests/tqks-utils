/*
 * Copyright 2019, TopicQuests
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.topicquests.ks.utils;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerCompressionWrapper;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

/**
 * @author jackpark
 *
 */
public class PersistentMap {
	private DB database;
	private HTreeMap<String, String> map;
	private boolean isClosed = true;

	/**
	 * @param databasePath e.g. data/mydata
	 * @param storeName e.g. mystore
	 */
	public PersistentMap(String databasePath, String storeName) {
		try {
			File f = new File(databasePath);
			System.out.println("PS-0a "+f);
			database = DBMaker.fileDB(f)
					.checksumHeaderBypass()
					.make();
			map = database.hashMap(storeName)
	        	.keySerializer(Serializer.STRING)
	        	.valueSerializer(Serializer.STRING)
	        	.createOrOpen();
			isClosed = false;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	/**
	 * Returns <code>true</code> if this overwrites a previous value
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean put(String key, String value) {
		Object o = map.put(key, value);
		return o != null;
	}
	
	/**
	 * Can return <code>null</code>
	 * @param key
	 * @return
	 */
	public Object get(String key) {
		return map.get(key);
	}
	
	public boolean containsKey(String key) {
		return map.get(key) != null;
	}
	
	public int size() {
		return map.size();
	}
	
	/**
	 * Utility function for dealing with JSON
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean putJSON(String key, JSONObject value) {
		return put(key, value.toJSONString());
	}
	
	/**
	 * Utility function for dealing with JSON
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public JSONObject getJSON(String key) throws Exception {
		String json = (String)map.get(key);
		if (json == null)
			return null;
		JSONParser p = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
		return (JSONObject)p.parse(json);
	}
	
	public Object remove(String key) {
		return map.remove(key);
	}
	
	/**
	 * Important to close this database
	 */
	public void shutDown() {
		if (!isClosed) {
			System.out.println("PersistentMap shutting down");
			database.commit();
			database.close();
			System.out.println("PersistentMap closed");
			isClosed = true;
		}
	}
	

}
