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

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * @author jackpark
 * 
 */
public class PersistentSet {
	private DB database;
	private NavigableSet<String> treeSet;
	private boolean isClosed = true;
	
	/**
	 * 
	 * @param databasePath
	 * @param storeName
	 */
	public PersistentSet(String databasePath, String storeName) {
		try {
			File f = new File(databasePath);
			System.out.println("PS-0a "+f);
			database = DBMaker.fileDB(f)
					.checksumHeaderBypass()
					.make();
			System.out.println("PS-1 "+database);
			treeSet = (NavigableSet<String>)database.treeSet(storeName).createOrOpen();
			System.out.println("PS-2 "+treeSet.size());
			isClosed = false;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	/**
	 * Returns <code>true</code> if <code>val</code> is added
	 * @param val
	 * @return
	 */
	public boolean add(String val) {
		return treeSet.add(val);
	}
	
	public boolean contains(String val) {
		return treeSet.contains(val);
	}
	
	/**
	 * Iterator on the values
	 * @return
	 */
	public Iterator<String> iterator() {
		return treeSet.iterator();
	}
	
	public int size() {
		return treeSet.size();
	}
	
	/**
	 * Returns <code>true</code> if <code>val</code> is removed
	 * @param val
	 * @return
	 */
	public boolean remove(String val) {
		return treeSet.remove(val);
	}
	
	/**
	 * Important to close the database
	 */
	public void shutDown() {
		if (!isClosed) {
			System.out.println("PersistentSet shutting down");
			database.commit();
			database.close();
			System.out.println("PersistentSet closed");
			isClosed = true;
		}
	}


}
