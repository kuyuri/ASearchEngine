package com.yxz.mySimpleDistributedCrawler.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  @author Yu 
 */
public class HBaseUtil {

	private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
	
	private static Configuration conf = null;

	/**   
     * 初始化配置   
    */
	static{
		conf = HBaseConfiguration.create();
	}
	       
	/**     
     * 插入一行记录     
     */        
    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception{   
    	HTable table = null;
        try {         
            table = new HTable(conf, tableName);         
            Put put = new Put(Bytes.toBytes(rowKey));         
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));         
            table.put(put);         
            logger.info("insert recored " + rowKey + " to table " + tableName +" ok.");         
        } catch (IOException e) {         
            e.printStackTrace();         
        } finally {
        	table.close();
        }
    }
	
    /**
     * get a record
     */
    public Result getRecord(String tableName, String row) throws IOException {
    	HTable table = null;
    	try {
    		table = new HTable(conf, tableName);
    		Get get = new Get(Bytes.toBytes(row));
    		Result result = table.get(get);
    		return result;
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		table.close();
    	}
    	return null;
    }
    
	/**     
     * 创建一张表     
     */        
    public static void createTable(String tableName, String... familys) throws Exception {
    	HBaseAdmin admin = null;
    	try {
	    	admin = new HBaseAdmin(conf);         
	        if (admin.tableExists(tableName)) {         
	            System.out.println("table already exists!");         
	        } 
	        else {         
	            HTableDescriptor tableDesc = new HTableDescriptor(tableName);         
	            for(int i=0; i<familys.length; i++){         
	                tableDesc.addFamily(new HColumnDescriptor(familys[i]));         
	            }         
	            admin.createTable(tableDesc);         
	            System.out.println("create table " + tableName + " ok.");         
	        }
    	} finally {
    		admin.close();
    	}
    } 
	
	
    /**
     * init all needed tables such as url-Table, document-Table...
     */
    public static void initTables() {
    	try {
	    	createTable(Constants.urlTableName, "info");
	    	createTable(Constants.documentTableName, "info");
	    	createTable(Constants.cacheDocumentTableName, "info");
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    /**     
     * 删除表     
     */        
    public static void deleteTable(String tableName) throws Exception {    
    	HBaseAdmin admin = null;
    	try {         
	        admin = new HBaseAdmin(conf);         
	        admin.disableTable(tableName);         
	        admin.deleteTable(tableName);         
	        System.out.println("delete table " + tableName + " ok.");               
    	} finally {
    		admin.close();
    	}
    }
    
    /**
     * clear all tables
     */
    public static void deleteTables() {
    	try {
	    	deleteTable("url");
	    	deleteTable("document");
	    	deleteTable("cachedocument");
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    
    
    public static void  main (String [] agrs) throws Exception {         
//    	initTables();
    }    

}
