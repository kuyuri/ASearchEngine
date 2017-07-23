package com.yxz.mySimpleDistributedCrawler.hspider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.spider.Fetcher;
import com.yxz.mySimpleDistributedCrawler.utils.BloomFilterRedisUtil;
import com.yxz.mySimpleDistributedCrawler.utils.Constants;
import com.yxz.mySimpleDistributedCrawler.utils.ParserUtil;
import com.yxz.mySimpleDistributedCrawler.utils.RedisUtil;

/**
 *  @author Yu   
 */
public class HParser extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(HFetcher.class);
	
	private static Configuration conf = HBaseConfiguration.create();
	
	static class HParserMapper extends TableMapper<ImmutableBytesWritable, Put> {
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			if(value.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("html"))) { 
				byte[] keyBytes = key.get();
				String html = Bytes.toString(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("html")));
				Set<String> urlSets = ParserUtil.parseUrl(html);
//				String urls = urlSets.toString();
//				urls = urls.substring(1, urls.length() - 1);
//				Put put = new Put(keyBytes);
				for(String url : urlSets) {
					if(HSpider.getUrlBloomFilter().contains(url)) {
						continue;
					}
					Put put = new Put(Bytes.toBytes(url));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes("0"));
					context.write(key, put);
				}
				
//				put.add(Bytes.toBytes("info"), Bytes.toBytes("outlinks"), Bytes.toBytes(urls));
//				put.add(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes("1"));
			}
		}
		
	}
	
	//no reducer
	
	@Override
	public int run(String[] arg0) throws Exception {
		 Job job = Job.getInstance(conf, "HParser");
		 job.setJarByClass(HParser.class);
		 job.setOutputFormatClass(TableOutputFormat.class);
		 job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
		 Constants.urlTableName);
		 job.setOutputKeyClass(ImmutableBytesWritable.class);
		 job.setOutputValueClass(Put.class);
		 job.setNumReduceTasks(0);
		 Scan scan = new Scan();
	     scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
	     scan.setCacheBlocks(false);  // don't set to true for MR jobs
		 TableMapReduceUtil.initTableMapperJob(Constants.cacheDocumentTableName,
				 								scan,
				 								HParserMapper.class,
				 								ImmutableBytesWritable.class,                 //map out argument
				 								Put.class,                                    //map out argument       
				 								job);
		 job.waitForCompletion(true);
		 return job.isSuccessful() ? 1 : 0;
	}

	public static void main(String[] args) {
		try {
			int returnCode = ToolRunner.run(new HParser(), args);
			System.exit(returnCode);
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
}
