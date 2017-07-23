package com.yxz.mySimpleDistributedCrawler.hspider;

import java.io.IOException;

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.spider.Fetcher;
import com.yxz.mySimpleDistributedCrawler.utils.Constants;
import com.yxz.mySimpleDistributedCrawler.utils.FetcherUtil;

/**
 *  @author Yu   
 */
public class HFetcher extends Configured implements Tool {
	
	private static final Logger logger = LoggerFactory.getLogger(HFetcher.class);
	
	private static Configuration conf = HBaseConfiguration.create();
	
	static class HFetcherMapper extends TableMapper<ImmutableBytesWritable, LongWritable> {

		private static final LongWritable lw = new LongWritable(1);
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			if(value.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("status"))) { 
				if("0".equals(new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))))) // put url whose status is 0 to reducer
					context.write(key, lw);
			}
		}
		
	}
	
	static class HFetcherPartioner extends Partitioner<ImmutableBytesWritable, LongWritable> {

		@Override
		public int getPartition(ImmutableBytesWritable key, LongWritable value,
				int numReducers) {
			String url = Bytes.toString(key.get());
			if(url.startsWith("https")) {
				url = url.substring(8);
			}
			else if(url.startsWith("http")) {
				url = url.substring(7);
			}
			if(url.contains("/")) {
				url = url.substring(0, url.indexOf('/'));
			}
			return url.hashCode() % numReducers;
		}
		
	}
	
	static class HFetcherReducer extends TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			ImmutableBytesWritable table = new ImmutableBytesWritable(Bytes.toBytes(Constants.documentTableName));
			ImmutableBytesWritable cacheTable = new ImmutableBytesWritable(Bytes.toBytes(Constants.cacheDocumentTableName));
			ImmutableBytesWritable urlTable = new ImmutableBytesWritable(Bytes.toBytes(Constants.urlTableName));
			String url = Bytes.toString(key.get());
			String htmlText = FetcherUtil.fetchHtml(url);
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("info"), Bytes.toBytes("html"), Bytes.toBytes(htmlText));
			Put put2 = new Put(key.get());
			put2.add(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes("1"));
			context.write(table, put);
			context.write(cacheTable, put);
			context.write(urlTable, put2);
			HSpider.getUrlBloomFilter().add(url);
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(conf, "HFetch");
		job.setJarByClass(HFetcher.class);
		job.setReducerClass(HFetcher.HFetcherReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob(Constants.urlTableName,
												scan,
												HFetcherMapper.class,
												ImmutableBytesWritable.class,             //map out argument
												LongWritable.class,                       //map out argument
												job);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 1 : 0;
	}

	public static void main(String[] args) {
		try {
			int returnCode = ToolRunner.run(new HFetcher(), args);
			System.exit(returnCode);
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
}
