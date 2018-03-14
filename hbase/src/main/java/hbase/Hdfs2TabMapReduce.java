package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Hdfs2TabMapReduce extends Configured implements Tool{
	
	public static class Hdfs2Tabmapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		private ImmutableBytesWritable rowkey = new ImmutableBytesWritable(); 
				
		protected void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException {
			String[] str = value.toString().split("\t");
			
			Put put = new Put(Bytes.toBytes(str[0]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(str[1]));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(str[2]));
			
			rowkey.set(Bytes.toBytes(str[0]));
			context.write(rowkey, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//create job
		Job job = Job.getInstance(this.getConf(),this.getClass().getSimpleName());
		
		//set class
		job.setJarByClass(this.getClass());
		
		//set path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//set mapper
		job.setMapperClass(Hdfs2Tabmapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		//set reduce
		TableMapReduceUtil.initTableReducerJob(
				"user", 
				null, 
				job
				);
		job.setNumReduceTasks(0);
		
		boolean b = job.waitForCompletion(true);
		
		if(!b){
			throw new IOException("error with the job");
		}
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		//get config
		Configuration conf = HBaseConfiguration.create();
		
		//submit job
		int status = ToolRunner.run(conf, new Hdfs2TabMapReduce(), args);
		
		//exit
		System.exit(status);
	}
}
