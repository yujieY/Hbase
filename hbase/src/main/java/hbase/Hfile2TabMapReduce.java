package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jboss.netty.handler.ssl.ImmediateExecutor;

public class Hfile2TabMapReduce extends Configured implements Tool {
	public static class Hfile2TabMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
		
		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			rowkey.set(Bytes.toBytes(str[0]));
			
			Put put = new Put(Bytes.toBytes(str[0]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(str[1]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(str[2]));
			
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
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//set map
		job.setMapperClass(Hfile2TabMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		//set reduce
		job.setReducerClass(PutSortReducer.class);
		
		HTable table = new HTable(getConf(), args[0]);
		//set Hfile output
		HFileOutputFormat2.configureIncrementalLoad(job, table );
		
		//submit job
		boolean b = job.waitForCompletion(true);
		if(!b){
			throw new Exception("error with the job!");
		}
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
		
		//load hfile
		loader.doBulkLoad(new Path(args[2]), table);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// set config
		Configuration conf = HBaseConfiguration.create();

		// submit job
		int status = ToolRunner.run(conf, new Hfile2TabMapReduce(), args);

		// exit
		System.exit(status);
	}
}
