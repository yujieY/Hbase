package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tab2TabMapReduce extends Configured implements Tool {

	// mapper class
	public static class TabMapper extends TableMapper<Text, Put> {
		private Text rowkey = new Text();
		
		protected void map(
				org.apache.hadoop.hbase.io.ImmutableBytesWritable key,
				org.apache.hadoop.hbase.client.Result value, Context context)
				throws java.io.IOException, InterruptedException {
			byte[] bytes = key.get();
			rowkey.set(Bytes.toString(bytes));
			
			Put put = new Put(bytes);
			
			for (Cell cell: value.rawCells()){
				//add cell
				if("info".equals(CellUtil.cloneFamily(cell))){
					if ("name".equals(CellUtil.cloneQualifier(cell))){
						put.add(cell);
					}
				}
			}
		}
	}

	// reduce class
	public static class TabReduce extends
			TableReducer<Text, Put, ImmutableBytesWritable> {
		protected void reduce(Text key, java.lang.Iterable<Put> values,
				Context context) throws java.io.IOException,
				InterruptedException {
			for (Put put: values){
				context.write(null, put);
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//create job
		Job job = Job.getInstance(this.getConf(),this.getClass().getSimpleName());
		
		//set run class
		job.setJarByClass(getClass());
		
		Scan scan = new Scan();
		scan.setCaching(500);//从服务器端读取的行数
		scan.setCacheBlocks(false);
		//set mapper
		TableMapReduceUtil.initTableMapperJob(
				"table1", //input table
				scan, //scan instance
				TabMapper.class,//set mapper class 
				Text.class, //mapper ouput key
				Put.class, //mapper output value
				job//set job
				);
		TableMapReduceUtil.initTableReducerJob(
				"table2", //output table
				TabReduce.class,//set reduce class 
				job//set job
				);
		
		job.setNumReduceTasks(1);
		boolean b = job.waitForCompletion(true);
		
		if(!b){
			System.err.println("error with the job!");
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		//create config
		Configuration config = HBaseConfiguration.create();
		
		//submit job
		int status = ToolRunner.run(config, new Tab2TabMapReduce(), args);
		
		//exit
		System.exit(status);
	}

}
