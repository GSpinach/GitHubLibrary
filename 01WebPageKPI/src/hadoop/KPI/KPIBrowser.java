package hadoop.KPI;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPIBrowser
{
	public static class KPIBrowserMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private String k = null;
		// private String v = null;

		@Override
		protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException
		{
			KPI kpi = KPI.filterBroswer(v1.toString());
			if (kpi.isValid())
			{
				k = kpi.getHttp_user_agent();
				context.write(new Text(k), new LongWritable(1));
			}
		}
	}

	public static class KPIBrowserReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{

		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException
		{
			long count = 0;
			for (LongWritable sum : v2)
			{
				count += sum.get();
			}
			context.write(k2, new LongWritable(count));
		}
	}

	public static void main(String[] args) throws Exception, IOException, InterruptedException
	{
		// System.setProperty("HADOOP_USER_NAME", "root");
		// String input = "hdfs://hadoop02:9000/user/practises/access.log.10";
		// String output = "hdfs://hadoop02:9000/user/practises/result_browser";

		String input = "/Users/Spinach/Desktop/practises/access.log.10";
		String output = "/Users/Spinach/Desktop/result_browser";

		// 构建Job对象
		Job job = Job.getInstance(new Configuration());

		// 注意：设置main方法的class
		job.setJarByClass(KPIBrowser.class);

		// 设置Mapper相关属性
		job.setMapperClass(KPIBrowserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(input));

		// 设置Reducer相关属性
		job.setReducerClass(KPIBrowserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		// 提交任务
		job.waitForCompletion(true);
	}
}
