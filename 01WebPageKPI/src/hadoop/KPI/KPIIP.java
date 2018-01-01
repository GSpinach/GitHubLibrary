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

public class KPIIP
{
	public static class KPIIPMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private String k = null;

		@Override
		protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException
		{
			KPI kpi = KPI.filterIPs(v1.toString());
			if (kpi.isValid())
			{
				k = kpi.getRemote_addr();
				context.write(new Text(k), new LongWritable(1));
			}
		}
	}

	public static class KPIIPReducer extends Reducer<Text, LongWritable, Text, LongWritable>
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

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		String input = "/Users/Spinach/Desktop/practises/access.log.10";
		String output = "/Users/Spinach/Desktop/result_ip";

		Job job = Job.getInstance(new Configuration());

		job.setJarByClass(KPIIP.class);
		job.setMapperClass(KPIIPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(input));

		job.setReducerClass(KPIIPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}
}
