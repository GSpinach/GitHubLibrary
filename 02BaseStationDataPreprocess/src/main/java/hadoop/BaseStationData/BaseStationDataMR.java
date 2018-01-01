package hadoop.BaseStationData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map输出-----Key：imsi|timeflag Value：position|time
 * Reducer输出-----Key：imsi|timeflag
 * Value：position|time合并，并排序，再计算时间差作为停留时间(对于大于60分钟的数据视为关机，删除)
 * 
 * @author Spinach
 * @version 1.0
 */

public class BaseStationDataMR
{
	public static class DataMap extends Mapper<LongWritable, Text, Text, Text>
	{
		boolean fileType;

		/**
		 * 功能描述：确定文件类型，如果是POS文件返回true，否则false(NET文件类型)
		 * 
		 * @param context
		 * @return .startsWith("POS") ? true : false;
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.fileType = ((FileSplit) context.getInputSplit()).getPath().getName().startsWith("POS") ? true : false;
		}

		@Override
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException
		{
			String line = v1.toString();
			ReadDataAndFormatPreprocess rdfp = new ReadDataAndFormatPreprocess();
			rdfp.DataProcess(line, this.fileType);
			// System.out.println("Map = " + rdfp.outKey() + "<----key value
			// ---->" + rdfp.outValue());
			context.write(new Text(rdfp.outKey()), new Text(rdfp.outValue()));
		}

	}

	public static class DataReducer extends Reducer<Text, Text, Text, Text>
	{
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void reduce(Text k2, Iterable<Text> v2, Context context) throws IOException, InterruptedException
		{
			// System.out.println("Reducer = " + k2 + "<---key value --->" +
			// v2);
			String imsi = k2.toString().split("#")[0];
			String timeflag = k2.toString().split("#")[1];
			// 汇总v2数据，并排序
			TreeMap<Long, String> sortMap = new TreeMap<Long, String>();
			String valueString;
			for (Text value : v2)
			{
				valueString = value.toString();
				sortMap.put(Long.valueOf(valueString.split("#")[1]), valueString.split("#")[0]);
			}

			try
			{
				Date temp = this.formatter.parse("2013-" + timeflag + " " + "23:59:59");
				sortMap.put((temp.getTime() / 1000L), "OFF");// 设定最后的计算基点
				// 计算v2停留时间
				HashMap<String, Float> unsortMap = calculateTime(sortMap);
				for (Entry<String, Float> entry : unsortMap.entrySet())
				{
					StringBuilder builder = new StringBuilder();
					builder.append(imsi).append("|");
					builder.append(entry.getKey()).append("|");
					builder.append(timeflag).append("|");
					builder.append(entry.getValue());

					context.write(new Text(), new Text(builder.toString()));
				}
			} catch (ParseException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private HashMap<String, Float> calculateTime(TreeMap<Long, String> sortMap)
		{
			HashMap<String, Float> outMap = new HashMap<String, Float>();

			Entry<Long, String> time, nextTime;
			Iterator<Entry<Long, String>> it = sortMap.entrySet().iterator();
			time = it.next();

			while (it.hasNext())
			{
				nextTime = it.next();
				float calculateTime = (float) ((nextTime.getKey() - time.getKey()) / 60.0f);
				if (calculateTime <= 60)
				{
					if (outMap.containsKey(time.getValue()))
					{
						outMap.put(time.getValue(), outMap.get(time.getValue()) + calculateTime);
					} else
					{
						outMap.put(time.getValue(), calculateTime);
					}
				}
				time = nextTime;
			}
			return outMap;
		}
	}

	/**
	 * 功能描述：记录每个用户的移动轨迹
	 * 
	 * @throws IOException
	 *             InterruptedException ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		long startTime = System.currentTimeMillis();
		// String input = "/Users/Spinach/Desktop/practises/POSITION";
		// String output = "/Users/Spinach/Desktop/result_BaseStation";
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(BaseStationDataMR.class);

		job.setMapperClass(DataMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.out.println("RunTime is " + (System.currentTimeMillis() - startTime) + " ms");
		System.exit(job.isSuccessful() ? 0 : 1);
	}
}
