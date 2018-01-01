package hadoop.BaseStationData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * pos文件格式：imsi A imei 0001 updatetype 1 loc X基站 time 2013-09-12 09:00:00
 * net文件格式：imsi A imei 0002 loc Y基站 time 2013-09-12 09:00:00 url www.baidu.com
 * 
 * 将pos和net文件中imsi loc time 转化格式为 imsi(=imsi) position(=loc) timeflag(=MM-dd)
 * time(unix时间戳)
 * 
 * 功能描述：
 * 
 * @author Spinach
 * @version 1.0
 */
public class ReadDataAndFormatPreprocess
{
	private String imsi, position, time, timeflag;
	private Date day;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void DataProcess(String line, boolean fileType)
	{
		String[] lineSplit = line.split("\t");
		// 分割字段
		if (fileType)
		{
			this.imsi = lineSplit[0];
			this.position = lineSplit[3];
			this.time = lineSplit[4];
		} else
		{
			this.imsi = lineSplit[0];
			this.position = lineSplit[2];
			this.time = lineSplit[3];
		}

		try
		{
			this.day = this.formatter.parse(this.time);
		} catch (ParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.timeflag = this.time.split(" ")[0].split("-")[1] + "-" + this.time.split(" ")[0].split("-")[2];
	}

	public String outKey()
	{
		return this.imsi + "#" + this.timeflag;
	}

	public String outValue()
	{
		return this.position + "#" + String.valueOf(this.day.getTime() / 1000l);
	}
}
