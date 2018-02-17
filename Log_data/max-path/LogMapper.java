package LogCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LogMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(" ");
		String[] path_name = SingleCountryData[6].split("/");
		if(SingleCountryData.length==10)
		{
			if(path_name.length>=1 && path_name[0].equals("http:"))
			{
				String x="";
				for(int i=3;i<path_name.length;i++)
				{	x+="/";
					x+=path_name[i];
				}
				output.collect(new Text(x), one);
			}
			else
				output.collect(new Text(SingleCountryData[6]), one);
		}
	}
}
