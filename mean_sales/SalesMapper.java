package SalesCountry;

import java.io.*;
import java.text.SimpleDateFormat;  
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, DoubleWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector <Text, DoubleWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] Data = valueString.split("\t");
		
		Calendar c = Calendar.getInstance();
		String input_date=Data[0];
  		SimpleDateFormat format1=new SimpleDateFormat("yyyy-MM-dd");
  		try{
		Date dt1=format1.parse(input_date);
		c.setTime(dt1);
		//System.out.println(new SimpleDateFormat("EEE").format(dt1));
		
		if(Data.length==6)
		{
			
			output.collect(new Text(new SimpleDateFormat("EEE").format(dt1)), new DoubleWritable(Double.parseDouble(Data[4])));
		}
		
		}
		catch (Exception e)
		{
		}
	}
}
