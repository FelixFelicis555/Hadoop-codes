package IndexCountry;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class IndexMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, Text> {
	//private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector <Text, Text> output, Reporter reporter) throws IOException {

		String valueString1 = value.toString();
		String valueString=valueString1.replaceAll("[\\n]"," ");
		String[] Data_raw = valueString.split("\t");
		if(Data_raw.length > 4)
		{
		
		String[] Data = Data_raw[4].split(" |\\,|\\.|!|\\?|\\:|\\;|\\\"|\\(|\\)|\\<|\\>|\\[|\\]|\\#|\\$|\\=|\\-|\\/");
		for(String s: Data)
		{
			output.collect(new Text(s.toString().toLowerCase()), new Text(Data_raw[0].toString()));
		}
		}
	}
}
