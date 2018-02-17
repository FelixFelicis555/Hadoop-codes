package LogCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LogReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	
	public static int max_path=-1;

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int frequencyForCountry = 0	;
		while (values.hasNext()) {
			
			IntWritable value = values.next();
			frequencyForCountry++;
			
			
		}
		max_path= Math.max(frequencyForCountry,max_path);
		output.collect(key, new IntWritable(max_path));
	}

}
