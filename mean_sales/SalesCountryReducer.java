package SalesCountry;

import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		double frequency = 0;
		int count=0;
		while (values.hasNext()) {
			
			DoubleWritable value = (DoubleWritable) values.next();
			frequency+=value.get();
			count++;
			
		}
		output.collect(key, new DoubleWritable(frequency/count));
	}
}
