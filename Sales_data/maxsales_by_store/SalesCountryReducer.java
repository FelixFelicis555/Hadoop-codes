package SalesCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		double frequencyForCountry = 0;
		while (values.hasNext()) {
			
			DoubleWritable value = (DoubleWritable) values.next();
			frequencyForCountry= Math.max(frequencyForCountry,value.get());

			
		}
		output.collect(key, new DoubleWritable(frequencyForCountry));
	}
}
