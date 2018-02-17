package SalesCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;



public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, DoubleWritable, DoubleWritable> {

	public static double total_cost = 0;
	public static double count=0;


	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<DoubleWritable,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		
		
		while (values.hasNext()) {
			
			DoubleWritable value = (DoubleWritable) values.next();
			count+=1;
			//frequencyForCountry= Math.max(frequencyForCountry,value.get());
			total_cost+=value.get();
			
		}
		output.collect(new DoubleWritable(count), new DoubleWritable(total_cost));
	}
}
