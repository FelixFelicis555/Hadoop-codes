package IndexCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class IndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector <Text, Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int frequency = 0;
		String keyString = key.toString();
		if(keyString.equals("fantastic"))
		{	while (values.hasNext()) 
			{			
				Text value = values.next();
				frequency++;	
			}
			output.collect(key, new Text(Integer.toString(frequency)));
		}
		else if(keyString.equals("fantastically"))
		{	
			StringBuffer buffer = new StringBuffer();
			while(values.hasNext())
			{
				buffer.append(values.next().toString());
			}
		 	output.collect(key, new Text(buffer.toString()));
		}

}
}
