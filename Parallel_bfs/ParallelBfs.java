package bfs_graph;  
import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;


public class ParallelBfs {

    public static String OUT = "outfile";
    public static String IN = "input_file";

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, LongWritable, Text> 
    {
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<LongWritable, Text> output, Reporter reporter)
                throws IOException 
        {
            //For every point look at everything it points to.  
            //Emit to the points the variable with the current distance + 1 

            Text word = new Text();
            String line = value.toString(); // looks like 1 0 2:3:

            String[] sp = line.split("\t| "); // splits on space

            //Add 1 to distance of current node
            int distanceadd = Integer.parseInt(sp[1]) + 1;

            //Neighbours of current node
            String[] adj_list = sp[2].split(":");

            for (int i = 0; i < adj_list.length; i++) 
            {
                word.set("VALUE " + distanceadd); 
                output.collect(new LongWritable(Integer.parseInt(adj_list[i])),word);
                word.clear();
            }

            // pass in current node's distance
            word.set("VALUE " + sp[1]);
            output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
            word.clear();

            //pass the node structure
            word.set("NODES " + sp[2]);
            output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
            word.clear();
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<LongWritable, Text, LongWritable, Text> {

       
        @Override
        public void reduce(LongWritable key, Iterator<Text> values,
                OutputCollector<LongWritable, Text> output, Reporter reporter)
                throws IOException 
        {

            //The key is the current point  
            //The values are all the possible distances to this point  
            //simply emit the point and the minimum distance value  

            String nodes = "UNMODED";
            Text word = new Text();
            int lowest = 125; // 125 is considered as infinite distance
            while (values.hasNext()) 
            {                           
                String[] sp = values.next().toString().split(" "); // splits on space
                                                                
                if (sp[0].equalsIgnoreCase("NODES")) 
                {
                    nodes = sp[1];
                } 
                else if (sp[0].equalsIgnoreCase("VALUE")) 
                {
                    int distance = Integer.parseInt(sp[1]);
                    lowest = Math.min(distance, lowest);
                }
            }
            word.set(lowest + " " + nodes);
            output.collect(key, word);
            word.clear();
        }
    }

    public static void main(String[] args) throws Exception 
    {
        run(args);
    }

    public static void run(String[] args) throws Exception 
    {
	int count=0;
        IN = args[0];
        OUT = args[1];
        String input = IN;
        String output = OUT + count;
        boolean isdone = false;

        // Run again and again till all nodes are visited
        while (isdone == false) 
	{
	    count++;
            JobConf conf = new JobConf(ParallelBfs.class);
            conf.setJobName("Dijkstra");
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(input));
            FileOutputFormat.setOutputPath(conf, new Path(output));

            JobClient.runJob(conf);

            input = output + "/part-00000";
            isdone = true;
            Path ofile = new Path(input);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
            HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
            String line = br.readLine();
            
	    // Read the current output file and put it into HashMap
            while (line != null) 
	    {
                String[] sp = line.split("\t| ");
                int node = Integer.parseInt(sp[0]);
                int distance = Integer.parseInt(sp[1]);
                imap.put(node, distance);
                line = br.readLine();
            }
            br.close();

            // Check if any node is still left
            Iterator<Integer> itr = imap.keySet().iterator();
            while (itr.hasNext()) 
	   {
                int key = itr.next();
                int value = imap.get(key);
                if (value >= 125) 
		{
                    isdone = false;
                }
            }
            input = output;
            output = OUT + count;
        }
    }
}
