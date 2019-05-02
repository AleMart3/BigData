package bigram;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperB extends Mapper<LongWritable, Text, Bigramma, IntWritable>{
	
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		Bigramma bigramma = new Bigramma();
		
		String line = value.toString();
		 String prev = null;
		 StringTokenizer itr = new StringTokenizer(line);
		 while (itr.hasMoreTokens()) {
			 String cur = itr.nextToken();
			 if (prev != null) {
				 bigramma.set(new Text(prev),new Text(cur));
				 context.write(bigramma, new IntWritable(1));
			 }
			 prev = cur;
			 }
		 } 
		
	}


