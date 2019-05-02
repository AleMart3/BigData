package top3;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper2 extends Mapper<LongWritable, Text, CustomKey, Text> {

	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		CustomKey c= new CustomKey();
		
		String prev=null;
		
		while (token.hasMoreTokens()) {
			 String cur = token.nextToken();
			 if (prev != null) {
				 c.set(new Text(prev),new IntWritable(Integer.parseInt(cur)));
				 context.write(c, new Text(""));
			 }
			 prev = cur;
		}
		
	
	}

}

