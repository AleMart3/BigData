package average;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperA extends Mapper<LongWritable, Text, Text, IntWritable> {

	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			String s =tokenizer.nextToken();
			String carattere= Character.toString(s.charAt(0));
			
			context.write(new Text(carattere), new IntWritable(s.length()));
		}

	}

}