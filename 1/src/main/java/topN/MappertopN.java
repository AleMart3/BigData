package topN;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MappertopN extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	 private final static IntWritable one = new IntWritable(1);
	 
	 private Text word = new Text();
	 
	 private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	 
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
		 StringTokenizer itr = new StringTokenizer(cleanLine);
		 
	 while (itr.hasMoreTokens()) {
		 word.set(itr.nextToken().trim()); //trim toglie whitespace
		 context.write(word, one);
	 }
	 }
}
	 