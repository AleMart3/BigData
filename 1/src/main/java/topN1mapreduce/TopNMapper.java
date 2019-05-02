package topN1mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
* The mapper reads one line at the time, splits it into an array of single words and emits every
* word to the reducers with the value of 1.
*/
public class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {

   private final static IntWritable one = new IntWritable(1);
   private Text word = new Text();
   private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

	   @Override
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
	   StringTokenizer itr = new StringTokenizer(cleanLine);
	   while (itr.hasMoreTokens()) {
	     word.set(itr.nextToken().trim());
	     context.write(word, one);
	    }
	   }
	  }
