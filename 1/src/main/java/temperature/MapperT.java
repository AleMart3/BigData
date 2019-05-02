package temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperT extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line= value.toString();
		String year= line.substring(15,19);
		
		int temperatura;
		
		if(line.charAt(87)=='+') {
			temperatura= Integer.parseInt(line.substring(88, 92));
		}
		else 
			temperatura= Integer.parseInt(line.substring(87, 92));
			
		
		if(temperatura!=9999) {
			context.write(new Text(year), new IntWritable(temperatura));
			
			
		}
		
		
			
}
		
	

}
