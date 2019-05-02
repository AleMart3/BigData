package meanMAX;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Mappermean extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("[,]");
		
		String mese_anno = line[Costanti.Data].substring(2);
		
		Double temperature = Double.parseDouble(line[Costanti.Max]);
		
		context.write(new Text(mese_anno), new DoubleWritable(temperature));
		
		

		

	}

}