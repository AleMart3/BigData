package job1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper1  extends Mapper<LongWritable, Text, Text, LongWritable> {
		

	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//crea un array di stringhe, stringhe che sono pari al numero di |
		String[] line = value.toString().split("[,]");
		
		
		String data= line[Costanti.date];
		
		SimpleDateFormat format= new SimpleDateFormat("yyyy-mm-dd");
		try {
			Date date = format.parse(data);
			date.getYear();
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
				
		String ticker = line[Costanti.ticker];
		String close = line[Costanti.close];
		String low = line[Costanti.low];
		String high= line[Costanti.high];
		String volume= line[Costanti.volume];
		
			
	}
	
	public int getAnno(String data) {
		
		
	}
}
