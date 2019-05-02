package callDATA;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperCall extends Mapper<LongWritable, Text, Text, LongWritable> {
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//crea un array di stringhe, stringhe che sono pari al numero di |
		String[] line = value.toString().split("[|]");
		
		if(line[Costanti.STDFlag].equals("1")) {
			
			String userId = line[Costanti.FromPhoneNumber];
			String end = line[Costanti.CallEndTime];
			String start= line[Costanti.CallStartTime];
			try {
				context.write(new Text(userId), new LongWritable(minuti(start,end)));
			} catch (ParseException e) {
				
				e.printStackTrace();
			}
		}
		
	}
		public long minuti(String start, String fine) throws ParseException {
			SimpleDateFormat data= new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
	    	Date d= data.parse(start);
	    	Date d2= data.parse(fine);
	    	Long dminuti = d.getTime();
	    	Long d2minuti= d2.getTime();
	    	return (d2minuti/(1000*60))-(dminuti/(1000*60));
			
			
		}
    	
		
		
		

	

	}


