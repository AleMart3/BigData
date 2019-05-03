package job1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper  extends Mapper<LongWritable, Text, TickerDate, Text> {


	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get()==0) return;

		//crea un array di stringhe, stringhe che sono pari al numero di |
		String[] line = value.toString().split("[,]");

		String data= line[Costanti.date];
		SimpleDateFormat format= new SimpleDateFormat("yyyy-mm-dd");
		
		try {
			Date date = format.parse(data);

			if (date.getYear()>= 98 && date.getYear()<=118){
				String ticker = line[Costanti.ticker];
				String campi = line[Costanti.close] + "," + line[Costanti.low] + "," + line[Costanti.high] + "," + line[Costanti.volume];

				context.write(new TickerDate(new Text(ticker), new IntWritable(date.getYear()+1900)), new Text(campi));

			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
