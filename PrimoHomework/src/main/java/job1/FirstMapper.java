package job1;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper  extends Mapper<LongWritable, Text, TickerDate, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get()==0) return;

		//crea un array di stringhe, stringhe che sono pari al numero di |
		String[] line = value.toString().split("[,]");

		String data= line[Costanti.date];
		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		try {
			LocalDate date = LocalDate.parse(data, format);

			if (date.getYear()>= 1998 && date.getYear()<=2018){
				String ticker = line[Costanti.ticker];
				String campi = line[Costanti.close] + "," + line[Costanti.low] + "," + line[Costanti.high] + "," + line[Costanti.volume];

				context.write(new TickerDate(new Text(ticker), new IntWritable(date.getYear())), new Text(campi));

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
