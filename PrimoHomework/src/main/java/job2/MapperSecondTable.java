package job2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import job1.Costanti;


public class MapperSecondTable extends Mapper <LongWritable, Text, Text, Text>{
	
	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		if(key.get()==0) return;

		String[] line = value.toString().split("[,]");

		String data= line[Costanti.date];
		SimpleDateFormat format= new SimpleDateFormat("yyyy-mm-dd");
		
		try {
			Date date = format.parse(data);

			if (date.getYear()>= 104 && date.getYear()<=118){
				String ticker = line[Costanti.ticker];
				String campi = String.valueOf(date.getYear()+1900) + "," + line[Costanti.close] + "," + line[Costanti.volume];

				context.write(new Text(ticker), new Text("secondTable\t" + campi));

			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}