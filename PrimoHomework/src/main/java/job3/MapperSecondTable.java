package job3;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilsJob3.Costanti_SecondTable;


public class MapperSecondTable extends Mapper <LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		if(key.get()==0) return;

		String[] line = value.toString().split("[,]");

		String data= line[Costanti_SecondTable.date];
		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		try {
			LocalDate date = LocalDate.parse(data, format);

			if (date.getYear()>= 2016 && date.getYear()<=2018){
				String ticker = line[Costanti_SecondTable.ticker];
				String campi = data + "," + line[Costanti_SecondTable.close];

				context.write(new Text(ticker), new Text("secondTable\t" + campi));

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}