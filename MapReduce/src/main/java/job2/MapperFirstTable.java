package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilsJob2.Costanti_FirstTable;

public class MapperFirstTable extends Mapper <LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		if(key.get()==0) return;

		String[] line = value.toString().split("[,]");
		
		String sector = line[line.length - 2 ];
		
		// Controllo necessario perché il campo industry di alcune righe della tabella historical_stocks è circondato da virgolette 
		// e contiene una virgola al suo interno, cosa che produce un errore quando viene effettuato lo split sulla stringa value.
		if (sector.contains((" \" ").trim())) sector = line[line.length - 3]; 
		
		context.write(new Text(line[Costanti_FirstTable.ticker]), new Text("firstTable\t" + sector));

	}

}