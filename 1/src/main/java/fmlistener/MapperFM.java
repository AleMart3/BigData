package fmlistener;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperFM extends Mapper<LongWritable, Text, Text, Text> {
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//crea un array di stringhe, stringhe che sono pari al numero di |
		String[] line = value.toString().split("[|]");
		String userId = line[Costanti.USER_ID];
		String trackId = line[Costanti.TRACK_ID];

		if (line.length==5)
		context.write(new Text(trackId), new Text(userId) );
		else //per le eccezioni, nella console stamperà il numero delle volte che si è presentata l'eccezione
			context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
		}

	}

