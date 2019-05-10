package utilsJob1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TickerDate implements WritableComparable<TickerDate> {

	private Text ticker;
	private IntWritable year;

	public TickerDate() {
	}

	public TickerDate(Text ticker, IntWritable date) {
		this.ticker = ticker;
		this.year = date;
	}

	public Text getTicker() {
		return this.ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public IntWritable getYear() {
		return this.year;
	}

	public void setDate(IntWritable year) {
		this.year = year;
	}

	public void set(Text ticker, IntWritable year) {
		this.ticker = ticker;
		this.year = year;
	}

	@Override
	public int hashCode() {
		return this.ticker.hashCode() + this.year.hashCode();
	}

	@Override
	public String toString() {
		return this.ticker.toString() + " " + this.year.toString();
	}

	@Override
	public boolean equals(Object o){
		TickerDate t= (TickerDate)o;
		return this.ticker.equals(t.getTicker()) && this.year.equals(t.getYear());
	}

	@Override
	public int compareTo(TickerDate that) {
		int cmp = this.ticker.hashCode()-that.getTicker().hashCode();
		if(cmp==0)
			cmp= this.year.hashCode()-that.getYear().hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.ticker = new Text(in.readUTF());
		this.year = new IntWritable(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.ticker.toString());
		out.writeInt(this.year.get());
	}
}
