package job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReducerOutputValues implements WritableComparable<ReducerOutputValues>{

	// + "," + String.valueOf(this.sommaVol) + "," + String.valueOf(cont);

	private IntWritable year;
	private IntWritable mediaChiusura;
	private IntWritable minLow;
	private IntWritable maxHigh;
	private IntWritable sommaVol;
	private IntWritable cont; 

	public ReducerOutputValues() {
	}


	public ReducerOutputValues(IntWritable year, IntWritable mediaChiusura, IntWritable minLow, IntWritable maxHigh,
			IntWritable sommaVol, IntWritable cont) {
		this.year = year;
		this.mediaChiusura = mediaChiusura;
		this.minLow = minLow;
		this.maxHigh = maxHigh;
		this.sommaVol = sommaVol;
		this.cont = cont;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getMediaChiusura() {
		return mediaChiusura;
	}

	public void setMediaChiusura(IntWritable mediaChiusura) {
		this.mediaChiusura = mediaChiusura;
	}

	public IntWritable getMinLow() {
		return minLow;
	}

	public void setMinLow(IntWritable minLow) {
		this.minLow = minLow;
	}

	public IntWritable getMaxHigh() {
		return maxHigh;
	}

	public void setMaxHigh(IntWritable maxHigh) {
		this.maxHigh = maxHigh;
	}

	public IntWritable getSommaVol() {
		return sommaVol;
	}

	public void setSommaVol(IntWritable sommaVol) {
		this.sommaVol = sommaVol;
	}

	public IntWritable getCont() {
		return cont;
	}

	public void setCont(IntWritable cont) {
		this.cont = cont;
	}

	@Override
	public int hashCode() {
		return this.year.hashCode() + this.mediaChiusura.hashCode() + this.minLow.hashCode() + 
				+ this.maxHigh.hashCode() + this.sommaVol.hashCode() + this.cont.hashCode();

	}
	
	@Override
	public String toString() {
		return "ReducerOutputValues [year=" + year + ", mediaChiusura=" + mediaChiusura + ", minLow=" + minLow
				+ ", maxHigh=" + maxHigh + ", sommaVol=" + sommaVol + ", cont=" + cont + "]";
	}


	@Override
	public void readFields(DataInput arg0) throws IOException {

		
	}


	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public int compareTo(ReducerOutputValues o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
