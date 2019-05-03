package job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstReducerOutputValues implements WritableComparable<FirstReducerOutputValues>{

	// + "," + String.valueOf(this.sommaVol) + "," + String.valueOf(cont);

	private IntWritable year;
	private DoubleWritable mediaChiusura;
	private DoubleWritable minLow;
	private DoubleWritable maxHigh;
	private DoubleWritable sommaVol;
	private DoubleWritable cont; 

	public FirstReducerOutputValues() {
	}


	public FirstReducerOutputValues(IntWritable year, DoubleWritable mediaChiusura, DoubleWritable minLow, DoubleWritable maxHigh,
			DoubleWritable sommaVol, DoubleWritable cont) {
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

	public DoubleWritable getMediaChiusura() {
		return mediaChiusura;
	}

	public void setMediaChiusura(DoubleWritable mediaChiusura) {
		this.mediaChiusura = mediaChiusura;
	}

	public DoubleWritable getMinLow() {
		return minLow;
	}

	public void setMinLow(DoubleWritable minLow) {
		this.minLow = minLow;
	}

	public DoubleWritable getMaxHigh() {
		return maxHigh;
	}

	public void setMaxHigh(DoubleWritable maxHigh) {
		this.maxHigh = maxHigh;
	}

	public DoubleWritable getSommaVol() {
		return sommaVol;
	}

	public void setSommaVol(DoubleWritable sommaVol) {
		this.sommaVol = sommaVol;
	}

	public DoubleWritable getCont() {
		return cont;
	}

	public void setCont(DoubleWritable cont) {
		this.cont = cont;
	}

	@Override
	public int hashCode() {
		return this.year.hashCode() + this.mediaChiusura.hashCode() + this.minLow.hashCode() + 
				+ this.maxHigh.hashCode() + this.sommaVol.hashCode() + this.cont.hashCode();

	}

	@Override
	public String toString() {
		return "," + year + ", " + mediaChiusura + ", " + minLow
				+ "," + maxHigh + "," + sommaVol + ", " + cont + "";
	}

	@Override
	public boolean equals(Object o){
		FirstReducerOutputValues a= (FirstReducerOutputValues)o;
		return this.year.equals(a.getYear()) && this.mediaChiusura.equals(a.getMediaChiusura()) &&
			   this.minLow.equals(a.getMinLow()) && this.maxHigh.equals(a.getMaxHigh()) &&
			   this.sommaVol.equals(a.getSommaVol()) && this.cont.equals(a.getCont());

	}

	public int compareTo(FirstReducerOutputValues that) {
		return 0;
	}

	public void readFields(DataInput in) throws IOException {
		this.year = new IntWritable(in.readInt());
		this.mediaChiusura = new DoubleWritable(in.readDouble());
		this.minLow = new DoubleWritable(in.readDouble());
		this.maxHigh = new DoubleWritable(in.readDouble());
		this.sommaVol = new DoubleWritable(in.readDouble());
		this.cont = new DoubleWritable(in.readDouble());
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.year.get());
		out.writeDouble(this.mediaChiusura.get());
		out.writeDouble(this.minLow.get());
		out.writeDouble(this.maxHigh.get());
		out.writeDouble(this.sommaVol.get());
		out.writeDouble(this.cont.get());

	}




}
