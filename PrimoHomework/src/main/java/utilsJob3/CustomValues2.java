package utilsJob3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValues2 implements WritableComparable<CustomValues2>{

	private Text data;	
	private IntWritable close;


	public CustomValues2() {
	}

	public CustomValues2(Text data, IntWritable close) {
		this.data=data;
		this.close = close;

	}

	public Text getData() {
		return data;
	}

	public void setData(Text data) {
		this.data = data;
	}

	public IntWritable getClose() {
		return close;
	}

	public void setClose(IntWritable close) {
		this.close = close;
	}


	public int getAnno() {
		return Integer.parseInt(this.data.toString().substring(0, 4));
	}

	public int getMese() {
		return Integer.parseInt(this.data.toString().substring(5, 7));
	}
	public int getGiorno() {
		return Integer.parseInt(this.data.toString().substring(8));
	}


	@Override
	public int hashCode() {
		return data.hashCode() + close.hashCode();
	}

	@Override
	public String toString() {
		return this.data.toString()+ " " +this.close.toString();
	}


	@Override
	public boolean equals(Object o){
		CustomValues2 a= (CustomValues2)o;
		return this.data.equals(a.getData()) &&this.close.equals(a.getClose());
	}

	@Override
	public int compareTo(CustomValues2 that) {
		int cmp= this.close.hashCode()-that.close.hashCode();
		if(cmp==0)
			cmp= this.data.hashCode()-that.data.hashCode();


		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.data= new Text(in.readUTF());
		this.close = new IntWritable(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.data.toString());
		out.writeInt(this.close.get());

	}


}

