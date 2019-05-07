package job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValues implements WritableComparable<CustomValues>{

	private Text data;	
	private DoubleWritable close;


	public CustomValues() {
	}

	public CustomValues(Text data, DoubleWritable close) {
		this.data=data;
		this.close = close;

	}

	public Text getData() {
		return data;
	}

	public void setData(Text data) {
		this.data = data;
	}

	public DoubleWritable getClose() {
		return close;
	}

	public void setClose(DoubleWritable close) {
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
		CustomValues a= (CustomValues)o;
		return this.data.equals(a.getData()) &&this.close.equals(a.getClose());
	}

	@Override
	public int compareTo(CustomValues that) {
		int cmp= this.close.hashCode()-that.close.hashCode();
		if(cmp==0)
			cmp= this.data.hashCode()-that.data.hashCode();


		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.data= new Text(in.readUTF());
		this.close = new DoubleWritable(in.readDouble());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.data.toString());
		out.writeDouble(this.close.get());

	}


}

