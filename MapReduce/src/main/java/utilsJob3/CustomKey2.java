package utilsJob3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey2 implements WritableComparable<CustomKey2>{

	private Text nome;
	private Text settore;
	private Text ticker; 
	private IntWritable anno;

	public CustomKey2() {
	}

	public CustomKey2(Text nome, Text settore, Text ticker, IntWritable anno) {
		this.nome = nome;
		this.settore = settore;
		this.ticker = ticker; 
		this.anno = anno;
	}


	public Text getNome() {
		return nome;
	}

	public void setNome(Text nome) {
		this.nome = nome;
	}

	public Text getSettore() {
		return settore;
	}

	public void setSettore(Text settore) {
		this.settore = settore;
	}

	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public IntWritable getAnno() {
		return anno;
	}

	public void setAnno(IntWritable anno) {
		this.anno = anno;
	}


	@Override
	public int hashCode() {
		return this.nome.hashCode() + this.settore.hashCode() + this.ticker.hashCode() + this.anno.hashCode();
	}

	@Override
	public String toString() {
		return this.nome.toString() + " " + this.settore.toString() + " " + this.ticker.toString() + " " + this.anno.toString();
	}


	@Override
	public boolean equals(Object o){
		CustomKey2 a= (CustomKey2)o;
		return this.nome.equals(a.getNome()) && this.settore.equals(a.getSettore()) 
				&& this.ticker.equals(a.getTicker()) && this.anno.equals(a.getAnno());
	}

	public int compareTo(CustomKey2 that) {
		int cmp = this.nome.hashCode()-that.getNome().hashCode();
		if(cmp==0){
			cmp= this.anno.hashCode()-that.getAnno().hashCode();
			if (cmp == 0){
				cmp= this.settore.hashCode()-that.getSettore().hashCode();
				if (cmp == 0)
					cmp = this.ticker.hashCode() - that.getTicker().hashCode(); 
			}
		}

		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.nome = new Text(in.readUTF());
		this.settore = new Text(in.readUTF());
		this.ticker = new Text(in.readUTF());
		this.anno = new IntWritable(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.nome.toString());
		out.writeUTF(this.settore.toString());
		out.writeUTF(this.ticker.toString());
		out.writeInt(this.anno.get());
	}




}