import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.io.EOFException;

public class ndxHandler {
	ArrayList tblAddr = null;
	TreeMap<Object, ArrayList> colndx = new TreeMap();
	
	//ndx file write
	public void ndxfilewriter(String dbname, String table,String column,String datatype) throws EOFException{
		try {
			RandomAccessFile ndxFile = new RandomAccessFile(dbname+"."+table+"."+column+".ndx", "rw");
			String[] temp = new String[2];
			int len = 0, lncount, val;
			String str;
			if(datatype.contains("("))
			{
				datatype=datatype.replace("(", " ");
				datatype=datatype.replace(")", "");
				temp = datatype.split(" ",2);
				datatype = temp[0];
				len = Integer.parseInt(temp[1]);
			}
			for(Entry<Object,ArrayList> lnitem : colndx.entrySet()) {
				switch(datatype){
				case "char":
				case "varchar":
								ndxFile.writeByte(((String)lnitem.getKey()).length());
								ndxFile.writeBytes((String) lnitem.getKey());
								break;
				case "int":
								ndxFile.writeInt((int) lnitem.getKey());
								break;
				case "double":
								ndxFile.writeDouble((double) lnitem.getKey());
								break;
				case "float":
								ndxFile.writeFloat((float) lnitem.getKey());
								break;
				case "short int":
				case "short":
								ndxFile.writeShort((short) lnitem.getKey());
								break;
				case "long int":
				case "long":
								ndxFile.writeLong((long) lnitem.getKey());
								break;
				default:
								System.out.println("Invalid DataType");
				}
				ArrayList value = lnitem.getValue();
				ndxFile.writeInt(value.size());
				for(int i=0;i<value.size();i++)
					ndxFile.writeInt((int) value.get(i));
			}
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e);
		}
	}
	
	//ndx file reader
	public void raffilereader(String database, String table, String col_name,String datatype) throws EOFException {
		try{
		RandomAccessFile ndxFile = new RandomAccessFile(database+"."+table+"."+col_name+".ndx", "rw");
		String[] dtholder;
		int lnCnt;
		ndxFile.seek(0);
		dtholder = datatype.replace("(", " ").replace(")", " ").split(" ");
		
		switch(dtholder[0]){
			case "int":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<Integer>();
							int temp = ndxFile.readInt();
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(temp, tblAddr);
						}
						ndxFile.close();
						break;
			case "byte":
						break;
			case "long int":
			case "long":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<Long>();
							long temp_key = ndxFile.readLong();
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(temp_key, tblAddr);
						}
						ndxFile.close();
						break;
			case "short":
			case "short int":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<Short>();
							Short temp_key = ndxFile.readShort();
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(temp_key, tblAddr);
						}
						ndxFile.close();
						break;
			case "char":
			case "varchar":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<String>();
							int len = ndxFile.readByte();
							byte[] str = new byte[len];
							ndxFile.readFully(str);
							
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(new String(str), tblAddr);
						}
						ndxFile.close();
						break;
			case "float":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<Float>();
							Float temp_key = ndxFile.readFloat();
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(temp_key, tblAddr);
						}
						ndxFile.close();
						break;
			case "double":
						while(ndxFile.getFilePointer()!=ndxFile.length()){
							tblAddr = new ArrayList<Double>();
							double temp_key = ndxFile.readDouble();
							lnCnt = ndxFile.readInt();
							for(int i = 0;i<lnCnt;i++)
								tblAddr.add(ndxFile.readInt());
							colndx.put(temp_key, tblAddr);
						}
						ndxFile.close();
						break;
			case "datetime":
						System.out.println("datetime");
						break;
			case "date":
						System.out.println("date");
						break;
			default:
						throw new Exception("Unable to write into table! :(");
		}
	}
	catch(Exception e){
			System.out.println(e);
	}
	}
	
	//ndx printer
	public void ndxprinter(){
		for(Entry<Object,ArrayList> lnitem : colndx.entrySet()) {
			Object key = lnitem.getKey();         // Get the index key
			System.out.print(key + " ===>> ");       // Display the index key
			ArrayList value = lnitem.getValue();   // Get the list of record addresses
			System.out.print("[" + value.get(0)); // Display the first address
			for(int i=1; i < value.size();i++) {  // Check for and display additional addresses for non-unique indexes
				System.out.print("," + value.get(i));
			}
			System.out.println("]");
		}
	}
}