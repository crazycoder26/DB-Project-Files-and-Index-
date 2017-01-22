import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Set;
import java.util.Stack;

public class InsertQuery {
	
	public InsertQuery(String query, String dbname,String table) throws EOFException {
		String temp = query.substring(query.indexOf("(")+1,query.length()-1);
		System.out.println("This my code:"+ temp);
		String[][] col_details = null;
		String[] values;
		values = temp.split(",");
		int offset;
		try{
		RandomAccessFile tblFile = new RandomAccessFile(dbname+"."+table+".tbl", "rw");
		
		col_details = get_column_details(dbname,table);
		//for(int i=0;i<col_details.length;i++)
			//System.out.println(col_details[i][0]+" "+col_details[i][1]+" "+col_details[i][2]+" "+col_details[i][3]);
		offset = goffset(tblFile);	
		ndxHandler[] col_ndx_handler = new ndxHandler[col_details.length];
		try{
		for(int i = 0;i<col_details.length;i++){
					col_ndx_handler[i] = new ndxHandler();
					col_ndx_handler[i].raffilereader(dbname,table,col_details[i][0],col_details[i][1]);
					//System.out.println("done reading");
					if(values[i].length()==0 && col_details[i][2].toUpperCase().equals("NO"))
						throw new Exception(col_details[i][0]+" cannot be NULL");
					//System.out.println(col_ndx_handler[i].colndx.containsKey(getValue(values[i], col_details[i][1]))+" "+ col_details[i][3].toUpperCase().equals("PRI"));
					if(col_ndx_handler[i].colndx.containsKey(getValue(values[i], col_details[i][1])) && col_details[i][3].equals("PRI"))
						throw new Exception(col_details[i][0]+" Duplicates not allowed");
					System.out.println("Primary key check done");
					if(col_ndx_handler[i].colndx.containsKey(getValue(values[i], col_details[i][1])))
						col_ndx_handler[i].colndx.get(getValue(values[i], col_details[i][1])).add(offset);
					else{
						ArrayList Al = new ArrayList<Integer>();
						Al.add(offset);
						col_ndx_handler[i].colndx.put(getValue(values[i], col_details[i][1]), Al);
						}
					//System.out.println("insert done");
					col_ndx_handler[i].ndxprinter();
					col_ndx_handler[i].ndxfilewriter(dbname,table,col_details[i][0],col_details[i][1]);
				}
			for(int i=0;i<col_details.length;i++)
				tblwriter(values[i],col_details[i][1],tblFile);
		
		}
		catch(Exception e){
			System.out.println(e);
			tblFile.close();
		}
		tblFile.close();
		}
		catch(Exception e){
			System.out.println(e);	
		
		}
		}

	private int goffset(RandomAccessFile tblFile) throws Exception {
		// TODO Auto-generated method stub
		int len = (int) tblFile.length();
		return len;
	}

	private void tblwriter(String value,String datatype, RandomAccessFile tblFile) throws Exception {
		String[] temp = new String[2];
		try{
		int len = 0;
		if(datatype.contains("("))
		{
			datatype=datatype.replace("(", " ");
			datatype=datatype.replace(")", "");
			temp = datatype.split(" ",2);
			datatype = temp[0];
			len = Integer.parseInt(temp[1]);
			if(value.length()>len)
				throw new Exception("size of "+value+" more than "+len);
		}
		
		//System.out.print(value+" ");
		switch(datatype){
			case "int":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeInt(Integer.parseInt(value));
						break;
			case "byte":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeByte(value.length());
						tblFile.writeBytes(value);
						break;
			case "char":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeByte(value.length());
						char[] val_to_char = value.toCharArray();
						for(int i = 0;i<val_to_char.length;i++)
							tblFile.writeChar(val_to_char[i]);
						break;
			case "date":
			case "varchar":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeByte(value.length());
						tblFile.writeBytes(value);
						break;			
			case "short int":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeShort(Short.parseShort(value));
						break;
			case "short":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeShort(Short.parseShort(value));
						break;
			case "long int":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeLong(Long.parseLong(value));
						break;
			case "long":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeLong(Long.parseLong(value));
						break;
			case "double":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeDouble(Double.parseDouble(value));
						break;
			case "float":
						tblFile.seek(0);
						tblFile.seek(tblFile.length());
						tblFile.writeFloat(Float.parseFloat(value));
						break;
			case "datetime":
							/*System.out.println("datetime");
							DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
							Date date = (Date) df.parse(value);
							String parsedDate = df.format(date);*/
							tblFile.seek(0);
							tblFile.seek(tblFile.length());
							tblFile.writeByte(value.length());
							tblFile.writeBytes(value);
							break;
			/*case "date":	System.out.println("date");
							/*DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date date1 = (Date) df1.parse(value);
							String parsedDate1 = df1.format(date1);
							tblFile.seek(0);
							tblFile.seek(tblFile.length());
							tblFile.writeByte(value.length());
							tblFile.writeBytes(value);
							break;*/
			default:
				throw new Exception("Unable to write to table");
				}
		}
		catch(Exception e)
		{
			System.out.println(e);
			tblFile.close();
		}
		
	}

	
	public Object getValue(String value,String datatype){
		String[] temp = new String[2];
		int len = 0;
		try{
		if(datatype.contains("("))
		{
			datatype=datatype.replace("(", " ");
			datatype=datatype.replace(")", "");
			temp = datatype.split(" ",2);
			datatype = temp[0];
			len = Integer.parseInt(temp[1]);
			if(value.length()>len)
				throw new Exception("size of "+value+" exceeds length "+len);
		}
		switch(datatype){
			case "int":
								return Integer.parseInt(value);
			case "long int":
			case "long":
								return Long.parseLong(value);
			case "varchar":
			case "char":
								return value;
			case "short":
			case "short int":
								return(Short.parseShort(value));
		}
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		return null;
	}

	@SuppressWarnings("null")
	private String[][] get_column_details(String schema, String table) throws Exception {
		RandomAccessFile tblfile = new RandomAccessFile("information_schema.table.tbl", "rw");
		RandomAccessFile colfile = new RandomAccessFile("information_schema.columns.tbl", "rw");
		byte[] temp_schema;
		byte[] temp_table;
		String[][] col_details =null;
		String temp_schema_string;
		String temp_table_string;
		boolean flag = false;
		
		try{
			do{
				byte schemaLength = tblfile.readByte();
				System.out.println("pls read!");
				System.out.println("byte "+schemaLength);
				temp_schema = null;
				temp_schema = new byte[schemaLength];
				tblfile.readFully(temp_schema);
				temp_schema_string = new String(temp_schema).toLowerCase();
				byte tableLength = tblfile.readByte();
				temp_table = null;
				temp_table = new byte[tableLength];
				tblfile.readFully(temp_table);
				temp_table_string = new String(temp_table).toLowerCase();
				tblfile.readLong();
				//System.out.println(schema+" "+temp_schema_string+" "+table+" "+temp_table_string+" "+ temp_schema_string.equals(schema) + " "+ temp_table_string.equals(table));
				if(temp_schema_string.equals(schema) && temp_table_string.equals(table))
					{flag = true; // table check
					break;}		
			}while(tblfile.getFilePointer()!=tblfile.length());
		if(!flag)
			throw new Exception("Table does not Exist");
		byte[] read = new byte[6];	
		byte[] temp_null_bytes;
		byte[] temp_pri_bytes;
		byte[] temp_schema_bytes;
		int ord_pos;
		byte[] temp_table_bytes;
		byte[] temp_col_bytes;
		byte[] temp_datatype_bytes;
		ArrayList<String> colname = new ArrayList<String>();
		ArrayList<String> pk = new ArrayList<String>();
		ArrayList<String> dtype = new ArrayList<String>();
		ArrayList<String> notnull = new ArrayList<String>();
			do{
				read[0] = colfile.readByte();
				temp_schema_bytes = new byte[read[0]];
				colfile.readFully(temp_schema_bytes);
				read[1] = colfile.readByte();
				temp_table_bytes = new byte[read[1]];
				colfile.readFully(temp_table_bytes);
				//System.out.println(new String(temp_schema_bytes)+" "+new String(temp_table_bytes));
				read[2] = colfile.readByte();
				temp_col_bytes = new byte[read[2]];
				colfile.readFully(temp_col_bytes);
				ord_pos = colfile.readInt();
				read[3] = colfile.readByte();
				temp_datatype_bytes = new byte[read[3]];
				colfile.readFully(temp_datatype_bytes);
				read[4] = colfile.readByte();
				temp_null_bytes = new byte[read[4]];
				colfile.readFully(temp_null_bytes);
				read[5] = colfile.readByte();
				temp_pri_bytes = new byte[read[5]];
				colfile.readFully(temp_pri_bytes);
				if(table.equals(new String(temp_table_bytes).toLowerCase()) && schema.equals(new String(temp_schema_bytes)))
				{
					colname.add(new String(temp_col_bytes));
					dtype.add(new String(temp_datatype_bytes).toLowerCase());
					notnull.add(new String(temp_null_bytes));
					if(read[5]==0)
						pk.add("");
					else
						pk.add(new String(temp_pri_bytes));
				}
			}while(colfile.getFilePointer()!=colfile.length());
			col_details = new String[colname.size()][4];
			for(int i=0;i<colname.size();i++)
			{
				col_details[i][0] = colname.get(i);
				col_details[i][1] = dtype.get(i);
				col_details[i][2] = notnull.get(i);
				col_details[i][3] = pk.get(i);
			}
			tblfile.close();
			colfile.close();
		}
		catch(Exception e){
			System.out.println(e);
			tblfile.close();
			colfile.close();
		}
		
		return col_details;
	}	
}
