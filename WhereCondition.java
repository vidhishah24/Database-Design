import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

public class WhereCondition {

	public static boolean whereMethod(Expression expression, String tuple, String header, String types)
			throws IOException {
		if (expression instanceof EqualsTo) {
			EqualsTo EqualsToExpression = (EqualsTo) expression;
			Expression leftExpression = (EqualsToExpression.getLeftExpression());
			Expression rightExpression = (EqualsToExpression.getRightExpression());

			String rightVal = rightExpression.toString();
			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}
			String type = types.split("\\|")[index];

			String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
				if (values[index].equals(rightVal.substring(1, rightVal.length() - 1))) {
					return true;
				}
			} else if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {

				if (Double.parseDouble(values[index]) == Double.parseDouble(rightVal)) {
					return true;
				}

			} else if (type.equalsIgnoreCase("date")) {

				if (values[index].equals(rightVal.substring(1, rightVal.length() - 1))) {
					return true;
				}
			}
		}

		else if (expression instanceof NotEqualsTo)

		{

			NotEqualsTo NotEqualsToExpression = (NotEqualsTo) expression;
			Expression leftExpression = (NotEqualsToExpression.getLeftExpression());
			Expression rightExpression = (NotEqualsToExpression.getRightExpression());

			String rightVal = rightExpression.toString();

			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}
			String type = types.split("\\|")[index];
			String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
				if (!values[index].equals(rightVal.substring(1, rightVal.length() - 1))) {
					// intermediateTable.add(tuple);
					return true;
				}
			} else if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {

				if (Double.parseDouble(values[index]) != Double.parseDouble(rightVal)) {
					// intermediateTable.add(tuple);
					return true;
				}

			} else if (type.equalsIgnoreCase("date")) {

				if (!values[index].equals(rightVal.substring(1, rightVal.length() - 1))) {
					// intermediateTable.add(tuple);
					return true;
				}
			}

		}

		else if (expression instanceof LikeExpression) {
			LikeExpression likeExpression = (LikeExpression) expression;
			Expression leftExpression = (likeExpression.getLeftExpression());
			Expression rightExpression = (likeExpression.getRightExpression());

			// int tupleNo = 0;

			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}

			// for (String tuple : table.subList(2, table.size())) {
			// tupleNo++;
			String values[] = tuple.split("\\|");

			if (values[index].equalsIgnoreCase(
					rightExpression.toString().substring(1, rightExpression.toString().length() - 1))) {
				// intermediateTable.add(tuple);
				return true;
			}

		}

		else if (expression instanceof GreaterThan)

		{

			GreaterThan greaterThanExpression = (GreaterThan) expression;
			Expression leftExpression = (greaterThanExpression.getLeftExpression());
			Expression rightExpression = (greaterThanExpression.getRightExpression());

			String rightVal = rightExpression.toString();

			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}
			String type = types.split("\\|")[index];
			// int tupleNo = 0;

			// for (String tuple : table.subList(2, table.size())) {
			// tupleNo++;
			String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {

				if (Double.parseDouble(values[index]) > Double.parseDouble(rightVal)) {
					// intermediateTable.add(tuple);
					return true;
				}

			} else if (type.equalsIgnoreCase("date")) {
				String leftDate[] = values[index].split("-");

				String rightDate[] = null;

				rightDate = rightVal.substring(1, rightVal.length() - 1).split("-");

				if (Integer.parseInt(leftDate[0]) < Integer.parseInt(rightDate[0])) {

				} else if (Integer.parseInt(leftDate[0]) == Integer.parseInt(rightDate[0])) {
					if (Integer.parseInt(leftDate[1]) < Integer.parseInt(rightDate[1])) {
					} else if (Integer.parseInt(leftDate[1]) == Integer.parseInt(rightDate[1])) {
						if (Integer.parseInt(leftDate[2]) <= Integer.parseInt(rightDate[2])) {
						} else {

							// intermediateTable.add(tuple);
							return true;
						}
					} else {
						// intermediateTable.add(tuple);
						return true;
					}
				} else {

					// intermediateTable.add(tuple);
					return true;
				}
			}

		}

		else if (expression instanceof GreaterThanEquals)

		{

			GreaterThanEquals greaterThanEqualsExpression = (GreaterThanEquals) expression;
			Expression leftExpression = (greaterThanEqualsExpression.getLeftExpression());
			Expression rightExpression = (greaterThanEqualsExpression.getRightExpression());
			String rightVal = rightExpression.toString();

			int index = 0;

			for (String str : header.split("\\|")) {

				if (str.equalsIgnoreCase(leftExpression.toString())) {
					break;
				}
				index++;
			}
			String type = types.split("\\|")[index];
			String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {

				if (Double.parseDouble(values[index]) >= Double.parseDouble(rightVal)) {
					
					return true;
				}

			} else if (type.equalsIgnoreCase("date")) {
				
				String leftDate[] = values[index].split("-");

				String rightDate[] = rightVal.substring(1, rightVal.length() - 1).split("-");

				if (Integer.parseInt(leftDate[0]) < Integer.parseInt(rightDate[0])) {

				} else if (Integer.parseInt(leftDate[0]) == Integer.parseInt(rightDate[0])) {
					if (Integer.parseInt(leftDate[1]) < Integer.parseInt(rightDate[1])) {
					} else if (Integer.parseInt(leftDate[1]) == Integer.parseInt(rightDate[1])) {
						if (Integer.parseInt(leftDate[2]) < Integer.parseInt(rightDate[2])) {
						} else {
							
							return true;
						}
					} else {
						
						return true;
					}
				} else {
					
					return true;
				}
			}

		}

		else if (expression instanceof MinorThan)

		{

			MinorThan minorThanExpression = (MinorThan) expression;
			Expression leftExpression = (minorThanExpression.getLeftExpression());

			Expression rightExpression = (minorThanExpression.getRightExpression());

			String rightVal = rightExpression.toString();

			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}
			String type = types.split("\\|")[index];
						String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {
				
				if (Double.parseDouble(values[index]) < Double.parseDouble(rightVal)) {
						return true;
				}

			} else if (type.equalsIgnoreCase("date")) {
				String leftDate[] = values[index].split("-");

				String rightDate[] = null;
				rightDate = rightVal.substring(1, rightVal.length() - 1).split("-");

				if (Integer.parseInt(leftDate[0]) > Integer.parseInt(rightDate[0])) {

				} else if (Integer.parseInt(leftDate[0]) == Integer.parseInt(rightDate[0])) {
					if (Integer.parseInt(leftDate[1]) > Integer.parseInt(rightDate[1])) {
					} else if (Integer.parseInt(leftDate[1]) == Integer.parseInt(rightDate[1])) {
						if (Integer.parseInt(leftDate[2]) >= Integer.parseInt(rightDate[2])) {
						} else {

						
							return true;
						}
					} else {

						
						return true;
					}
				} else {

					
					return true;
				}

			}

		} else if (expression instanceof MinorThanEquals)

		{

			MinorThanEquals minorThanEqualsExpression = (MinorThanEquals) expression;
			Expression leftExpression = (minorThanEqualsExpression.getLeftExpression());

			Expression rightExpression = (minorThanEqualsExpression.getRightExpression());

			String rightVal = rightExpression.toString();

			int index = 0;
			for (String str : header.split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index++;
			}
			String type = types.split("\\|")[index];
						String values[] = tuple.split("\\|");

			if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")
					|| type.equalsIgnoreCase("DOUBLE")) {

				if (Double.parseDouble(values[index]) <= Double.parseDouble(rightVal)) {
					
					return true;
				}

			} else if (type.equalsIgnoreCase("date")) {
				String leftDate[] = values[index].split("-");

				String rightDate[] = null;

				rightDate = rightVal.substring(1, rightVal.length() - 1).split("-");

				if (Integer.parseInt(leftDate[0]) > Integer.parseInt(rightDate[0])) {

				} else if (Integer.parseInt(leftDate[0]) == Integer.parseInt(rightDate[0])) {
					if (Integer.parseInt(leftDate[1]) > Integer.parseInt(rightDate[1])) {
					} else if (Integer.parseInt(leftDate[1]) == Integer.parseInt(rightDate[1])) {
						if (Integer.parseInt(leftDate[2]) > Integer.parseInt(rightDate[2])) {
						} else {

						
							return true;
						}
					} else {
						
						return true;
					}
				} else {
					
					return true;
				}
			}

		}

		return false;

	}

	public static ArrayList<String> whereJoinMethod(Expression expression, ArrayList<String> table1,
			ArrayList<String> table2,Map<String,Integer> joinCount) throws IOException {
		
		ArrayList<String> output = new ArrayList<String>();
		output.add(table1.get(0) + "|" + table2.get(0));
		output.add(table1.get(1) + "|" + table2.get(1));
		EqualsTo EqualsToExpression = (EqualsTo) expression;
		Expression leftExpression = (EqualsToExpression.getLeftExpression());
		Expression rightExpression = (EqualsToExpression.getRightExpression());
		int index1 = 0, index2 = 0;
		
		for (String str : table1.get(0).split("\\|")) {
			if (str.equalsIgnoreCase(leftExpression.toString()))
				break;
			index1++;
		}
		
		for (String str : table2.get(0).split("\\|")) {
			
			if (str.equalsIgnoreCase(rightExpression.toString()))
				break;
			index2++;
		}
		
		HashMap<String, ArrayList<String>> tableMap = new HashMap<String, ArrayList<String>>();
		if (joinCount.get(rightExpression.toString().split("\\.")[0])< joinCount.get(leftExpression.toString().split("\\.")[0])) {
			
			for (String tuple1 : table2.subList(2, table2.size())) {
				StringTokenizer tuple1Split = new StringTokenizer(tuple1, "\\|");
				int count = 0;
				while (count < index2) {
					tuple1Split.nextToken();
					count++;
				}
				
				String temp = tuple1Split.nextToken();
			
				if (!temp.equalsIgnoreCase("NULL")) {
					if (tableMap.containsKey(temp)) {
						tableMap.get(temp).add(tuple1);
					} else {
						
						ArrayList<String> tupleList = new ArrayList<String>();
						tupleList.add(tuple1);
						tableMap.put(temp, tupleList);
					}
				}

			}
			
			joinCount.put(rightExpression.toString().split("\\.")[0],joinCount.get(rightExpression.toString().split("\\.")[0])-1 );
			if(joinCount.get(rightExpression.toString().split("\\.")[0])==0){
			table2.clear();
			table2.trimToSize();
			}
			
			
			for (String tuple2 : table1.subList(2, table1.size())) {
				StringTokenizer tuple2Split = new StringTokenizer(tuple2, "\\|");
				int count = 0;
				
				while (count < index1) {
					tuple2Split.nextToken();
					count++;
				}
				
				String temp = tuple2Split.nextToken();
				
				if (tableMap.containsKey(temp)) {

					for (String str : tableMap.get(temp)) {
						output.add( tuple2+"|"+str);
					}
				}
			}
			joinCount.put(leftExpression.toString().split("\\.")[0],joinCount.get(leftExpression.toString().split("\\.")[0])-1 );
			if(joinCount.get(leftExpression.toString().split("\\.")[0])==0){
			table1.clear();
			table1.trimToSize();
			}

		} else {
			
			for (String tuple1 : table1.subList(2, table1.size())) {
				StringTokenizer tuple1Split = new StringTokenizer(tuple1, "\\|");
				int count = 0;
				while (count < index1) {
					tuple1Split.nextToken();
					count++;
				}
				String temp = tuple1Split.nextToken();
				if (!temp.equalsIgnoreCase("NULL")) {
					if (tableMap.containsKey(temp)) {
						tableMap.get(temp).add(tuple1);
					} else {
						ArrayList<String> tupleList = new ArrayList<String>();
						tupleList.add(tuple1);
						tableMap.put(temp, tupleList);
					}
				}

			}
			joinCount.put(leftExpression.toString().split("\\.")[0],joinCount.get(leftExpression.toString().split("\\.")[0])-1 );
			if(joinCount.get(leftExpression.toString().split("\\.")[0])==0){
			table1.clear();
			table1.trimToSize();
			}

			

			for (String tuple2 : table2.subList(2, table2.size())) {
				StringTokenizer tuple2Split = new StringTokenizer(tuple2, "\\|");
				int count = 0;
				while (count < index2) {
					tuple2Split.nextToken();
					count++;
				}
				String temp = tuple2Split.nextToken();
				if (tableMap.containsKey(temp)) {

					for (String str : tableMap.get(temp)) {
						output.add(str + "|" + tuple2);
					}
				}
			}
		}
		joinCount.put(rightExpression.toString().split("\\.")[0],joinCount.get(rightExpression.toString().split("\\.")[0])-1 );
		if(joinCount.get(rightExpression.toString().split("\\.")[0])==0){
		table2.clear();
		table2.trimToSize();
		}


		return output;

	}

	public static ArrayList<String> whereANDJoinMethod(List<Expression> joinList, ArrayList<String> table1,
			ArrayList<String> table2) throws IOException {
		
		ArrayList<String> output = new ArrayList<String>();
		output.add(table1.get(0) + "|" + table2.get(0));
		output.add(table1.get(1) + "|" + table2.get(1));
		ArrayList<Integer> indexArray = new ArrayList<Integer>();
		for (Expression e : joinList) {
			EqualsTo EqualsToExpression = (EqualsTo) e;
			Expression leftExpression = (EqualsToExpression.getLeftExpression());
			Expression rightExpression = (EqualsToExpression.getRightExpression());
			int index1 = 0, index2 = 0;
			for (String str : table1.get(0).split("\\|")) {
				if (str.equalsIgnoreCase(leftExpression.toString()))
					break;
				index1++;
			}
			
			for (String str : table2.get(0).split("\\|")) {
				
				if (str.equalsIgnoreCase(rightExpression.toString()))
					break;
				index2++;
			}
			indexArray.add(index1);
			indexArray.add(index2);
		}

		HashMap<String, ArrayList<String>> tableMap = new HashMap<String, ArrayList<String>>();
		if (table1.size() > table2.size()) {
			for (String tuple1 : table2.subList(2, table2.size())) {
				String[] tuple1Split = tuple1.split("\\|");

				String temp1 = tuple1Split[indexArray.get(1)];
				String temp2 = tuple1Split[indexArray.get(3)];
				if (!temp1.equalsIgnoreCase("NULL") && !temp2.equalsIgnoreCase("NULL")) {
					if (tableMap.containsKey(temp1 + "|" + temp2)) {
						tableMap.get(temp1 + "|" + temp2).add(tuple1);
					} else {
						
						ArrayList<String> tupleList = new ArrayList<String>();
						tupleList.add(tuple1);
						tableMap.put(temp1 + "|" + temp2, tupleList);
					}
				}

			}
			
			for (String tuple2 : table1.subList(2, table1.size())) {
				String[] tuple2Split = tuple2.split("\\|");

				String temp1 = tuple2Split[indexArray.get(0)];
				String temp2 = tuple2Split[indexArray.get(2)];
				if (tableMap.containsKey(temp1 + "|" + temp2)) {
					for (String str : tableMap.get(temp1 + "|" + temp2)) {
						output.add(str + "|" + tuple2);
					}

				}
			}
		} else {
			
			for (String tuple1 : table1.subList(2, table1.size())) {
				String[] tuple1Split = tuple1.split("\\|");

				String temp1 = tuple1Split[indexArray.get(0)];
				String temp2 = tuple1Split[indexArray.get(2)];
				if (!temp1.equalsIgnoreCase("NULL") && !temp2.equalsIgnoreCase("NULL")) {
					if (tableMap.containsKey(temp1 + "|" + temp2)) {
						tableMap.get(temp1 + "|" + temp2).add(tuple1);
					} else {
						
						ArrayList<String> tupleList = new ArrayList<String>();
						tupleList.add(tuple1);
						tableMap.put(temp1 + "|" + temp2, tupleList);
					}
				}

			}
			
			for (String tuple2 : table2.subList(2, table2.size())) {
				String[] tuple2Split = tuple2.split("\\|");

				String temp1 = tuple2Split[indexArray.get(1)];
				String temp2 = tuple2Split[indexArray.get(3)];
				if (tableMap.containsKey(temp1 + "|" + temp2)) {
					for (String str : tableMap.get(temp1 + "|" + temp2)) {
						output.add(str + "|" + tuple2);
					}

				}
			}
		}
		
		return output;

	}

	public static ArrayList<String> whereOuterJoin(ArrayList<String> table1, ArrayList<String> table2) {
		ArrayList<String> output = new ArrayList<String>();
		output.add(table1.get(0) + "|" + table2.get(0));
		output.add(table1.get(1) + "|" + table2.get(1));
		for (String str : table1.subList(2, table1.size())) {
			for (String s : table2.subList(2, table2.size())) {
				output.add(str + "|" + s);
			}
		}
		return output;

	}

	public static ArrayList<Integer> calculate(Expression expression, ArrayList<String> table)
			throws NumberFormatException, IOException {
		ArrayList<Integer> rowNum = new ArrayList<Integer>();
		return rowNum;
	}

	public static ArrayList<String> populateTable(ArrayList<String> table, ArrayList<Integer> rowNum) {

		ArrayList<String> intermediateTable = new ArrayList<String>();
		int tupleNo = 0;
		intermediateTable.add(table.get(0));
		intermediateTable.add(table.get(1));
		for (String tuple : table.subList(2, table.size())) {
			tupleNo++;
			if (rowNum.contains(tupleNo)) {
				intermediateTable.add(tuple);
			}
		}
		
		return intermediateTable;
	}

	public static boolean whereMethodIn(String rightExpression, String value, String type) {
		String items[] = rightExpression.toString().split(",");
		ArrayList<Integer> rowNum = new ArrayList<Integer>();

		if (type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
			for (String s : items) {
				s = s.trim();
				if (value.equals(s.substring(1, s.length() - 1))) {
					return true;
				}
			}
		} else if (type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("decimal")) {
			for (String s : items) {
				s = s.trim();
				if (Double.parseDouble(value) == Double.parseDouble(s)) {
					
					return true;
				}

			}
		} else if (type.equalsIgnoreCase("date")) {
			for (String s : items) {
				s = s.trim();
				if (value.equals(s.substring(1, s.length() - 1))) {
					
					return true;
				}
			}
		}
		return false;

	}

	

}
