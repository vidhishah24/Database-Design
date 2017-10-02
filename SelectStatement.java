import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectStatement {

	@SuppressWarnings("unchecked")
	public static Results selectMethod(PlainSelect selectBody, HashMap<String, SQLParser> tables, boolean subquery)
			throws IOException, SQLException {
		Results results = new Results();
		Results subQuery = new Results();
		boolean aggregate = false;
		boolean groupBy = false;
		boolean orFlag = false;
		ArrayList<String> intermediateList = new ArrayList<String>();
		ArrayList<String> tableNamesString = new ArrayList<String>();
		HashMap<String, ArrayList<String>> tableListMap = new HashMap<String, ArrayList<String>>();

		ArrayList<Expression> nonJoinList = new ArrayList<Expression>();
		List<Expression> tempJoinList = new ArrayList<Expression>();
		ArrayList<String> orList = new ArrayList<String>();
		Expression where = selectBody.getWhere();

		if (where.toString().contains(" OR")) {
			orFlag = true;
		}
		if (where != null) {
			if (where instanceof AndExpression) {
				while (where instanceof AndExpression) {
					AndExpression andExpression = (AndExpression) where;
					Expression leftExpression = (andExpression.getLeftExpression());
					Expression rightExpression = (andExpression.getRightExpression());
					if (!(rightExpression instanceof EqualsTo)) {
						nonJoinList.add(rightExpression);
					} else {
						EqualsTo EqualsToExpression = (EqualsTo) rightExpression;
						Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
						String rightEqualsToArray[] = rightEqualsTo.toString().split("\\.");
						if (rightEqualsToArray.length > 1) {
							tempJoinList.add(rightExpression);
						} else
							nonJoinList.add(rightExpression);
					}
					where = leftExpression;
				}
				if (!(where instanceof EqualsTo)) {
					nonJoinList.add(where);
				} else {
					EqualsTo EqualsToExpression = (EqualsTo) where;
					Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
					String rightEqualsToArray[] = rightEqualsTo.toString().split("\\.");
					if (rightEqualsToArray.length > 1) {
						tempJoinList.add(where);
					} else
						nonJoinList.add(where);
				}
			} else if (!(where instanceof EqualsTo)) {
				nonJoinList.add(where);
			} else if (where instanceof EqualsTo) {
				EqualsTo EqualsToExpression = (EqualsTo) where;
				Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
				String rightEqualsToArray[] = rightEqualsTo.toString().split("\\.");
				if (rightEqualsToArray.length > 1) {
					tempJoinList.add(where);
				} else
					nonJoinList.add(where);
			}
		}

		// System.exit(0);
		HashMap<String, Integer> tempJoinCount = new HashMap<String, Integer>();
		for (Expression e : tempJoinList) {
			EqualsTo EqualsToExpression = (EqualsTo) e;
			Expression leftEqualsTo = (EqualsToExpression.getLeftExpression());
			Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
			String leftTemp = leftEqualsTo.toString().split("\\.")[0];
			String rightTemp = rightEqualsTo.toString().split("\\.")[0];
			// System.out.println("left: " + leftEqualsTo);
			// System.out.println("right: " + rightEqualsTo);

			if (tempJoinCount.containsKey(leftTemp)) {
				// System.out.println("left contains");
				tempJoinCount.put(leftTemp, tempJoinCount.get(leftTemp) + 1);
			} else { // (!tempJoinCount.containsKey(leftTemp))
				// System.out.println("left not contains");
				tempJoinCount.put(leftTemp, 1);
			}

			if (tempJoinCount.containsKey(rightTemp)) {
				// System.out.println("right contains");
				tempJoinCount.put(rightTemp, tempJoinCount.get(rightTemp) + 1);
			} else {
				// System.out.println("right not contains");//
				// (!tempJoinCount.containsKey(rightTemp))
				// {
				tempJoinCount.put(rightTemp, 1);
			}
		}

		if ((selectBody) != null) {
			if (selectBody.getFromItem().toString().contains("SELECT ")
					|| selectBody.getFromItem().toString().contains("select ")
					|| selectBody.getFromItem().toString().contains("Select ")) {
				FromItem fromItem = selectBody.getFromItem();
				String alias = ((SubSelect) fromItem).getAlias();
				subQuery = SelectStatement.selectMethod((PlainSelect) ((SubSelect) fromItem).getSelectBody(), tables,
						true);
				if (alias != null) {
					subQuery.subQueryAlias = alias.toLowerCase();
					String sArray[] = subQuery.output.get(0).split("\\|");
					for (int i = 0; i < sArray.length; i++) {
						if (sArray[i].contains(".")) {
							sArray[i] = alias.toLowerCase() + "." + sArray[i].split("\\.")[1];
						} else {
							sArray[i] = alias.toLowerCase() + "." + sArray[i];
						}
					}
					String temp = "";
					for (String s : sArray) {
						temp = temp + s + "|";
					}
					subQuery.output.set(0, temp.substring(0, temp.length() - 1));
					results.subQueryAdd(subQuery);
					tableNamesString.add(alias.toLowerCase());
					tableListMap.put(alias.toLowerCase(), subQuery.output);
				}
			} else {
				tableNamesString.add(selectBody.getFromItem().toString().toLowerCase());
			}
		}

		if (selectBody.getJoins() != null) {
			for (Join tableToJoin : selectBody.getJoins()) {
				if (tableToJoin.toString().contains("SELECT ") || tableToJoin.toString().contains("select ")
						|| tableToJoin.toString().contains("Select ")) {
					FromItem fromItem = tableToJoin.getRightItem();
					String alias = ((SubSelect) fromItem).getAlias();
					subQuery = SelectStatement.selectMethod((PlainSelect) ((SubSelect) fromItem).getSelectBody(),
							tables, true);
					if (alias != null) {
						subQuery.subQueryAlias = alias.toLowerCase();
						String sArray[] = subQuery.output.get(0).split("\\|");
						for (int i = 0; i < sArray.length; i++) {
							if (sArray[i].contains(".")) {
								sArray[i] = alias.toLowerCase() + "." + sArray[i].split("\\.")[1];
							} else {
								sArray[i] = alias.toLowerCase() + "." + sArray[i];
							}
						}
						String temp = "";
						for (String s : sArray) {
							temp = temp + s + "|";
						}
						subQuery.output.set(0, temp.substring(0, temp.length() - 1));
						tableNamesString.add(alias.toLowerCase());
						tableListMap.put(alias.toLowerCase(), subQuery.output);
						results.subQueryAdd(subQuery);
					}
				} else {
					tableNamesString.add(tableToJoin.toString().toLowerCase());
				}
			}
		}
		for (String tableName : tableNamesString) {
			int count = 0;
			String tableNameSplit[] = tableName.split(" ");
			if (tableNameSplit.length > 1) {
				results.fromTables.put(tableNameSplit[2].toLowerCase(), tableNameSplit[0].toLowerCase());
			} else {
				results.fromTables.put(tableNameSplit[0].toLowerCase(), tableNameSplit[0].toLowerCase());
			}
		}

		if (orFlag) {

			Expression e = selectBody.getWhere();

			AndExpression andExpression = (AndExpression) e;
			Expression leftExpression = andExpression.getLeftExpression();
			Expression rightExpression = andExpression.getRightExpression();

			if (rightExpression != null) {
				while (rightExpression instanceof OrExpression) {
					HashMap<String, ArrayList<String>> tempTableListMap = new HashMap<String, ArrayList<String>>();
					if (rightExpression instanceof OrExpression) {

						OrExpression inOrExpression = (OrExpression) rightExpression;
						Expression inLeftExpression = (inOrExpression.getLeftExpression());
						Expression inRightExpression = (inOrExpression.getRightExpression());

						while (inRightExpression instanceof AndExpression) {

							AndExpression inAndExpression = (AndExpression) inRightExpression;
							Expression leftExp = (inAndExpression.getLeftExpression());
							Expression rightExp = (inAndExpression.getRightExpression());

							ArrayList<String> tableArrayList = new ArrayList<String>();
							if (rightExp instanceof InExpression) {
								String right = "";
								InExpression inExpression = (InExpression) rightExp;
								Expression inInLeftExpression = (inExpression.getLeftExpression());
								ItemsList inInRightExpression = inExpression.getItemsList();

								right = inInRightExpression.toString().substring(1,
										inInRightExpression.toString().length() - 1);
								ArrayList<String> tableArrayListIn = new ArrayList<String>();
								String temp = inInLeftExpression.toString().split("\\.")[0].toLowerCase();
								if (tempTableListMap.containsKey(temp)) {
									int index = 0;
									for (String str : tempTableListMap.get(temp).get(0).split("\\|")) {
										if (str.equalsIgnoreCase(inInLeftExpression.toString()))
											break;
										index++;
									}
									String type = tempTableListMap.get(temp).get(1).split("\\|")[index];
									tableArrayListIn.add(tempTableListMap.get(temp).get(0));
									tableArrayListIn.add(tempTableListMap.get(temp).get(1));
									for (String tuple : tempTableListMap.get(temp).subList(2,
											tempTableListMap.get(temp).size())) {
										String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethodIn(right, values[index], type))
											tableArrayListIn.add(tuple);
										// tuple = br.readLine();
									}
									tempTableListMap.put(temp, tableArrayListIn);
								} else {
									SQLParser table = null;
									table = tables.get(results.fromTables.get(temp));
									// System.out.println(table.tableFilePath);
									FileReader fr = new FileReader(table.tableFilePath);
									BufferedReader br = new BufferedReader(fr);
									String tuple = br.readLine();
									String firstRow = "";
									String secondRow = "";
									// if (tableNameSplit.length > 2) {
									table.aliasName = temp;
									for (String columnName : table.columnNames) {
										firstRow = firstRow + table.aliasName + "." + columnName + "|";
										secondRow = secondRow
												+ table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
														.getColDataType().getDataType()
												+ "|";
									}
									tableArrayListIn.add(firstRow.substring(0, firstRow.length() - 1));
									tableArrayListIn.add(secondRow.substring(0, secondRow.length() - 1));
									int index = 0;
									for (String str : firstRow.split("\\|")) {
										if (str.equalsIgnoreCase(inInLeftExpression.toString()))
											break;
										index++;
									}
									String type = secondRow.split("\\|")[index];
									while (tuple != null) {
										String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethodIn(right, values[index], type))
											tableArrayListIn.add(tuple);
										tuple = br.readLine();
									}
								}
								tempTableListMap.put(temp, tableArrayListIn);
							} else {
								String temp = rightExp.toString().split("\\.")[0].toLowerCase();
								if (tempTableListMap.containsKey(temp)) {
									tableArrayList.add(tempTableListMap.get(temp).get(0));
									tableArrayList.add(tempTableListMap.get(temp).get(1));
									for (String tuple : tempTableListMap.get(temp).subList(2,
											tempTableListMap.get(temp).size())) {
										if (WhereCondition.whereMethod(rightExp, tuple,
												tempTableListMap.get(temp).get(0), tempTableListMap.get(temp).get(1)))
											tableArrayList.add(tuple);
									}
									tempTableListMap.put(temp, tableArrayList);
								} else {
									// System.out.println("idahr aa");
									SQLParser table = null;
									table = tables.get(results.fromTables.get(temp));
									// System.out.println(table.tableFilePath);

									FileReader fr = new FileReader(table.tableFilePath);
									BufferedReader br = new BufferedReader(fr);
									String tuple = br.readLine();
									String firstRow = "";
									String secondRow = "";
									// if (tableNameSplit.length > 2) {
									table.aliasName = temp;
									for (String columnName : table.columnNames) {
										firstRow = firstRow + table.aliasName + "." + columnName + "|";
										secondRow = secondRow
												+ table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
														.getColDataType().getDataType()
												+ "|";
									}
									tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
									tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
									while (tuple != null) {
										// String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethod(rightExp, tuple, firstRow, secondRow))
											tableArrayList.add(tuple);
										tuple = br.readLine();
									}
									tempTableListMap.put(temp, tableArrayList);
								}
							}
							inRightExpression = leftExp;
						}
						ArrayList<String> tableArrayList = new ArrayList<String>();
						String temp = inRightExpression.toString().split("\\.")[0].toLowerCase();
						if (tempTableListMap.containsKey(temp)) {
							tableArrayList.add(tempTableListMap.get(temp).get(0));
							tableArrayList.add(tempTableListMap.get(temp).get(1));
							for (String tuple : tempTableListMap.get(temp).subList(2,
									tempTableListMap.get(temp).size())) {
								if (WhereCondition.whereMethod(inRightExpression, tuple,
										tempTableListMap.get(temp).get(0), tempTableListMap.get(temp).get(1)))
									tableArrayList.add(tuple);
							}
							tempTableListMap.put(temp, tableArrayList);
						} else {
							// System.out.println("idahr aa");
							SQLParser table = null;
							table = tables.get(results.fromTables.get(temp));
							// System.out.println(table.tableFilePath);
							FileReader fr = new FileReader(table.tableFilePath);
							BufferedReader br = new BufferedReader(fr);
							String tuple = br.readLine();
							String firstRow = "";
							String secondRow = "";
							// if (tableNameSplit.length > 2) {
							table.aliasName = temp;
							for (String columnName : table.columnNames) {
								firstRow = firstRow + table.aliasName + "." + columnName + "|";
								secondRow = secondRow + table.columnDescriptionList
										.get(table.columnNames.indexOf(columnName)).getColDataType().getDataType()
										+ "|";
							}
							tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
							tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
							while (tuple != null) {
								// String values[] = tuple.split("\\|");
								if (WhereCondition.whereMethod(inRightExpression, tuple, firstRow, secondRow))
									tableArrayList.add(tuple);
								tuple = br.readLine();
							}
							tempTableListMap.put(temp, tableArrayList);

							// tableListMap = (HashMap<String,
							// ArrayList<String>>) tempTableListMap.clone();

						}

						for (String key : tempTableListMap.keySet()) {
							if (tableListMap.containsKey(key)) {
								ArrayList<String> tempArrayList = tableListMap.get(key);
								tempArrayList
										.addAll(tempTableListMap.get(key).subList(2, tempTableListMap.get(key).size()));
								tableListMap.put(key, tempArrayList);
							} else {
								tableListMap.put(key, tempTableListMap.get(key));
							}
						}
						tempTableListMap.clear();
						rightExpression = inLeftExpression;
					}

				}
				HashMap<String, ArrayList<String>> tempTableListMap = new HashMap<String, ArrayList<String>>();
				while (rightExpression instanceof AndExpression) {

					AndExpression inAndExpression = (AndExpression) rightExpression;
					Expression leftExp = (inAndExpression.getLeftExpression());
					Expression rightExp = (inAndExpression.getRightExpression());

					ArrayList<String> tableArrayList = new ArrayList<String>();
					if (rightExp instanceof InExpression) {
						String right = "";
						InExpression inExpression = (InExpression) rightExp;
						Expression inInLeftExpression = (inExpression.getLeftExpression());
						ItemsList inInRightExpression = inExpression.getItemsList();

						right = inInRightExpression.toString().substring(1,
								inInRightExpression.toString().length() - 1);
						ArrayList<String> tableArrayListIn = new ArrayList<String>();
						String temp = inInLeftExpression.toString().split("\\.")[0].toLowerCase();
						if (tempTableListMap.containsKey(temp)) {
							int index = 0;
							for (String str : tempTableListMap.get(temp).get(0).split("\\|")) {
								if (str.equalsIgnoreCase(inInLeftExpression.toString()))
									break;
								index++;
							}
							String type = tempTableListMap.get(temp).get(1).split("\\|")[index];
							tableArrayListIn.add(tempTableListMap.get(temp).get(0));
							tableArrayListIn.add(tempTableListMap.get(temp).get(1));
							for (String tuple : tempTableListMap.get(temp).subList(2,
									tempTableListMap.get(temp).size())) {
								String values[] = tuple.split("\\|");
								if (WhereCondition.whereMethodIn(right, values[index], type))
									tableArrayListIn.add(tuple);
								// tuple = br.readLine();
							}
							tempTableListMap.put(temp, tableArrayListIn);
						} else {
							SQLParser table = null;
							table = tables.get(results.fromTables.get(temp));
							// System.out.println(table.tableFilePath);
							FileReader fr = new FileReader(table.tableFilePath);
							BufferedReader br = new BufferedReader(fr);
							String tuple = br.readLine();
							String firstRow = "";
							String secondRow = "";
							// if (tableNameSplit.length > 2) {
							table.aliasName = temp;
							for (String columnName : table.columnNames) {
								firstRow = firstRow + table.aliasName + "." + columnName + "|";
								secondRow = secondRow + table.columnDescriptionList
										.get(table.columnNames.indexOf(columnName)).getColDataType().getDataType()
										+ "|";
							}
							tableArrayListIn.add(firstRow.substring(0, firstRow.length() - 1));
							tableArrayListIn.add(secondRow.substring(0, secondRow.length() - 1));
							int index = 0;
							for (String str : firstRow.split("\\|")) {
								if (str.equalsIgnoreCase(inInLeftExpression.toString()))
									break;
								index++;
							}
							String type = secondRow.split("\\|")[index];
							while (tuple != null) {
								String values[] = tuple.split("\\|");
								if (WhereCondition.whereMethodIn(right, values[index], type))
									tableArrayListIn.add(tuple);
								tuple = br.readLine();
							}
						}
						tempTableListMap.put(temp, tableArrayListIn);
					} else {
						String temp = rightExp.toString().split("\\.")[0].toLowerCase();
						if (tempTableListMap.containsKey(temp)) {
							tableArrayList.add(tempTableListMap.get(temp).get(0));
							tableArrayList.add(tempTableListMap.get(temp).get(1));
							for (String tuple : tempTableListMap.get(temp).subList(2,
									tempTableListMap.get(temp).size())) {
								if (WhereCondition.whereMethod(rightExp, tuple, tempTableListMap.get(temp).get(0),
										tempTableListMap.get(temp).get(1)))
									tableArrayList.add(tuple);
							}
							tempTableListMap.put(temp, tableArrayList);
						} else {
							// System.out.println("idahr aa");
							SQLParser table = null;
							table = tables.get(results.fromTables.get(temp));
							// System.out.println(table.tableFilePath);

							FileReader fr = new FileReader(table.tableFilePath);
							BufferedReader br = new BufferedReader(fr);
							String tuple = br.readLine();
							String firstRow = "";
							String secondRow = "";
							// if (tableNameSplit.length > 2) {
							table.aliasName = temp;
							for (String columnName : table.columnNames) {
								firstRow = firstRow + table.aliasName + "." + columnName + "|";
								secondRow = secondRow + table.columnDescriptionList
										.get(table.columnNames.indexOf(columnName)).getColDataType().getDataType()
										+ "|";
							}
							tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
							tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
							while (tuple != null) {
								// String values[] = tuple.split("\\|");
								if (WhereCondition.whereMethod(rightExp, tuple, firstRow, secondRow))
									tableArrayList.add(tuple);
								tuple = br.readLine();
							}
							tempTableListMap.put(temp, tableArrayList);
						}
					}
					rightExpression = leftExp;
				}
				ArrayList<String> tableArrayList = new ArrayList<String>();
				String temp = rightExpression.toString().split("\\.")[0].toLowerCase();
				if (tempTableListMap.containsKey(temp)) {
					tableArrayList.add(tempTableListMap.get(temp).get(0));
					tableArrayList.add(tempTableListMap.get(temp).get(1));
					for (String tuple : tempTableListMap.get(temp).subList(2, tempTableListMap.get(temp).size())) {
						if (WhereCondition.whereMethod(rightExpression, tuple, tempTableListMap.get(temp).get(0),
								tempTableListMap.get(temp).get(1)))
							tableArrayList.add(tuple);
					}
					tempTableListMap.put(temp, tableArrayList);
				} else {
					// System.out.println("idahr aa");
					SQLParser table = null;
					table = tables.get(results.fromTables.get(temp));
					// System.out.println(table.tableFilePath);
					FileReader fr = new FileReader(table.tableFilePath);
					BufferedReader br = new BufferedReader(fr);
					String tuple = br.readLine();
					String firstRow = "";
					String secondRow = "";
					// if (tableNameSplit.length > 2) {
					table.aliasName = temp;
					for (String columnName : table.columnNames) {
						firstRow = firstRow + table.aliasName + "." + columnName + "|";
						secondRow = secondRow + table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
								.getColDataType().getDataType() + "|";
					}
					tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
					tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
					while (tuple != null) {
						// String values[] = tuple.split("\\|");
						if (WhereCondition.whereMethod(rightExpression, tuple, firstRow, secondRow))
							tableArrayList.add(tuple);
						tuple = br.readLine();
					}
					tempTableListMap.put(temp, tableArrayList);

				}
				for (String key : tempTableListMap.keySet()) {
					if (tableListMap.containsKey(key)) {
						ArrayList<String> tempArrayList = tableListMap.get(key);
						tempArrayList.addAll(tempTableListMap.get(key).subList(2, tempTableListMap.get(key).size()));
						tableListMap.put(key, tempArrayList);
					} else {
						tableListMap.put(key, tempTableListMap.get(key));
					}
				}
				// tableListMap = (HashMap<String,
				// ArrayList<String>>) tempTableListMap.clone();
				tempTableListMap.clear();

			}

			if (leftExpression instanceof AndExpression) {

				AndExpression inAndExpression = (AndExpression) leftExpression;
				Expression leftExp = (inAndExpression.getLeftExpression());
				Expression rightExp = (inAndExpression.getRightExpression());

				String temp = rightExp.toString().split("\\.")[0].toLowerCase();
				ArrayList<String> tableArrayList = new ArrayList<String>();

				if (tableListMap.containsKey(temp)) {
					tableArrayList.add(tableListMap.get(temp).get(0));
					tableArrayList.add(tableListMap.get(temp).get(1));

					for (String tuple : tableListMap.get(temp).subList(2, tableListMap.get(temp).size())) {

						if (WhereCondition.whereMethod(rightExp, tuple, tableListMap.get(temp).get(0),
								tableListMap.get(temp).get(1)))
							tableArrayList.add(tuple);
					}
					// ArrayList<String> tempArrayList = tableListMap.get(temp);
					// tempArrayList.addAll(tableArrayList);
					tableListMap.put(temp, tableArrayList);
				}

			}
		}

		else {

			for (String tableName : tableNamesString) {
				int count = 0;
				String tableNameSplit[] = tableName.split(" ");
				// if (tableNameSplit.length > 1) {
				// results.fromTables.put(tableNameSplit[2].toLowerCase(),
				// tableNameSplit[0].toLowerCase());
				// } else {
				// results.fromTables.put(tableNameSplit[0].toLowerCase(),
				// tableNameSplit[0].toLowerCase());
				// }
				for (Expression e : nonJoinList) {

					String temp = e.toString().replaceAll("[^a-zA-Z0-9\\.]", " ");
					// System.out.println(temp);
					// System.out.println(tableName);
					if (temp.split(" ")[0].split("\\.").length > 1 && tableNameSplit.length > 1) {
						// System.out.println("idhar aaya abi");
						if (temp.split(" ")[0].split("\\.")[0].equalsIgnoreCase(tableNameSplit[2])) {
							// System.out.println("idhar bhi aaya abi");
							if (e instanceof InExpression) {
								String right = "";
								InExpression inExpression = (InExpression) e;
								Expression leftExpression = (inExpression.getLeftExpression());
								ItemsList rightExpression = inExpression.getItemsList();
								if (rightExpression.toString().contains("SELECT ")
										|| rightExpression.toString().contains("select ")
										|| rightExpression.toString().contains("Select ")) {
									Results subQueryIn = null;
									subQueryIn = SelectStatement.selectMethod(
											(PlainSelect) ((SubSelect) rightExpression).getSelectBody(), tables, true);
									results.subQueryAdd(subQueryIn);
									for (String s : subQueryIn.output.subList(2, subQueryIn.output.size())) {
										right = right + s + ",";
									}
									subQueryIn.output.clear();
									right = right.substring(0, right.length() - 1);
								} else {
									right = rightExpression.toString().substring(1,
											rightExpression.toString().length() - 1);
								}
								ArrayList<String> tableArrayList = new ArrayList<String>();
								if (tableListMap.containsKey(tableNameSplit[2])) {
									int index = 0;
									for (String str : tableListMap.get(tableNameSplit[2]).get(0).split("\\|")) {
										if (str.equalsIgnoreCase(leftExpression.toString()))
											break;
										index++;
									}
									String type = tableListMap.get(tableNameSplit[2]).get(1).split("\\|")[index];
									tableArrayList.add(tableListMap.get(tableNameSplit[2]).get(0));
									tableArrayList.add(tableListMap.get(tableNameSplit[2]).get(1));
									for (String tuple : tableListMap.get(tableNameSplit[2]).subList(2,
											tableListMap.get(tableNameSplit[2]).size())) {
										String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethodIn(right, values[index], type))
											tableArrayList.add(tuple);
										// tuple = br.readLine();
									}
									tableListMap.put(tableNameSplit[2], tableArrayList);
								} else {
									SQLParser table = null;
									table = tables.get(tableNameSplit[0]);
									// System.out.println(table.tableFilePath);
									FileReader fr = new FileReader(table.tableFilePath);
									BufferedReader br = new BufferedReader(fr);
									String tuple = br.readLine();
									String firstRow = "";
									String secondRow = "";
									// if (tableNameSplit.length > 2) {
									table.aliasName = tableNameSplit[2];
									for (String columnName : table.columnNames) {
										firstRow = firstRow + table.aliasName + "." + columnName + "|";
										secondRow = secondRow
												+ table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
														.getColDataType().getDataType()
												+ "|";
									}
									tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
									tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
									int index = 0;
									for (String str : firstRow.split("\\|")) {
										if (str.equalsIgnoreCase(leftExpression.toString()))
											break;
										index++;
									}
									String type = secondRow.split("\\|")[index];
									while (tuple != null) {
										String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethodIn(right, values[index], type))
											tableArrayList.add(tuple);
										tuple = br.readLine();
									}
								}
								tableListMap.put(tableNameSplit[2], tableArrayList);
							} else {
								ArrayList<String> tableArrayList = new ArrayList<String>();
								if (tableListMap.containsKey(tableNameSplit[2])) {
									tableArrayList.add(tableListMap.get(tableNameSplit[2]).get(0));
									tableArrayList.add(tableListMap.get(tableNameSplit[2]).get(1));
									for (String tuple : tableListMap.get(tableNameSplit[2]).subList(2,
											tableListMap.get(tableNameSplit[2]).size())) {
										if (WhereCondition.whereMethod(e, tuple,
												tableListMap.get(tableNameSplit[2]).get(0),
												tableListMap.get(tableNameSplit[2]).get(1)))
											tableArrayList.add(tuple);
									}
									tableListMap.put(tableNameSplit[2], tableArrayList);
								} else {
									// System.out.println("idahr aa");
									SQLParser table = null;
									table = tables.get(tableNameSplit[0]);
									// System.out.println(table.tableFilePath);
									FileReader fr = new FileReader(table.tableFilePath);
									BufferedReader br = new BufferedReader(fr);
									String tuple = br.readLine();
									String firstRow = "";
									String secondRow = "";
									// if (tableNameSplit.length > 2) {
									table.aliasName = tableNameSplit[2];
									for (String columnName : table.columnNames) {
										firstRow = firstRow + table.aliasName + "." + columnName + "|";
										secondRow = secondRow
												+ table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
														.getColDataType().getDataType()
												+ "|";
									}
									tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
									tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
									while (tuple != null) {
										// String values[] = tuple.split("\\|");
										if (WhereCondition.whereMethod(e, tuple, firstRow, secondRow))
											tableArrayList.add(tuple);
										tuple = br.readLine();
									}
									tableListMap.put(tableNameSplit[2], tableArrayList);
								}
							}
						} else {
							count++;
						}
					} else if (tableNameSplit[0].length() > 2) {
						if (e instanceof InExpression) {
							String right = "";
							InExpression inExpression = (InExpression) e;
							Expression leftExpression = (inExpression.getLeftExpression());
							ItemsList rightExpression = inExpression.getItemsList();
							if (rightExpression.toString().contains("SELECT ")
									|| rightExpression.toString().contains("select ")
									|| rightExpression.toString().contains("Select ")) {
								Results subQueryIn = null;
								subQueryIn = SelectStatement.selectMethod(
										(PlainSelect) ((SubSelect) rightExpression).getSelectBody(), tables, true);
								results.subQueryAdd(subQueryIn);
								for (String s : subQueryIn.output.subList(2, subQueryIn.output.size())) {
									right = right + s + ",";
								}
								subQueryIn.output.clear();
								right = right.substring(0, right.length() - 1);
							} else {
								right = rightExpression.toString().substring(1,
										rightExpression.toString().length() - 1);
							}
							ArrayList<String> tableArrayList = new ArrayList<String>();
							// if (tables.get(tableNameSplit[0]) != null) {
							if (tableListMap.containsKey(tableNameSplit[0])) {
								int index = 0;
								for (String str : tableListMap.get(tableNameSplit[0]).get(0).split("\\|")) {
									if (str.equalsIgnoreCase(leftExpression.toString()))
										break;
									index++;
								}
								String type = tableListMap.get(tableNameSplit[0]).get(1).split("\\|")[index];
								tableArrayList.add(tableListMap.get(tableNameSplit[0]).get(0));
								tableArrayList.add(tableListMap.get(tableNameSplit[0]).get(1));
								for (String tuple : tableListMap.get(tableNameSplit[0]).subList(2,
										tableListMap.get(tableNameSplit[0]).size())) {
									String values[] = tuple.split("\\|");
									if (WhereCondition.whereMethodIn(right, values[index], type))
										tableArrayList.add(tuple);
									// tuple = br.readLine();
								}
								tableListMap.put(tableNameSplit[0], tableArrayList);
							} else {
								SQLParser table = null;
								table = tables.get(tableNameSplit[0]);
								// System.out.println(table.tableFilePath);
								FileReader fr = new FileReader(table.tableFilePath);
								BufferedReader br = new BufferedReader(fr);
								String tuple = br.readLine();
								String firstRow = "";
								String secondRow = "";
								// if (tableNameSplit.length > 2) {
								// table.aliasName = tableNameSplit[2];
								for (String columnName : table.columnNames) {
									firstRow = firstRow + columnName + "|";
									secondRow = secondRow + table.columnDescriptionList
											.get(table.columnNames.indexOf(columnName)).getColDataType().getDataType()
											+ "|";
								}
								tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
								tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
								int index = 0;
								for (String str : firstRow.split("\\|")) {
									if (str.equalsIgnoreCase(leftExpression.toString()))
										break;
									index++;
								}
								String type = secondRow.split("\\|")[index];
								while (tuple != null) {
									String values[] = tuple.split("\\|");
									if (WhereCondition.whereMethodIn(right, values[index], type))
										tableArrayList.add(tuple);
									tuple = br.readLine();
								}
							}
							tableListMap.put(tableNameSplit[0], tableArrayList);
						}

						else {
							ArrayList<String> tableArrayList = new ArrayList<String>();
							if (tableListMap.containsKey(tableNameSplit[0])) {
								tableArrayList.add(tableListMap.get(tableNameSplit[0]).get(0));
								tableArrayList.add(tableListMap.get(tableNameSplit[0]).get(1));
								for (String tuple : tableListMap.get(tableNameSplit[0]).subList(2,
										tableListMap.get(tableNameSplit[0]).size())) {
									if (WhereCondition.whereMethod(e, tuple, tableListMap.get(tableNameSplit[0]).get(0),
											tableListMap.get(tableNameSplit[0]).get(1)))
										tableArrayList.add(tuple);
								}
								tableListMap.put(tableNameSplit[0], tableArrayList);
							} else {
								SQLParser table = null;
								table = tables.get(tableNameSplit[0]);
								// System.out.println(table.tableFilePath);
								FileReader fr = new FileReader(table.tableFilePath);
								BufferedReader br = new BufferedReader(fr);
								String tuple = br.readLine();
								String firstRow = "";
								String secondRow = "";
								// if (tableNameSplit.length > 2) {
								// table.aliasName = tableNameSplit[2];
								for (String columnName : table.columnNames) {
									firstRow = firstRow + columnName + "|";
									secondRow = secondRow + table.columnDescriptionList
											.get(table.columnNames.indexOf(columnName)).getColDataType().getDataType()
											+ "|";
								}
								tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
								tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
								while (tuple != null) {
									// String values[] = tuple.split("\\|");
									// System.out.println(tuple);
									if (WhereCondition.whereMethod(e, tuple, firstRow, secondRow))
										tableArrayList.add(tuple);
									tuple = br.readLine();
								}
								tableListMap.put(tableNameSplit[0], tableArrayList);
							}

						}

					} else {
						count++;
					}

				}
				if (count == nonJoinList.size() && !tableListMap.containsKey(tableNameSplit[0])) {
					ArrayList<String> tableArrayList = new ArrayList<String>();
					SQLParser table = null;
					table = tables.get(tableNameSplit[0]);
					// System.out.println(table.tableFilePath);
					FileReader fr = new FileReader(table.tableFilePath);
					BufferedReader br = new BufferedReader(fr);
					String tuple = br.readLine();
					String firstRow = "";
					String secondRow = "";
					// if (tableNameSplit.length > 2) {
					table.aliasName = tableNameSplit[2];
					for (String columnName : table.columnNames) {
						firstRow = firstRow + table.aliasName + "." + columnName + "|";
						secondRow = secondRow + table.columnDescriptionList.get(table.columnNames.indexOf(columnName))
								.getColDataType().getDataType() + "|";
					}
					tableArrayList.add(firstRow.substring(0, firstRow.length() - 1));
					tableArrayList.add(secondRow.substring(0, secondRow.length() - 1));
					while (tuple != null) {
						tableArrayList.add(tuple);
						tuple = br.readLine();
					}
					tableListMap.put(tableNameSplit[2], tableArrayList);
				}
			}
		}

		Map<String, Integer> joinCount = sortByValue(tempJoinCount, tableListMap);

		List<Expression> joinList = new ArrayList<Expression>();
		// joinList.add(0, tempJoinList.get(1));
		// joinList.add(1, tempJoinList.get(tempJoinList.size()-1));
		if (tempJoinList.size() >= 2) {
			for (int i = 0; i < tempJoinList.size(); i++) {
				if (i == 1) {
					joinList.add(0, tempJoinList.get(i));
				} else if (i == tempJoinList.size() - 1) {
					joinList.add(1, tempJoinList.get(i));
				} else
					joinList.add(i, tempJoinList.get(i));
			}

			// tempJoinList.clear();
		} else {
			joinList.addAll(tempJoinList);
		}
		if (!joinList.isEmpty())
			for (Expression s : joinList) {
				String temp[] = s.toString().split("=");
				if (results.fromTables.get(temp[0].split("\\.")[0].trim().toLowerCase()) != null)
					results.joins.add(results.fromTables.get(temp[0].split("\\.")[0].trim().toLowerCase()) + "."
							+ temp[0].split("\\.")[1].trim());
				else {
					for (String sQ : results.subQueryProjections) {
						if (sQ.contains(temp[0].split("\\.")[1].trim())) {
							results.joins.add(sQ);
							break;
						}
					}
				}
				if (results.fromTables.get(temp[1].split("\\.")[0].trim().toLowerCase()) != null)
					results.joins.add(results.fromTables.get(temp[1].split("\\.")[0].trim().toLowerCase()) + "."
							+ temp[1].split("\\.")[1].trim());
				else
					for (String sQ : results.subQueryProjections) {
						if (sQ.contains(temp[1].split("\\.")[1])) {
							results.joins.add(sQ);
							break;
						}
					}
			}

		int count = 0;
		ArrayList<String> joinedTables = new ArrayList<String>();
		ArrayList<String> intermediateList1 = new ArrayList<String>();

		if (tableNamesString.size() == joinList.size()) {

			EqualsTo EqualsToExpression = (EqualsTo) joinList.get(0);
			Expression leftEqualsTo = (EqualsToExpression.getLeftExpression());
			Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
			String rightEqualsToArray[] = rightEqualsTo.toString().split("\\.");
			String leftEqualsToArray[] = leftEqualsTo.toString().split("\\.");
			intermediateList = WhereCondition.whereANDJoinMethod(joinList,
					tableListMap.get(leftEqualsToArray[0].toLowerCase()),
					tableListMap.get(rightEqualsToArray[0].toLowerCase()));

			// }
		}

		else { //// condition
			for (String str : joinCount.keySet()) {
				for (int i = 0; i < joinList.size(); i++) {
					EqualsTo EqualsToExpression = (EqualsTo) joinList.get(i);
					Expression leftEqualsTo = (EqualsToExpression.getLeftExpression());
					Expression rightEqualsTo = (EqualsToExpression.getRightExpression());
					String rightEqualsToArray[] = rightEqualsTo.toString().split("\\.");

					String leftEqualsToArray[] = leftEqualsTo.toString().split("\\.");
					if (rightEqualsToArray[0].equalsIgnoreCase(str) || leftEqualsToArray[0].equalsIgnoreCase(str)) {
						count++;

						if (count < 2) {

							joinedTables.add(rightEqualsToArray[0].toLowerCase());
							joinedTables.add(leftEqualsToArray[0].toLowerCase());
							intermediateList = WhereCondition.whereJoinMethod(joinList.get(i),
									tableListMap.get(leftEqualsToArray[0].toLowerCase()),
									tableListMap.get(rightEqualsToArray[0].toLowerCase()), joinCount);
						} else {
							if (!(joinedTables.contains(leftEqualsToArray[0].toLowerCase()))
									&& (joinedTables.contains(rightEqualsToArray[0].toLowerCase()))) {
								// System.out.println("right there left not
								// there: " + joinList.get(i).toString());
								intermediateList = WhereCondition.whereJoinMethod(joinList.get(i),
										tableListMap.get(leftEqualsToArray[0].toLowerCase()), intermediateList,
										joinCount);
								joinedTables.add(leftEqualsToArray[0].toLowerCase());
							} else if (!(joinedTables.contains(rightEqualsToArray[0].toLowerCase()))
									&& (joinedTables.contains(leftEqualsToArray[0].toLowerCase()))) {

								intermediateList = WhereCondition.whereJoinMethod(joinList.get(i), intermediateList,
										tableListMap.get(rightEqualsToArray[0].toLowerCase()), joinCount);
								joinedTables.add(rightEqualsToArray[0].toLowerCase());
							} else if (!(joinedTables.contains(rightEqualsToArray[0].toLowerCase()))
									&& !(joinedTables.contains(leftEqualsToArray[0].toLowerCase()))) {
								intermediateList1 = WhereCondition.whereJoinMethod(joinList.get(i),
										tableListMap.get(leftEqualsToArray[0].toLowerCase()),
										tableListMap.get(rightEqualsToArray[0].toLowerCase()), joinCount);
								joinedTables.add(rightEqualsToArray[0].toLowerCase());
								joinedTables.add(leftEqualsToArray[0].toLowerCase());
							} else if ((joinedTables.contains(rightEqualsToArray[0].toLowerCase()))
									&& (joinedTables.contains(leftEqualsToArray[0].toLowerCase()))) {

								if (intermediateList.get(0).contains(rightEqualsToArray[0])) {

									intermediateList = WhereCondition.whereJoinMethod(joinList.get(i),
											intermediateList1, intermediateList, joinCount);

								} else if (intermediateList.get(0).contains(leftEqualsToArray[0])) {

									intermediateList = WhereCondition.whereJoinMethod(joinList.get(i), intermediateList,
											intermediateList1, joinCount);

								}
							}
						}
						joinList.remove(i);
					}
				}
			}
		}
		if (tempJoinList.size() < tableListMap.size() - 1) {
			if (joinedTables.size() == tableListMap.size()) {
				intermediateList = WhereCondition.whereOuterJoin(intermediateList, intermediateList1);
			} else {
				for (String s : tableListMap.keySet()) {
					if (!joinedTables.contains(s)) {
						intermediateList = WhereCondition.whereOuterJoin(intermediateList, tableListMap.get(s));
						break;
					}
				}
			}
		}

		if (intermediateList.size() == 0) {
			for (String str : tableListMap.get(tableListMap.keySet().iterator().next())) {
				intermediateList.add(str);
			}
		}

		Expression havingExp = selectBody.getHaving();
		List<Column> groupByItems = selectBody.getGroupByColumnReferences();
		List<SelectItem> selectItems = selectBody.getSelectItems();

		for (SelectItem selectItem : selectItems) {
			if ((selectItem.toString().contains("*") || selectItem.toString().contains("+")
					|| selectItem.toString().contains("-") || selectItem.toString().contains("/"))
					&& !selectItem.toString().equalsIgnoreCase("count(*)") && !selectItem.toString().equals("*")
					&& selectItem.toString().contains("(")) {
				String insideselectItem = selectItem.toString().substring(selectItem.toString().indexOf("(") + 1,
						selectItem.toString().lastIndexOf(")"));
				String expression = "";

				if (selectItem.toString().contains(" AS "))
					expression = selectItem.toString().split(" as ")[0];
				else if (selectItem.toString().contains(" = ")) {
					expression = selectItem.toString().split(" = ")[1];
				}
				if (expression.toString().contains("sum(") || expression.toString().contains("SUM(")
						|| expression.toString().contains("Sum(") || expression.toString().contains("avg(")
						|| expression.toString().contains("AVG(") || expression.toString().contains("Avg(")
						|| expression.toString().contains("count(") || expression.toString().contains("COUNT(")
						|| expression.toString().contains("Count(") || expression.toString().contains("min(")
						|| expression.toString().contains("Min(") || expression.toString().contains("MIN(")
						|| expression.toString().contains("max(") || expression.toString().contains("Max(")
						|| expression.toString().contains("MAX(")) {
					// expression = expression.substring(expression.indexOf("(")
					// + 1,
					// expression.lastIndexOf(")"));
					expression = insideselectItem;

				}

				// while (expression.contains("*") || expression.contains("+")
				// ||expression.contains("/") || expression.contains("-")) {
				// System.out.println(expression +" inside - ");

				String innerExpression[] = expression.split("\\*");

				// System.out.println(expression+" inside - ");

				if (expression.contains("-")) {

					String expressionArray[] = innerExpression[1].split("-");

					String header[] = intermediateList.get(0).split("\\|");
					int index = 0;
					for (String s : header) {
						if (s.split("\\.")[1].equalsIgnoreCase(expressionArray[1].trim())) {
							break;
						}
						index++;
					}

					intermediateList.set(0, intermediateList.get(0) + "|" + selectItem.toString().split(" as ")[0]);
					intermediateList.set(1, intermediateList.get(1) + "|" + "Double");
					for (int i = 2; i < intermediateList.size(); i++) {
						String[] values = intermediateList.get(i).split("\\|");
						String tmp = intermediateList.get(i) + "|"
								+ (Double.parseDouble(expressionArray[0]) - Double.parseDouble(values[index]));
						intermediateList.set(i, tmp);
					}
					// System.out.println("inetermediate list after
					// -:"+intermediateList);
				}
				// - done

				String newexpression = innerExpression[0].trim();

				// if(expression.contains("*")){
				// String expressionArray[] = expression.split("-");
				String header[] = intermediateList.get(0).split("\\|");
				int index1 = 0;
				// System.out.println(header);
				for (String s : header) {

					if (s.split("\\.")[1].equalsIgnoreCase(newexpression.trim())) {
						break;
					}
					index1++;
				}
				int index2 = header.length - 1;
				intermediateList.set(0, intermediateList.get(0) + "|" + insideselectItem);
				intermediateList.set(1, intermediateList.get(1) + "|" + "Double");
				for (int i = 2; i < intermediateList.size(); i++) {
					String[] values = intermediateList.get(i).split("\\|");
					String tmp = intermediateList.get(i) + "|"
							+ (Double.parseDouble(values[index1]) * Double.parseDouble(values[index2]));
					intermediateList.set(i, tmp);
				}
				// }
				// System.out.println("inetermediate list after
				// *:"+intermediateList);
				// expression = expression.substring(0, expression.indexOf("(")
				// );
			}

		}
		results.output.add(intermediateList.get(0));
		results.output.add(intermediateList.get(1));
		for (SelectItem selectItem : selectItems) {
			if (selectItem.toString().contains("sum(") || selectItem.toString().contains("SUM(")
					|| selectItem.toString().contains("Sum(") || selectItem.toString().contains("avg(")
					|| selectItem.toString().contains("AVG(") || selectItem.toString().contains("Avg(")
					|| selectItem.toString().contains("count(") || selectItem.toString().contains("COUNT(")
					|| selectItem.toString().contains("Count(") || selectItem.toString().contains("min(")
					|| selectItem.toString().contains("Min(") || selectItem.toString().contains("MIN(")
					|| selectItem.toString().contains("max(") || selectItem.toString().contains("Max(")
					|| selectItem.toString().contains("MAX(")) {
				String aggregatefunc = selectItem.toString().substring(0, selectItem.toString().indexOf("("));
				String colName = selectItem.toString().substring(selectItem.toString().indexOf("(") + 1,
						selectItem.toString().indexOf(")"));
				String type;
				int index = 0;
				if (!(aggregatefunc.equalsIgnoreCase("Sum")) && !(aggregatefunc.equalsIgnoreCase("count"))
						&& !(aggregatefunc.equalsIgnoreCase("Avg"))) {
					for (String str : intermediateList.get(0).split("\\|")) {
						if (colName.equalsIgnoreCase(str))
							break;
						index++;
					}
					type = intermediateList.get(1).split("\\|")[index];
				} else if (aggregatefunc.equalsIgnoreCase("Avg")) {
					type = "DOUBLE";
				} else {
					// System.out.println("here");
					type = "INTEGER";
				}
				aggregate = true;

				// for (String s : results.output) {
				// System.out.println("s: " + s);
				// }
				if (groupByItems == null) {
					if (selectItem.toString().contains(" as ") || selectItem.toString().contains(" AS ")
							|| selectItem.toString().contains(" As ")) {
						// System.out.println("as select");
						results.output.set(0, results.output.get(0) + "|" + selectItem.toString().split(" AS ")[1]);
					} else {
						results.output.set(0, results.output.get(0) + "|" + selectItem.toString());
					}
					results.output.set(1, results.output.get(1) + "|" + type);
					if (results.output.size() > 2) {
						results.output.set(2, results.output.get(2) + "|"
								+ Aggregate.aggregateNoGroupBy(intermediateList, selectItem.toString()));
					} else {
						results.output.add(Aggregate.aggregateNoGroupBy(intermediateList, selectItem.toString()));
					}
					// }
					// for (String s : results.output) {
					// System.out.println("soutput: " + s);
					// }
				} else if (groupByItems != null) {
					groupBy = true;
					if (selectItem.toString().contains(" as ") || selectItem.toString().contains(" AS ")
							|| selectItem.toString().contains(" As ")) {
						// System.out.println("as select");
						results.output.set(0, results.output.get(0) + "|" + selectItem.toString().split(" AS ")[1]);
					} else {
						results.output.set(0, results.output.get(0) + "|" + selectItem.toString());
					}
					results.output.set(1, results.output.get(1) + "|" + type);
					HashMap<String, ArrayList<String>> groupbyMap = new HashMap<String, ArrayList<String>>(
							Aggregate.aggregateGroupBy(intermediateList, selectItem.toString(), groupByItems));
					if (havingExp != null) {
						if (havingExp.toString().contains(aggregatefunc)) {
							groupbyMap = Having.havingFilter(groupbyMap, havingExp);
							for (String s : groupbyMap.keySet()) {
								results.output.add(groupbyMap.get(s).get(1) + "|" + groupbyMap.get(s).get(0));
							}
						} else {
							String havingItem = havingExp.toString().substring(0,
									havingExp.toString().indexOf(")") + 1);
							HashMap<String, ArrayList<String>> havingMap = Aggregate.aggregateGroupBy(intermediateList,
									havingItem, groupByItems);
							HashMap<String, ArrayList<String>> having = Having.havingFilter(havingMap, havingExp);
							for (String s : groupbyMap.keySet()) {
								if (!having.containsKey(s)) {
									groupbyMap.remove(s);
								}
							}
							for (String s : groupbyMap.keySet()) {
								results.output.add(groupbyMap.get(s).get(1)); // ?check
							}
						}
					} else {
						for (String s : groupbyMap.keySet()) {
							results.output.add(groupbyMap.get(s).get(1) + "|" + groupbyMap.get(s).get(0));
						}
					}
				}
			}
		}

		if (!aggregate) {
			if (groupByItems != null) {
				groupBy = true;
				if (havingExp != null) {
					String havingItem = havingExp.toString().substring(0, havingExp.toString().indexOf(")") + 1);
					HashMap<String, ArrayList<String>> havingMap = Aggregate.aggregateGroupBy(intermediateList,
							havingItem, groupByItems);
					HashMap<String, ArrayList<String>> having = Having.havingFilter(havingMap, havingExp);
					for (String s : having.keySet()) {
						results.output.add(having.get(s).get(1));
					}
				} else if (havingExp == null) {
					HashMap<String, String> Map = new HashMap<String, String>(
							Aggregate.GroupBy(intermediateList, groupByItems));
					for (String s : Map.keySet()) {
						results.output.add(Map.get(s));
					}
				}

			}

		}

		if (!aggregate && !groupBy) {
			results.output = intermediateList;
		}

		List<OrderByElement> orderBy = selectBody.getOrderByElements();
		if (orderBy != null) {
			results.output = OrderBy.orderByMethod(results.output, orderBy);
		}

		Limit limitBy = selectBody.getLimit();
		if (limitBy != null) {
			results.output.removeAll(results.output
					.subList(Integer.parseInt(limitBy.toString().trim().split(" ")[1]) + 2, results.output.size()));
		}

		results.output = ProjectItems.projections(selectItems, results.output);
		Distinct distinctItems = selectBody.getDistinct();
		if (distinctItems != null) {

			results.output = (ArrayList<String>) results.output.stream().distinct().collect(Collectors.toList());
		}

		for (String s : results.output.get(0).split("\\|")) {
			if (s.split("\\.").length > 1) {
				if (results.fromTables.get(s.split("\\.")[0]) != null)
					results.projection.add(results.fromTables.get(s.split("\\.")[0]) + "." + s.split("\\.")[1]);
				else {
					for (String sQ : results.subQueryProjections) {
						if (sQ.contains(s.split("\\.")[1])) {
							results.projection.add(sQ);
							break;
						}
					}

				}
			} else if (tableNamesString.size() == 1) {
				boolean flag = false;
				for (String colName : tableListMap.get(tableListMap.keySet().iterator().next()).get(0).split("\\|")) {
					if (colName.contains(s)) {
						results.projection
								.add(results.fromTables.get(results.fromTables.keySet().iterator().next()) + "." + s);
						flag = true;
						break;
					}
				}
				if (!flag) {
					results.projection.add(s);
				}

			} else {
				results.projection.add(s);
			}
		}

		if (!nonJoinList.isEmpty()) {
			for (Expression s : nonJoinList) {
				String temp[] = s.toString().replaceAll("[^a-zA-Z0-9\\.]", " ").split(" ");
				if (temp[0].split("\\.").length > 1) {
					if (results.fromTables.get(temp[0].split("\\.")[0].toLowerCase()) != null)
						results.selection.add(results.fromTables.get(temp[0].split("\\.")[0].toLowerCase()) + "."
								+ temp[0].split("\\.")[1]);
					else {
						for (String sQ : results.subQueryProjections) {
							if (sQ.contains(temp[0].split("\\.")[1])) {
								results.selection.add(sQ);
								break;
							}
						}

					}
				} else if (tableNamesString.size() == 1) {
					results.selection
							.add(results.fromTables.get(results.fromTables.keySet().iterator().next()) + "." + temp[0]);
				} else {
					results.selection.add(s.toString());
				}

			}
		}

		if (groupByItems != null) {
			for (Column s : groupByItems) {
				if (s.getWholeColumnName().split("\\.").length > 1) {
					if (results.fromTables.get(s.getWholeColumnName().split("\\.")[0].toLowerCase()) != null)
						results.groupBy.add(results.fromTables.get(s.getWholeColumnName().split("\\.")[0].toLowerCase())
								+ "." + s.getColumnName());
					else
						for (String sQ : results.subQueryProjections) {
							if (sQ.contains(s.getColumnName())) {
								results.groupBy.add(sQ);
								break;
							}
						}
				} else if (tableNamesString.size() == 1) {
					results.groupBy.add(results.fromTables.get(results.fromTables.keySet().iterator().next()) + "."
							+ s.getColumnName());
				} else {
					// System.out.println(s.toString());
				}
			}
		}

		int orderByCount = 0;
		if (orderBy != null)
			for (OrderByElement s : orderBy) {
				if (s.toString().split("\\.").length > 1) {
					if (results.fromTables.get(s.toString().split(" ")[0].split("\\.")[0].toLowerCase()) != null)
						if (s.toString().split(" ")[0].length() > 1)
							results.orderBy.add(
									results.fromTables.get(s.toString().split(" ")[0].split("\\.")[0].toLowerCase())
											+ "." + s.toString().split(" ")[0].split("\\.")[1]);
						else
							for (String sQ : results.subQueryProjections) {
								if (sQ.contains(s.toString().split(" ")[0].split("\\.")[1].toLowerCase())) {
									results.orderBy.add(sQ);
									break;
								}
							}
				} else if (tableNamesString.size() == 1) {
					boolean flag = false;
					for (String colName : tableListMap.get(tableListMap.keySet().iterator().next()).get(0)
							.split("\\|")) {
						if (colName.contains(s.toString())) {
							results.orderBy.add(results.fromTables.get(results.fromTables.keySet().iterator().next())
									+ "." + s.toString().split(" ")[0]);
							flag = true;
							break;
						}
					}
					if (!flag) {
						results.orderBy.add(s.toString().split(" ")[0]);
					}
				} else {
					results.orderBy.add(s.toString().split(" ")[0]);
				}
				if (s.toString().split(" ").length > 1) {
					results.orderBy.set(orderByCount,
							results.orderBy.get(orderByCount) + " " + s.toString().split(" ")[1]);
				}
				orderByCount++;
			}

		return results;
	}

	private static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap,
			HashMap<String, ArrayList<String>> tableListMap) {
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				if (o1.getValue().compareTo(o2.getValue()) > 0)
					return 1;
				else if (o1.getValue().compareTo(o2.getValue()) < 0)
					return -1;
				else {
					// System.out.println(tableListMap.get(o1.getKey().toLowerCase()).size());
					if (tableListMap.get(o1.getKey().toLowerCase()).size() > tableListMap.get(o2.getKey().toLowerCase())
							.size()) {
						return -1;
					} else {
						return 1;
					}
				}
				// return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
