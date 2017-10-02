
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class SQLParser {
	String tableName;
	File tableFilePath;
	ArrayList<ColumnDefinition> columnDescriptionList;
	ArrayList<String> columnNames = new ArrayList<String>();
	String aliasName;

	public SQLParser(String Name, int numColumns, File filePath, File dataDirectoryPath) throws FileNotFoundException {
		tableName = Name;
		tableFilePath = filePath;
		this.columnDescriptionList = new ArrayList<ColumnDefinition>();
	}

	public static void main(String args[]) throws IOException, ParseException, SQLException {
		long startTime = System.currentTimeMillis();
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, File> tableFileMap = new HashMap<String, File>();
		HashMap<String, SQLParser> tables = new HashMap<String, SQLParser>();
		File dataDir = new File("A:\\Semester 3\\DB\\FinalData\\"); // A:\\Semester
		// 3\\DB\\FinalData\\
		sqlFiles.add(new File("A:\\Semester 3\\DB\\FinalData\\schema.sql"));
		for (File tableFile : dataDir.listFiles()) {
			String fileName = tableFile.getName();

			if (fileName.endsWith(".csv")) {
				if (!tableFileMap.containsKey(fileName.substring(0, fileName.lastIndexOf(".")))) {
					tableFileMap.put(fileName.substring(0, fileName.lastIndexOf(".")), tableFile);
				}
			}
		}
		for (File sql : sqlFiles) {
			FileReader stream = new FileReader(sql);
			CCJSqlParser parser = new CCJSqlParser(stream);
			Statement stmt;
			int queryCount = 0;
			while ((stmt = parser.Statement()) != null) {
				if (stmt instanceof CreateTable) {
					CreateTable ct = (CreateTable) stmt;
					String tableName = ct.getTable().getName();

					SQLParser resultTable = new SQLParser(tableName.toLowerCase(), ct.getColumnDefinitions().size(),
							tableFileMap.get(tableName.toLowerCase()), dataDir);
					resultTable.columnDescriptionList = (ArrayList<ColumnDefinition>) ct.getColumnDefinitions();
					for (ColumnDefinition cd : resultTable.columnDescriptionList) {
						resultTable.columnNames.add(cd.getColumnName());
					}
					tables.put(tableName.toLowerCase(), resultTable);
				} else if (stmt instanceof Select) {
					queryCount++;
					Results results = new Results();
					if (stmt.toString().contains("UNION") || stmt.toString().contains("union")
							|| stmt.toString().contains("Union")) {
						SelectBody selectBody = ((Select) stmt).getSelectBody();
						List<PlainSelect> unionBody = ((Union) selectBody).getPlainSelects();
						int count = 0;
						System.out.println("\nQUERY: " + queryCount);
						for (PlainSelect plainSelect : unionBody) {
							if (count == 0) {
								results = SelectStatement.selectMethod(plainSelect, tables, false);
							} else {
								// System.out.println("Union Query: " +
								// String.valueOf(count + 1));
								Results results2 = SelectStatement.selectMethod(plainSelect, tables, false);
								results.output.addAll(results2.output.subList(2, results2.output.size()));
								results.subQueryAdd(results2);
							}
							count++;
						}
						results.output = (ArrayList<String>) results.output.stream().distinct()
								.collect(Collectors.toList());
					} else {
						System.out.println("\nQUERY: " + queryCount);
						SelectBody selectBody = ((Select) stmt).getSelectBody();
						results = SelectStatement.selectMethod((PlainSelect) selectBody, tables, false);
					}
					System.out.print("PROJECTION: ");
					for (String s : results.projection) {
						System.out.print(s + ", ");
					}
					System.out.println();
					System.out.print("FROM: ");
					for (String s : results.fromTables.values()) {
						System.out.print(s + ", ");
					}
					System.out.println();
					System.out.print("SELECTION: ");
					if (results.selection.size() == 0) {
						System.out.print("NULL");
					} else
						for (String s : results.selection) {
							System.out.print(s + ", ");
						}
					System.out.println();
					System.out.print("JOIN: ");
					if (results.joins.size() == 0) {
						System.out.print("NULL");
					} else
						for (String s : results.joins) {
							System.out.print(s + ", ");
						}
					System.out.println();
					System.out.print("GROUP-BY: ");
					if (results.groupBy.size() == 0) {
						System.out.print("NULL");
					} else
						for (String s : results.groupBy) {
							System.out.print(s + ", ");
						}
					System.out.println();
					System.out.print("ORDER-BY: ");
					if (results.orderBy.size() == 0) {
						System.out.print("NULL");
					} else
						for (String s : results.orderBy) {
							System.out.print(s + ", ");
						}
					System.out.println("\n");
					System.out.println("OUTPUT: ");
					for (String s : results.output) {
						System.out.println(s);
					}
				} else {
					System.out.println("Cannot Parse such statement!!");
				}

			}
			System.out.println("Total time taken: " + String.valueOf(startTime - System.currentTimeMillis()));
		}

	}
}
