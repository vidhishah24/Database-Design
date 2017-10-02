import java.util.*;

public class Results {

	ArrayList<String> output = new ArrayList<String>();
	HashMap<String, String> fromTables = new HashMap<String, String>();
	Set<String> projection = new HashSet<String>();
	Set<String> selection = new HashSet<String>();
	Set<String> joins = new HashSet<String>();
	Set<String> groupBy = new HashSet<String>();
	ArrayList<String> orderBy = new ArrayList<String>();
	String subQueryAlias;
	Set<String> subQueryProjections = new HashSet<String>();

	public void subQueryAdd(Results subQuery) {
		subQueryProjections.addAll(new HashSet<String>(subQuery.projection));
		for (String s : subQuery.fromTables.keySet()) {
			fromTables.put(s, subQuery.fromTables.get(s));
		}
		orderBy.addAll(new HashSet<String>(subQuery.orderBy));
		joins.addAll(new HashSet<String>(subQuery.joins));
		groupBy.addAll(new HashSet<String>(subQuery.groupBy));
		selection.addAll(new HashSet<String>(subQuery.selection));
		subQuery.projection.clear();
		subQuery.orderBy.clear();
		subQuery.joins.clear();
		subQuery.selection.clear();
		subQuery.groupBy.clear();

	}

}
