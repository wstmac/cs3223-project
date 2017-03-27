package qp.optimizer;

import java.io.*;
import java.util.*;

import qp.operators.*;
import qp.utils.*;

public class DynamicOptimizer {
	static SQLQuery sqlquery;
	int numJoin;
	int numTable;
	/**
	 * vector at position i means all subset s such that |s|=i+1
	 * Set<String> stores the tables name of the corresponding subset plan
	 * Operator is the root of the corresponding subset plan 
	 */
	Vector<Map<Set<String>, Operator>> planSpace;

	public DynamicOptimizer(SQLQuery sqlquery) {
		DynamicOptimizer.sqlquery = sqlquery;
		this.numTable = DynamicOptimizer.sqlquery.getFromList().size();
		planSpace = new Vector<Map<Set<String>, Operator>>(this.numTable);
		for (int i = 0; i < this.numTable; i++) {
			Map<Set<String>, Operator> map = new HashMap<Set<String>, Operator>();
			planSpace.add(map);
		}
	}

	public Operator getOptimalPlan() {
		Operator root = null;
	
		// find the plan for subset where |s|=1
		for (int i = 0; i < numTable; i++) {
			Set<String> tableName = new HashSet<String>();
			tableName.add((String) DynamicOptimizer.sqlquery.getFromList().elementAt(i));
			root = createInitialOperator((String) DynamicOptimizer.sqlquery.getFromList().elementAt(i));
			planSpace.elementAt(0).put(tableName, root);
		}
		
		// for |s|=2 to N, find the optimal plan
		for (int i = 1; i < numTable; i++) {
			//get all subset s where |s|=i+1
			Map<Integer, Set<String>> allSubset = getAllSubsets(i + 1);
			//for current i, get the best plan for the corresponding subset s
			for (int j = 0; j < allSubset.size(); j++) {
				Set<String> curSubset = allSubset.get(j);
				String[] curSubsetArray = curSubset.toArray(new String[0]);
				Operator curRoot = getBestPlan(curSubsetArray);
				Set<String> tableNames = new HashSet<String>();
				for (int k = 0; k <= i; k++) {
					tableNames.add(curSubsetArray[k]);
				}
				planSpace.elementAt(i).put(tableNames, curRoot);
				if(i==numTable-1){
					return curRoot;
				}
			}
		}
		return root;
	}
	
	
	/** AFter finding a choice of method for each operator
	prepare an execution plan by replacing the methods with
	corresponding join operator implementation
		**/
	
	public static Operator makeExecPlan(Operator node) {
	
		if (node.getOpType() == OpType.JOIN) {
			Operator left = makeExecPlan(((Join) node).getLeft());
			Operator right = makeExecPlan(((Join) node).getRight());
			int joinType = ((Join) node).getJoinType();
			int numbuff = BufferManager.getBuffersPerJoin();
			switch (joinType) {
			case JoinType.NESTEDJOIN:
	
				NestedJoin nj = new NestedJoin((Join) node);
				nj.setLeft(left);
				nj.setRight(right);
				nj.setNumBuff(numbuff);
				return nj;
	
			case JoinType.BLOCKNESTED:
	
				// NestedJoin bj2 = new NestedJoin((Join) node);
				BlockNested bj = new BlockNested((Join) node);
				bj.setLeft(left);
				bj.setRight(right);
				bj.setNumBuff(numbuff);
				/* + other code */
				return bj;
	
			case JoinType.SORTMERGE:
	
				NestedJoin sm = new NestedJoin((Join) node);
				/* + other code */
				return sm;
	
			case JoinType.HASHJOIN:
	
				NestedJoin hj = new NestedJoin((Join) node);
				/* + other code */
				return hj;
			default:
				return node;
			}
		} else if (node.getOpType() == OpType.SELECT) {
			Operator base = makeExecPlan(((Select) node).getBase());
			((Select) node).setBase(base);
			return node;
		} else if (node.getOpType() == OpType.PROJECT) {
			Operator base = makeExecPlan(((Project) node).getBase());
			((Project) node).setBase(base);
			return node;
		} else {
			return node;
		}
	}
	
	//create scan and select operator for subset s where |s|=1
	public Operator createInitialOperator(String tabName) {
		// create scan operator
		Scan tempop = null;
		Operator root = null;
		Scan op1 = new Scan(tabName, OpType.SCAN);
		tempop = op1;
		String filename = tabName + ".md";
		try {
			ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
			Schema schm = (Schema) _if.readObject();
			op1.setSchema(schm);
			_if.close();
		} catch (Exception e) {
			System.err.println("createInitialOperator(): Error in reading Schema of table " + filename);
			System.exit(1);
		}
		root = op1;
	
		// create select operator
		Select op2 = null;
		for (int j = 0; j < DynamicOptimizer.sqlquery.getSelectionList().size(); j++) {
			Condition cn = (Condition) DynamicOptimizer.sqlquery.getSelectionList().elementAt(j);
			if (cn.getOpType() == Condition.SELECT) {
				if (tabName == cn.getLhs().getTabName()) {
					op2 = new Select(op1, cn, OpType.SELECT);
					op2.setSchema(tempop.getSchema());
					root = op2;
				}
			}
		}
		return root;
	}
	
	// / find the best plan for one subset
	public Operator getBestPlan(String[] subset) {
		int size = subset.length;
		int bestPlanCost = Integer.MAX_VALUE;
		Operator curRoot = null;
		
		//for each table Ti in subset S, compute the cost of Ti join with S-Ti; get the smallest cost
		for (int i = 0; i < size; i++) {

			String tableName = subset[i];
			Set<String> oneTable = new HashSet<String>();
			oneTable.add(tableName);
	
			Set<String> restTables = new HashSet<String>();
			Condition condition = null;
			for (int j = 0; j < size; j++) {
				if (j != i)
					restTables.add(subset[j]);
			}
			Operator preRoot = planSpace.elementAt(size - 2).get(restTables);
			if (preRoot == null) {
				continue;
			}

			condition = getJoinCondition(preRoot, oneTable, size-1);
			int plancost;
			PlanCost pc = new PlanCost();
			if (condition != null) {
				Join newJoin = new Join(preRoot, planSpace.elementAt(0).get(oneTable), condition, OpType.JOIN);
				Schema newSchema = preRoot.getSchema().joinWith(planSpace.elementAt(0).get(oneTable).getSchema());
				newJoin.setSchema(newSchema);

	
				int curCost = 0;
				plancost = Integer.MAX_VALUE;
				int type=1;
	
				newJoin.setJoinType(JoinType.BLOCKNESTED);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.BLOCKNESTED;
				}
	
				newJoin.setJoinType(JoinType.NESTEDJOIN);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.NESTEDJOIN;
				}
	
				newJoin.setJoinType(JoinType.HASHJOIN);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.HASHJOIN;
				}
	
//				newJoin.setJoinType(JoinType.SORTMERGE);
//				pc = new PlanCost();
//				curCost = pc.getCost(newJoin);
//				if (curCost < plancost) {
//					plancost = curCost;
//					type = JoinType.SORTMERGE;
//				}
	
				newJoin.setJoinType(type);
	
				if (plancost < bestPlanCost) {
					bestPlanCost = plancost;
					curRoot = newJoin;
				}
			}
		}
		System.out.print("subset size " + size + "----for subset[ ");
		for (int t=0; t<subset.length; t++){
			System.out.print(subset[t] + " ");
		}
		System.out.println("]");
		return curRoot;
	}
	
	// / return left deep tree
	public Condition getJoinCondition(Operator lastroot, Set<String> singleTableSet, int level) {
		for (int i = 0; i < DynamicOptimizer.sqlquery.getJoinList().size(); i++) {
			Condition con = (Condition) DynamicOptimizer.sqlquery.getJoinList().elementAt(i);
			Attribute leftjoinAttr = con.getLhs();
			Attribute rightjoinAttr = (Attribute) con.getRhs();
			if ((lastroot.getSchema().contains(leftjoinAttr)
					&& (planSpace.elementAt(0).get(singleTableSet).getSchema().contains(rightjoinAttr)))) {
				return (Condition) con.clone();
			} else if (lastroot.getSchema().contains(rightjoinAttr)
					&& planSpace.elementAt(0).get(singleTableSet).getSchema().contains(leftjoinAttr)) {
				Condition newCon = (Condition) con.clone();
				newCon.setRhs((Attribute) leftjoinAttr.clone());
				newCon.setLhs((Attribute) rightjoinAttr.clone());
				return newCon;
			}
		}
		return null;
	}
	
	// / find all subsets with the specified subset size
	public Map<Integer, Set<String>> getAllSubsets(int cardinality) {
		Map<Integer, Set<String>> map = new HashMap<Integer, Set<String>>();
		int numOfTable = sqlquery.getFromList().size();
		int i, j, numOfSubset = (int) Math.pow(2, numOfTable);
		int key = 0;
		for (i = 0; i < numOfSubset; i++) {
			if (cardinality == countNumOfOnes(i)) {
				Set<String> set = new HashSet<String>();
				for (j = 0; j < numOfTable; j++) {
					if ((i & (1 << j)) != 0)
						set.add((String) sqlquery.getFromList().get(j));
				}
				map.put(key, set);
				key++;
			}
		}
		return map;
	}

// /count no. of one's when n is represented in binary
	public int countNumOfOnes(int n) {
		int num = 0;
		while (n != 0) {
			if ((n & 1) != 0)
				num++;
			n >>= 1;
		}
		return num;
	}
}
