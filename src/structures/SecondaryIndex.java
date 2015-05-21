package rdtrc.structures;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SecondaryIndex {

	public SecondaryIndex(String _tableName){
		tableName = _tableName;
	}
	
	private String tableName;
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private SecondaryIndexNode root;             // root of BST

	private class SecondaryIndexNode {
        private Integer key;           // sorted by key
        private HashMap<Tuple<Integer, Integer>, List<Integer>> val;         // associated data
        private SecondaryIndexNode left, right;  // left and right subtrees
        private Integer N;             // number of nodes in subtree

        public SecondaryIndexNode(Integer key, HashMap<Tuple<Integer, Integer>, List<Integer>> val, Integer N) {            this.key = key;
            this.val = val;
            this.N = N;
        }
    }
	
	// is  empty?
    public boolean isEmpty() {
        return size() == 0;
    }

    // return number of key-value pairs in SIT
    public Integer size() {
        return size(root);
    }

    // return number of key-value pairs in SIT rooted at x
    private Integer size(SecondaryIndexNode x) {
        if (x == null) return 0;
        else return x.N;
    }
    
    /***********************************************************************
     *  Search SIT for given key, and return associated value if found,
     *  return null if not found
     ***********************************************************************/
     // does there exist a key-value pair with given key?
     public boolean contains(Integer key) {
         return get(key) != null;
     }

     // return value associated with the given key, or null if no such key exists
     public HashMap<Tuple<Integer, Integer>, List<Integer>> get(Integer key) {
    	 return get(root, key);
     }

     private HashMap<Tuple<Integer, Integer>, List<Integer>> get(SecondaryIndexNode x, Integer key) {
    	 if (x == null) return null;
         int cmp = key.compareTo(x.key);
         if      (cmp < 0) return get(x.left, key);
         else if (cmp > 0) return get(x.right, key);
         else              return x.val;
     }
     
     /***********************************************************************
      *  Insert key-value pair into SIT
      *  If key already exists, update with new value
      ***********************************************************************/
     public void put(Integer key, HashMap<Tuple<Integer, Integer>, List<Integer>> val) {
          if (val == null) { delete(key); return; }
          root = put(root, key, val);
          assert check();
      }

     private SecondaryIndexNode put(SecondaryIndexNode x, Integer key, HashMap<Tuple<Integer, Integer>, List<Integer>> val) {
    	  if (x == null) return new SecondaryIndexNode(key, val, 1);
          int cmp = key.compareTo(x.key);
          if      (cmp < 0) x.left  = put(x.left,  key, val);
          else if (cmp > 0) x.right = put(x.right, key, val);
          else              x.val   = val;
          x.N = 1 + size(x.left) + size(x.right);
          return x;
      }
      
      /***********************************************************************
       *  Delete
       ***********************************************************************/
      
      public void deleteMin() {
          if (isEmpty()) return;
          root = deleteMin(root);
          assert check();
      }

      private SecondaryIndexNode deleteMin(SecondaryIndexNode x) {
          if (x.left == null) return x.right;
          x.left = deleteMin(x.left);
          x.N = size(x.left) + size(x.right) + 1;
          return x;
      }

      public void deleteMax() {
          if (isEmpty()) return;
          root = deleteMax(root);
          assert check();
      }      

      private SecondaryIndexNode deleteMax(SecondaryIndexNode x) {
          if (x.right == null) return x.left;
          x.right = deleteMax(x.right);
          x.N = size(x.left) + size(x.right) + 1;
          return x;
      }
      
      public void delete(Integer key) {
          root = delete(root, key);
          assert check();
      }
      
      private SecondaryIndexNode delete(SecondaryIndexNode x, Integer key) {
    	  if (x == null) return null;
          int cmp = key.compareTo(x.key);
          if      (cmp < 0) x.left  = delete(x.left,  key);
          else if (cmp > 0) x.right = delete(x.right, key);
          else { 
              if (x.right == null) return x.left;
              if (x.left  == null) return x.right;
              SecondaryIndexNode t = x;
              x = min(t.right);
              x.right = deleteMin(t.right);
              x.left = t.left;
          } 
          x.N = size(x.left) + size(x.right) + 1;
          return x;
      }
      
      /***********************************************************************
       *  Min, max, floor, and ceiling
       ***********************************************************************/
       public Integer min() {
           if (isEmpty()) return null;
           return min(root).key;
       } 

       private SecondaryIndexNode min(SecondaryIndexNode x) { 
           if (x.left == null) return x; 
           else                return min(x.left); 
       } 

       public Integer max() {
           if (isEmpty()) return null;
           return max(root).key;
       } 

       private SecondaryIndexNode max(SecondaryIndexNode x) { 
           if (x.right == null) return x; 
           else                 return max(x.right); 
       } 

       public Integer floor(Integer key) {
           SecondaryIndexNode x = floor(root, key);
           if (x == null) return null;
           else return x.key;
       } 

       private SecondaryIndexNode floor(SecondaryIndexNode x, Integer key) {
    	   if (x == null) return null;
           int cmp = key.compareTo(x.key);
           if (cmp == 0) return x;
           if (cmp <  0) return floor(x.left, key);
           SecondaryIndexNode t = floor(x.right, key); 
           if (t != null) return t;
           else return x;  
       } 

       public Integer ceiling(Integer key) {
           SecondaryIndexNode x = ceiling(root, key);
           if (x == null) return null;
           else return x.key;
       }

       private SecondaryIndexNode ceiling(SecondaryIndexNode x, Integer key) {
    	   if (x == null) return null;
           int cmp = key.compareTo(x.key);
           if (cmp == 0) return x;
           if (cmp < 0) { 
        	   SecondaryIndexNode t = ceiling(x.left, key); 
               if (t != null) return t;
               else return x; 
           } 
           return ceiling(x.right, key); 
       } 

      /***********************************************************************
       *  Rank and selection
       ***********************************************************************/
       public Integer select(Integer k) {
           if (k < 0 || k >= size()) return null;
           SecondaryIndexNode x = select(root, k);
           return x.key;
       }

       // Return key of rank k. 
       private SecondaryIndexNode select(SecondaryIndexNode x, Integer k) {
           if (x == null) return null; 
           Integer t = size(x.left); 
           if      (t > k) return select(x.left,  k); 
           else if (t < k) return select(x.right, k-t-1); 
           else            return x; 
       } 

       public int rank(Integer key) {
           return rank(key, root);
       } 

       // Number of keys in the subtree less than key.
       private int rank(Integer key, SecondaryIndexNode x) {
    	   if (x == null) return 0; 
           int cmp = key.compareTo(x.key); 
           if      (cmp < 0) return rank(key, x.left); 
           else if (cmp > 0) return 1 + size(x.left) + rank(key, x.right); 
           else              return size(x.left);  
       } 

      /***********************************************************************
       *  Range count and range search.
       ***********************************************************************/
       public Iterable<Integer> keys() {
           return keys(min(), max());
       }

       public Iterable<Integer> keys(Integer lo, Integer hi) {
           Queue<Integer> queue = new LinkedList<Integer>();
           keys(root, queue, lo, hi);
           return queue;
       } 

       private void keys(SecondaryIndexNode x, Queue<Integer> queue, Integer lo, Integer hi) { 
    	   if (x == null) return; 
           int cmplo = lo.compareTo(x.key); 
           int cmphi = hi.compareTo(x.key); 
           if (cmplo < 0) keys(x.left, queue, lo, hi); 
           if (cmplo <= 0 && cmphi >= 0) queue.add(x.key); 
           if (cmphi > 0) keys(x.right, queue, lo, hi);  
       } 

       public int size(Integer lo, Integer hi) {
    	   if (lo.compareTo(hi) > 0) return 0;
           if (contains(hi)) return rank(hi) - rank(lo) + 1;
           else              return rank(hi) - rank(lo);
       }


       // height of this BST (one-node tree has height 0)
       public int height() { return height(root); }
       private int height(SecondaryIndexNode x) {
           if (x == null) return -1;
           return 1 + Math.max(height(x.left), height(x.right));
       }


       // level order traversal
       public Iterable<Integer> levelOrder() {
           Queue<Integer> keys = new LinkedList<Integer>();
           Queue<SecondaryIndexNode> queue = new LinkedList<SecondaryIndexNode>();
           queue.add(root);
           while (!queue.isEmpty()) {
               SecondaryIndexNode x = queue.poll();
               if (x == null) continue;
               keys.add(x.key);
               queue.add(x.left);
               queue.add(x.right);
           }
           return keys;
       }

     /*************************************************************************
       *  Check integrity of BST data structure
       *************************************************************************/
       private boolean check() {
           if (!isBST())            System.out.println("Not in symmetric order");
           if (!isSizeConsistent()) System.out.println("Subtree counts not consistent");
           if (!isRankConsistent()) System.out.println("Ranks not consistent");
           return isBST() && isSizeConsistent() && isRankConsistent();
       }

       // does this binary tree satisfy symmetric order?
       // Note: this test also ensures that data structure is a binary tree since order is strict
       private boolean isBST() {
           return isBST(root, null, null);
       }

       // is the tree rooted at x a BST with all keys strictly between min and max
       // (if min or max is null, treat as empty constraint)
       // Credit: Bob Dondero's elegant solution
       private boolean isBST(SecondaryIndexNode x, Integer min, Integer max) {
           if (x == null) return true;
           if (min != null && x.key.compareTo(min) <= 0) return false;
           if (max != null && x.key.compareTo(max) >= 0) return false;
           return isBST(x.left, min, x.key) && isBST(x.right, x.key, max);
       } 

       // are the size fields correct?
       private boolean isSizeConsistent() { return isSizeConsistent(root); }
       private boolean isSizeConsistent(SecondaryIndexNode x) {
           if (x == null) return true;
           if (x.N != size(x.left) + size(x.right) + 1) return false;
           return isSizeConsistent(x.left) && isSizeConsistent(x.right);
       } 

       // check that ranks are consistent
       private boolean isRankConsistent() {
           for (int i = 0; i < size(); i++)
               if (i != rank(select(i))) return false;
           for (Integer key : keys())
               if (key.compareTo(select(rank(key))) != 0) return false;
           return true;
       }

}
