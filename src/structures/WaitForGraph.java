package rdtrc.structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jgraph.graph.DefaultEdge;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;

public class WaitForGraph {

	DefaultDirectedGraph<String, DefaultEdge> graph;
	CycleDetector<String, DefaultEdge> cycleDetector;
	private HashMap<DefaultEdge, String> entities;

	public WaitForGraph() {
		entities = new HashMap<DefaultEdge, String>();
		graph = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
		cycleDetector = null;
	}

	public void addNode(long transactionId) {
		if (!graph.containsVertex(String.valueOf(transactionId)))
			graph.addVertex(String.valueOf(transactionId));
	}

	// returns TRUE: if deadlock
	public boolean buildEdge(long from, long to, String tableName) {
		boolean result = false;

		DefaultEdge edge = graph.addEdge(String.valueOf(from),
				String.valueOf(to));
		cycleDetector = new CycleDetector<String, DefaultEdge>(graph);
		result = cycleDetector.detectCycles();
		if (!result)
			entities.put(edge, tableName);

		return result;
	}

	public boolean isBlocked(long transactionId) {
		boolean result = false;
		try {
			if (graph.outDegreeOf(String.valueOf(transactionId)) > 0)
				result = true;
		} catch (Exception e) {

		}
		return result;
	}

	public List<Long> removeNode(long transactionId) {
		List<Long> result = new ArrayList<Long>();
		List<Long> connectedNodes = new ArrayList<Long>();
		List<String> tables = new ArrayList<String>();

		if(graph.containsVertex(String.valueOf(transactionId))) {
			Iterator<DefaultEdge> incomingEdgesIterator = graph.incomingEdgesOf(
					String.valueOf(transactionId)).iterator();
			while (incomingEdgesIterator.hasNext()) {
				DefaultEdge edge = incomingEdgesIterator.next();
				tables.add(entities.get(edge));
				connectedNodes.add(Long.valueOf(graph.getEdgeSource(edge)));
			}


			Set<String> duplicates = new HashSet();
			Set<String> tempHolder = new HashSet();
			for (String t : entities.values()) {
				if (!tempHolder.add(t)) {
					duplicates.add(t);
				}
			}
			cleanStructure(transactionId);

			for (int i = 0; i < tables.size(); i++) {
				if (!duplicates.contains(tables.get(i)))
					result.add(connectedNodes.get(i));
			}

			List<Long> list = new ArrayList<Long>();
			Iterator<String> duplicatesIterator = duplicates.iterator();
			while (duplicatesIterator.hasNext()) {
				String temp = duplicatesIterator.next();
				for (int j = 0; j < tables.size(); j++) {
					if (temp.equals(tables.get(j))) {
						list.add(connectedNodes.get(j));
					}
				}
				Collections.sort(list);
				for (int k = list.size() - 1; k > 0; k--) {
					DefaultEdge edge = graph.addEdge(String.valueOf(list.get(k)),
							String.valueOf(list.get(k - 1)));
					entities.put(edge, temp);
					result.add(list.get(k - 1));
				}
				list.clear();
			}

			graph.removeVertex(String.valueOf(transactionId));
		}

		return result;
	}

	// ////////////////////////////////////////////////////////////

	private void cleanStructure(long transactionId) {
		Set<DefaultEdge> incoimingEdges = graph.incomingEdgesOf(String
				.valueOf(transactionId));
		Set<DefaultEdge> outgoingEdges = graph.outgoingEdgesOf(String
				.valueOf(transactionId));

		java.util.Iterator<DefaultEdge> incomingIterator = incoimingEdges
				.iterator();
		java.util.Iterator<DefaultEdge> outgoingIterator = outgoingEdges
				.iterator();

		while (incomingIterator.hasNext()) {
			DefaultEdge edge = incomingIterator.next();
			entities.remove(edge);
		}
		while (outgoingIterator.hasNext()) {
			DefaultEdge edge = outgoingIterator.next();
			entities.remove(edge);
		}

	}

	public String toString() {
		return graph.toString();
	}
}
