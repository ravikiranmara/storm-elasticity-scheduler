package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.HelperFuncs;
import backtype.storm.scheduler.Elasticity.Node;

public class ScaleInExecutorStrategy {
	protected static Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	protected TreeMap<Node, Integer> _rankedMap;
	protected ArrayList<Node> _rankedList = new ArrayList<Node>();

	public ScaleInExecutorStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies, TreeMap<Node, Integer> rankedMap) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this._rankedMap = rankedMap;
		for(Entry<Node, Integer> entry : rankedMap.entrySet()) {
			this._rankedList.add(entry.getKey());
		}
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	
	public Map<Component, Integer> findComponentsOnNode(ArrayList<String>supervisorIds) {
		HashMap<Component, Integer> comps = new HashMap<Component, Integer>();
		for(String supervisorId : supervisorIds) {
			Node n = this._globalState.nodes.get(supervisorId);
			for(ExecutorDetails exec : n.execs) {
				String comp = this._topo.getExecutorToComponent().get(exec);
				Component component = this._globalState.components.get(this._topo.getId()).get(comp);
				if(comps.containsKey(comp) == false) {
					comps.put(component, 0);
				}
				comps.put(component, comps.get(component)+1);
			}
		}
		return comps;
		
	} 
	
	public void removeNodesBySupervisorId(ArrayList<String> supervisorIds) {
		
		
			Map<Component, Integer> compExecRm = this.findComponentsOnNode(supervisorIds);
			LOG.info("Nodes: {} has Components: {}", supervisorIds, compExecRm);
			
			HelperFuncs.decreaseParallelism(compExecRm, this._topo);
			
			LOG.info("Status: {}", HelperFuncs.getStatus(this._topo.getId()));
			while(HelperFuncs.getStatus(this._topo.getId()).equals("ACTIVE")){
				
			}
			LOG.info("Status: {}", HelperFuncs.getStatus(this._topo.getId()));

			while(HelperFuncs.getStatus(this._topo.getId()).equals( "REBALANCING")) {
				
			}
				
				
				
			
	}
	
	public static void decreaseParallelism(Map<Component, Integer> compMap, TopologyDetails topo) {
		String cmd = "/var/storm/storm_0/bin/storm rebalance -w 0 "+topo.getName();
		for(Entry<Component, Integer> entry : compMap.entrySet()) {
			Integer parallelism_hint = entry.getKey().execs.size() - entry.getValue();
			String component_id = entry.getKey().id;
			LOG.info("Increasing parallelism to {} of component {} in topo {}", new Object[]{parallelism_hint, component_id, topo.getName()});
			cmd+=" -e "+component_id+"="+parallelism_hint;
		}

		Process p;
		try {
			LOG.info("cmd: {}", cmd);
			p = Runtime.getRuntime().exec(cmd);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public WorkerSlot findBestSlot3(Node node) {

		LOG.info("Node: " + node.hostname);
		WorkerSlot target = null;
		int least = Integer.MAX_VALUE;
		for (Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec
				.entrySet()) {
			List<ExecutorDetails> execs = this._globalState.schedState.get(
					this._topo.getId()).get(entry.getKey());
			if (execs != null && execs.size() > 0) {
				LOG.info("-->slots: {} execs {}", entry.getKey().getPort(),
						execs.size());
				if (execs.size() < least) {

					target = entry.getKey();
					least = execs.size();
				}
			}
		}
		return target;
	}
}
