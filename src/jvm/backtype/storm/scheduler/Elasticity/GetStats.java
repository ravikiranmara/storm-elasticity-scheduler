package backtype.storm.scheduler.Elasticity;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class GetStats {

	  private final TSocket tsocket;
	  private final Nimbus.Client client;
	  private static final Logger LOG = LoggerFactory
	      .getLogger(GetStats.class);
	  TFramedTransport tTransport;
	  TBinaryProtocol tBinaryProtocol;
	  Client metrics_send_client;

	  public GetStats() {
	    this.tsocket = new TSocket("localhost", 6627);
	    this.tTransport = new TFramedTransport(tsocket);
	    this.tBinaryProtocol = new TBinaryProtocol(tTransport);
	    this.client = new Nimbus.Client(tBinaryProtocol);
	  }

	  public void getStatistics() {
	    LOG.info("Getting stats...");
	    try {
	      this.tTransport.open();


	        ClusterSummary clusterSummary = client.getClusterInfo();
	        List<TopologySummary> topologies = clusterSummary.get_topologies();
	        LOG.info("number of topologies: {}", topologies.size());
	        for (TopologySummary topo : topologies) {
	          TopologyInfo topologyInfo = null;
	          try {
	            topologyInfo = client.getTopologyInfo(topo.get_id());
	          } catch (Exception e) {
	            System.out.println(e);
	            continue;
	          }
	          List<ExecutorSummary> executorSummaries = topologyInfo
	              .get_executors();

	          for (ExecutorSummary executorSummary : executorSummaries) {

	            ExecutorStats executorStats = executorSummary.get_stats();
	            if (executorStats == null) {
	              System.out.println("NULL");
	              continue;
	            }
	            String host = executorSummary.get_host();
	            int port = executorSummary.get_port();
	            String componentId = executorSummary.get_component_id();

	            // System.out.println("task_id: "+Integer.toString(executorSummary.getExecutor_info().getTask_start()));

	            String taskId = Integer.toString(executorSummary
	                .get_executor_info().get_task_start());

	            Map<String, Map<String, Long>> transfer = executorStats
	                .get_transferred();

	            if (transfer.get(":all-time").get("default") != null) {

	              System.out.println((host + ':' + port + ':' + componentId + ":"
	                  + topo.get_id() + ":" + taskId + "," + transfer.get(
	                  ":all-time").get("default")));
	              LOG.info((host + ':' + port + ':' + componentId + ":"
	                  + topo.get_id() + ":" + taskId + "," + transfer.get(
	                  ":all-time").get("default")));
	              /*
	              for (SupervisorSummary sup : clusterSummary.get_supervisors()) {
	                LOG.info("SUP: {} availResources: {}", sup.get_host(), sup.get_total_resources());
	              }
	              */
	              long unixTime = System.currentTimeMillis() / 1000;
	              String data = String.valueOf(unixTime) + ':' + host + ':' + port + ':' + componentId + ":"
	                  + topo.get_id() + ":" + taskId + "," + transfer.get(
	                  ":all-time").get("default")+"\n";
	              
	              String filePath = "/tmp/scheduler_output";
	              try {
	              LOG.info("writting to file...");
	              File file = new File(filePath);
	              FileWriter fileWritter = new FileWriter(file,true);
	              BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	              bufferWritter.append(data);
	              bufferWritter.close();
	              fileWritter.close();
	              } catch(IOException ex) {
	                LOG.info("error! writin to file {}", ex);
	              }
	            }
	          }
	        }
	    } catch (TException e) {
	      e.printStackTrace();
	    }
	  }

	}

