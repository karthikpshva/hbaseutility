package com.alpha.hbase.compaction;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helps to find compaction status across region-servers.
 * Gives total no of region count that resides across regions-servers for a table.
 * 
 * @author karthik
 *
 */

public class StatusUtil {
	private static HBaseAdmin hbaseAdmin = null;

	public static void getStatus(String tableName) throws IOException, InterruptedException {
		ArrayList<String> statistics = new ArrayList<String>();
		int regionCount = 0;
		HRegionInfo regionInfo = null;
		CompactionRequest.CompactionState state = null;
		String hostname = "";
		HConnection connection = hbaseAdmin.getConnection();
		ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
		Collection<ServerName> regionServerDetails = clusterStatus.getServers();
		Collection<ServerName> deadRegionServerDetails = clusterStatus.getDeadServerNames();
		Iterator<ServerName> iter = regionServerDetails.iterator();
		ServerName regionServerDetail = null;
		while (iter.hasNext()) {
			regionServerDetail = iter.next();
			if (deadRegionServerDetails.contains(regionServerDetail)) {
				iter.remove();
			}
		}

		HRegionInterface[] regionServers = new HRegionInterface[regionServerDetails.size()];
		iter = regionServerDetails.iterator();
		while (iter.hasNext()) {
			regionServerDetail = iter.next();
			regionServers[regionCount] = connection.getHRegionConnection(regionServerDetail.getHostname(),
					regionServerDetail.getPort());
			regionCount++;
		}
		List<HRegionInfo> regionListInTable = hbaseAdmin.getTableRegions(tableName.getBytes());

		for (int k = 0; k < regionServers.length; k++) {
			int major = 0, minor = 0, major_and_minor = 0, none = 0, count = 0;
			hostname = regionServers[k].getHServerInfo().getHostname();
			for (int j = 0; j < regionListInTable.size(); j++) {
				try {
					regionInfo = regionServers[k].getRegionInfo(regionListInTable.get(j).getRegionName());
					if (regionInfo != null) {
						state = hbaseAdmin.getCompactionState(regionListInTable.get(j).getRegionName());
						if (state == CompactionState.MAJOR) {
							major++;
						} else if (state == CompactionState.MINOR) {
							minor++;
						} else if (state == CompactionState.MAJOR_AND_MINOR) {
							major_and_minor++;
						} else {
							none++;
						}
						count++;
					}
				} catch (Exception e) {
					continue;
				}
			}

			statistics.add("-------------------\nRegionServer : " + hostname + "\nRegion Count : " + count
					+ "\nMajor Compacting Region : " + major + "\nminor Compacting Region : " + minor
					+ "\nMajor & Minor Region : " + major_and_minor + "\nCompaction Not Required Region : " + none);

		}
		for (String info : statistics) {
			System.out.println(info);
		}

	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		//Uncomment the line below after adding Quorum. (or) Add hbase-site.xml in classpath
		//conf.set("hbase.zookeeper.quorum", "Comma Separated Zookeeper Quorum");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseAdmin = new HBaseAdmin(conf);
		String tablename = args[0];
		if (!hbaseAdmin.isTableAvailable(Bytes.toBytes(tablename))) {
			System.out.println("Given table is not available");
			System.exit(-1);
		}
		getStatus(tablename);
		hbaseAdmin.close();
	}
}