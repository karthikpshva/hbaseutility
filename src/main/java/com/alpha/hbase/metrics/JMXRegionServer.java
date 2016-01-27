package com.alpha.hbase.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.zookeeper.KeeperException;

/**
 * JMX RegionServer Metrics.
 * 
 * @author karthik
 *
 */

public class JMXRegionServer {

	// You can specify the port, Which is used to connect RS via JMX.
	// This port is configured in "/opt/hbase/conf/hbase-env.sh"
	// export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10102"
	
	private static final String JMXREMOTEPORT = "10102";
	private static final String REGIONSERVEROBJECT = "hadoop:service=RegionServer,name=RegionServerStatistics";
	private static final String JMXRMICONTEXT = "/jmxrmi";
	private static final String JMXSERVICEURL = "service:jmx:rmi:///jndi/rmi://";

	public static void main(String[] args) throws IOException,
			InterruptedException, MalformedObjectNameException,
			InstanceNotFoundException, IntrospectionException,
			ReflectionException, AttributeNotFoundException, MBeanException,
			KeeperException {

		Configuration conf = HBaseConfiguration.create();
		/*
		 * Uncomment the line below after adding Quorum. (or) Add hbase-site.xml
		 * in classpath
		 */
		// conf.set("hbase.zookeeper.quorum", "Comma Separated Zookeeper Quorum");
		// conf.set("hbase.zookeeper.property.clientPort", "2181");

		JMXServiceURL jmxURL=null;
		ObjectName objectName=new ObjectName(REGIONSERVEROBJECT);
		HashMap<String, String> enviornment=new HashMap<String, String>();
		HBaseAdmin hbaseAdmin=new HBaseAdmin(conf);
		HConnection connection=hbaseAdmin.getConnection();
		ClusterStatus clusterStatus=hbaseAdmin.getClusterStatus();
		Collection<ServerName> regionServerDetails=clusterStatus.getServers();
		Collection<ServerName> deadRegionServerDetails=clusterStatus.getDeadServerNames();
		Iterator<ServerName> iter=regionServerDetails.iterator();
		ServerName regionServerDetail=null;
		while (iter.hasNext()) {
			regionServerDetail=iter.next();
			if (!deadRegionServerDetails.contains(regionServerDetail)) {
				String regionServerHost=regionServerDetail.getHostname().split(",")[0];
				System.out.println("\n---------------------------\n"+regionServerHost+"\n---------------------------\n");
				jmxURL=new JMXServiceURL(JMXSERVICEURL+regionServerHost+":" +JMXREMOTEPORT+JMXRMICONTEXT);
				enviornment.clear();
				MBeanServerConnection beanServerConnection=JMXConnectorFactory.connect(jmxURL, enviornment).getMBeanServerConnection();
				MBeanInfo beanInfo = beanServerConnection.getMBeanInfo(objectName);
				for (MBeanAttributeInfo beanAttributeInfo : beanInfo.getAttributes()) {
					System.out.println(beanAttributeInfo.getName()+"="+beanServerConnection.getAttribute(objectName,beanAttributeInfo.getName()));
				}
			}
		}
	}
}