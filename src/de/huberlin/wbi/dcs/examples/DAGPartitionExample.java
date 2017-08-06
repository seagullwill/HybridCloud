package de.huberlin.wbi.dcs.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

//import de.huberlin.wbi.dcs.HybridCloudletScheduler;
import de.huberlin.wbi.dcs.HybridHost;
import de.huberlin.wbi.dcs.HCOC;
import de.huberlin.wbi.dcs.DynamicHybridDCBroker;
import de.huberlin.wbi.dcs.HybridDCBroker;
import de.huberlin.wbi.dcs.PrivateDatacenter;
import de.huberlin.wbi.dcs.PublicDatacenter;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.workflow.DAG;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import de.huberlin.wbi.dcs.workflow.io.AlignmentTraceFileReader;
import de.huberlin.wbi.dcs.workflow.io.DaxFileReader;
import de.huberlin.wbi.dcs.workflow.io.MontageTraceFileReader;

public class DAGPartitionExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		  DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  
		Parameters.parseParameters(args);
		try {
				Calendar calendar = Calendar.getInstance();
				DAG workflow = new DaxFileReader().parseLogFiledag(2,
						"examples/CYBERSHAKE.n.500.10.dax", true, true, null);
					//	"examples/Montage_25.xml", true, true, null);
				Date start=new Date();
				//workflow.TabuPartition(workflow.getSortedTasks(),355,20*1024*1024);
				Date tabu=new Date();
				workflow.multiPartition(355,20*1024*1024);
			Date end=new Date();
			  Log.printLine("start time- "+start+" end- "+end);
			  Log.printLine("tabutime="+(tabu.getTime()-start.getTime()));
			  Log.printLine("multitime="+(end.getTime()-tabu.getTime()));
			  Log.printLine("multidata="+workflow.sumcut);
			  Log.printLine("totaldata="+workflow.getData());
		}catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
		
	}
}
