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
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import de.huberlin.wbi.dcs.workflow.io.AlignmentTraceFileReader;
import de.huberlin.wbi.dcs.workflow.io.DaxFileReader;
import de.huberlin.wbi.dcs.workflow.io.MontageTraceFileReader;

public class HybridCloudExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Date start=new Date();
		  DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Parameters.parseParameters(args);
		try {
			//for (int i = 0; i < Parameters.numberOfRuns; i++) {
				HybridCloudExample ex = new HybridCloudExample();
				if (!Parameters.outputDatacenterEvents) {
					Log.disable();
				}
				// Initialize the CloudSim package
				int num_user = 1; // number of grid users
				Calendar calendar = Calendar.getInstance();
				boolean trace_flag = false; // mean trace events
				/*CloudSim.init(num_user, calendar, trace_flag);

				ex.createPrivateDatacenter("private");
				ex.createPublicDatacenter("public");
				//HybridDCBroker scheduler = ex.createScheduler(i);
				*/
				HybridDCBroker scheduler = ex.createScheduler(0);
				
				/*ex.createVms(i, scheduler,"private");
				//ex.createVms(i, scheduler,"public");
				double times[]=new double [51986];
				try {
		            BufferedReader reader = new BufferedReader(new FileReader("/Users/oupeng/Documents/workspace/data07/jobtime.csv"));//换成你的文件名
		            String line = line=reader.readLine();
		            String item[] = line.split(",");
		            for(int i=0;i<51986;i++)               
		                times[i]=(double)(Double.parseDouble(item[i])/1000);//jobtime,s 
		        } catch (Exception e) {
		            e.printStackTrace();
		        }
				//for(int i=0;i<51986;i++){
				/for(int i=0;i<100;i++){*/
					Workflow workflow = buildWorkflow(scheduler);
				//	workflow.setSubmitTime(times[i]);
					//ex.submitWorkflow(workflow, scheduler);				
				//}
				// Start the simulation
				
				/*CloudSim.startSimulation();
				//CloudSim.stopSimulation();
		//	int dlmet=0;
		//	for(int i=0;i<scheduler.getWorkflows().size();i++){
			//	Log.printLine("DL-"+i+": "+scheduler.getWorkflows().get(i).getDeadline());
				//Log.printLine("finish-"+i+" : "+scheduler.getWorkflows().get(i).getFinishTime());
			//	if(scheduler.getWorkflows().get(i).getFinishTime()<scheduler.getWorkflows().get(i).getDeadline())
			//		dlmet++;
		//	}
			Log.printLine("DL met: "+dlmet);
			Log.printLine("cost: "+scheduler.getCost());
			Log.printLine("moveData: "+scheduler.getMovedata());
			Log.printLine("public: "+scheduler.publicNum());
*/
			Date end=new Date();
			  Log.printLine("start time- "+format.format(start)+" end- "+end);
			/*try {
				File csvRec = new File("/Users/oupeng/Documents/workspace/data07/result.csv");
				BufferedWriter bw = new BufferedWriter(new FileWriter(csvRec, true)); 
				Iterator<String> result=scheduler.Result.iterator();
				while(result.hasNext()){
					bw.write(result.next());
					bw.newLine();
				}
				bw.close(); 			
			}catch (FileNotFoundException e) {
				// 捕获File对象生成时的异常             
				e.printStackTrace();
				} catch (IOException e) {
					// 捕获BufferedWriter对象关闭时的异常
					e.printStackTrace();
					}*/
		}catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
		
	}
	public HybridDCBroker createScheduler(int i) {
		try {
			switch (Parameters.scheduler) {
			case HCOC:
				return new HCOC("HCOC", Parameters.taskSlotsPerVm);
			case OnlineTask:
				return new DynamicHybridDCBroker("Online-task", Parameters.taskSlotsPerVm);
			
			//case OnSub:
			//	return new OnSub("online-subworkflow", Parameters.taskSlotsPerVm);
			default:
				
				Log.printLine("wrong!");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Workflow buildWorkflow(HybridDCBroker scheduler) {
		Random rand=new Random(Parameters.seed++);
		String file=Integer.toString(rand.nextInt(20));
		switch (Parameters.experiment) {
		case MONTAGE_25:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage_25.xml", true, true, null);
		case EPIGENOMICS_997:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Epigenomics_997.xml", true, true, null);
		case MONTAGE_50:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage.n.50.19.dax", true, true, null);
		case MONTAGE_100:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage.n.100.0.dax", true, true, null);
		case MONTAGE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage.n.1000.0.dax", true, true, null);
		
		case CYBERSHAKE_50:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.50.19.dax", true, true, null);
		case CYBERSHAKE_100:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.100."+file+".dax", true, true, null);
		case CYBERSHAKE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.1000."+file+".dax", true, true, null);
		case CYBERSHAKE_200:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.200.17.dax", true, true, null);
		case CYBERSHAKE_300:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.300.17.dax", true, true, null);	
		case CYBERSHAKE_400:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.400.17.dax", true, true, null);	
		case CYBERSHAKE_500:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.500.17.dax", true, true, null);	
		case CYBERSHAKE_600:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.600.17.dax", true, true, null);	
		case CYBERSHAKE_700:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.700.17.dax", true, true, null);	
		case CYBERSHAKE_800:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.800.17.dax", true, true, null);	
		case CYBERSHAKE_900:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CYBERSHAKE.n.900.17.dax", true, true, null);	
		
		case GENOME_100:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/GENOME.n.100.0.dax", true, true, null);
		case GENOME_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/GENOME.n.1000.0.dax", true, true, null);
		}
		return null;
	}

	public void submitWorkflow(Workflow workflow, HybridDCBroker scheduler) {
		// Create Cloudlets and send them to Scheduler
		if (Parameters.outputWorkflowGraph) {
			workflow.visualize(1920, 1200);
		}
		scheduler.submitWorkflow(workflow);
	}

	// all numbers in 1000 (e.g. kb/s)
	public PrivateDatacenter createPrivateDatacenter(String name) {
		List<HybridHost> hostList = new ArrayList<HybridHost>();
		long storage = 1024 * 1024;
		int ram = (int) (32 * 1024 );			
		int hostId = 0;
		int hostnum=1;
		if(name.contains("public")){
			hostId=1000;
			hostnum=3;
			for (int i = 0; i < Parameters.nXeonE5430public*hostnum; i++) 				
				hostList.add(new HybridHost(hostId++, ram, Parameters.INBW, Parameters.iopsPerPe, storage,
						Parameters.nCusPerCoreXeonE5430, Parameters.nCoresXeonE5430, Parameters.mipsPerCoreXeonE5430));

		}
		else{			
		for (int i = 0; i < Parameters.nXeonE5430private*hostnum; i++) 				
			hostList.add(new HybridHost(hostId++, ram, Parameters.INBW, Parameters.iopsPerPe, storage,
					Parameters.nCusPerCoreXeonE5430, Parameters.nCoresXeonE5430, Parameters.mipsPerCoreXeonE5430));
		}
		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<Storage>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		PrivateDatacenter datacenter = null;
		
		try {
			datacenter = new PrivateDatacenter(name, characteristics,
					new VmAllocationPolicyRandom(hostList, Parameters.seed++), storageList, 0);//随机把vm放到一个host上
					//new VmAllocationPolicyRandom(hostList, 0), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return datacenter;
	}
	public PublicDatacenter createPublicDatacenter(String name) {
		List<HybridHost> hostList = new ArrayList<HybridHost>();
		long storage = 1024 * 1024;
		int ram = (int) (32 * 1024 );			
		int hostId = 0;
		int hostnum=1;
		hostId=1000;
		hostnum=3;
		for (int i = 0; i < Parameters.nXeonE5430public*hostnum; i++) 				
			hostList.add(new HybridHost(hostId++, ram, Parameters.INBW, Parameters.iopsPerPe, storage,
					Parameters.nCusPerCoreXeonE5430, Parameters.nCoresXeonE5430, Parameters.mipsPerCoreXeonE5430));

		
		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<Storage>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		PublicDatacenter datacenter = null;
		
		try {
			datacenter = new PublicDatacenter(name, characteristics,
					new VmAllocationPolicyRandom(hostList, Parameters.seed++), storageList, 0);//随机把vm放到一个host上
					//new VmAllocationPolicyRandom(hostList, 0), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return datacenter;
	}
}
