package de.huberlin.wbi.dcs;

import java.util.List;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;

public class PublicDatacenter extends PrivateDatacenter{

	public PublicDatacenter(String name, DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval)
					throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);

		setLastMonitor(0.0);
	}

	
	@Override
	public int allocateTask(Task task){
		int vmId=0;
		double bestFinish=Double.MAX_VALUE;
		
		for(Vm vm:getVmList()){
			double est=0.0;
			for (DataDependency inEdge : task.getWorkflow().getGraph().getInEdges(task)) {
				Task parent = task.getWorkflow().getGraph().getSource(inEdge);
				long filesize=task.getInputFilelist().get(parent).getSize();
				double filetime=0;
				if(!inDC(parent.getVmId()))
					filetime=filesize/Parameters.OUTBW;
					
			if(est<parent.getFinishTime()+filetime)	
				est=parent.getFinishTime()+filetime;
			}
			double finishvm=Double.max(est,vm.getCloudletScheduler().getNextSlot())+task.getCloudletLength()/vm.getMips();
			if(bestFinish>finishvm){
				bestFinish=finishvm;
				vmId=vm.getId();
			}
		}
		return vmId;
	}
	@Override
	protected double predictFileTransferTime(Task task,HybridVm vm) {
		double maxTaskStart=0.0;
		double maxParentFinish=0.0;
		for (DataDependency inEdge : task.getWorkflow().getGraph().getInEdges(task)) {
			Task parent = task.getWorkflow().getGraph().getSource(inEdge);
			
			long filesize=task.getInputFilelist().get(parent).getSize();
			
			double filetime=0;
			if(!inDC(parent.getVmId()))
				filetime=filesize/Parameters.OUTBW;
			
			double parentfinish=parent.getFinishTime();
			if(maxTaskStart<parentfinish+filetime)
				maxTaskStart=parentfinish+filetime;
			if(maxParentFinish<parentfinish)
				maxParentFinish=parentfinish;
		}
		//Log.printLine(task.getCloudletId()+" in "+task.getCloudletFileSize()+" out "+task.getCloudletOutputSize() +" time "+time+"------");
		return maxTaskStart-maxParentFinish;
	}
	
}
