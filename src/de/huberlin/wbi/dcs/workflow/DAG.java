package de.huberlin.wbi.dcs.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.Log;

public class DAG extends Workflow{
static	public long sumcut=0;  ;
	//num: number of partitions
		public void TabuPartition(List<Task> tasks, double compute,double bw){	
			
			int num=(int)Math.ceil((double)(tasks.size()/10));//num 划分次数，
			List<Partition> pList=new ArrayList<>();
			int size=(tasks.size()/num==0)?(tasks.size()/num):(tasks.size()/num+1);
		
			for(int i=0;i<num;i++){
				List<Task> ts=new ArrayList<>();
				for(int j=i*size;j<Integer.min(size*(i+1),tasks.size());j++)
					ts.add(tasks.get(j));					
				pList.add(new Partition(ts));
			}	
			Map<Partition, Double> weights=new HashMap<>();
			class WeightComparator implements Comparator<Partition> {
				@Override
				public int compare(Partition p1, Partition p2) {
					return Double.compare(weights.get(p2), weights.get(p1));
				}
			}
			for(Partition p:pList)
				weights.put(p, p.getWeight(compute, bw));		
			
			//Map<Task, Map<Partition,Long>> moveGains=new HashMap<>();
			Map<Task,Long> moveGains=new HashMap<>();
			class MoveGainComparator implements Comparator<Task> {
				@Override
				public int compare(Task t1, Task t2) {
					return Long.compare(moveGains.get(t1), moveGains.get(t2));
				}
			}
			for(Task task:tasks){
				for(Partition p:pList){//Log.printLine(task.getCloudletId()+"-"+task.getPartition());
					if(isNeibor(p,task.getPartition()))
						moveGains.put(task,MoveGain(task, p));
				}
			}
			int stop=0;
			
			List<List<Task>> bestPlist=new ArrayList<>();
			long bestCut=sumCut(pList);
			long seed=0;
			while(stop<100){	
				Random rand=new Random(seed++);
							
				Collections.sort(pList,new WeightComparator());
				int pIndex=rand.nextInt(pList.size()-1)+1;
				Partition destp=pList.get(pIndex);
				List<Task> movetaskList=new ArrayList<>();
				for(int i=0;i<pIndex;i++)
					movetaskList.addAll(pList.get(i).getTasks());
	//Log.printLine(pIndex+"---"+movetaskList.size()+"--"+destp.getTasks().size());
				Task movet=Collections.max(movetaskList,new MoveGainComparator());
							
				if(stop%10==0){
					pIndex=rand.nextInt(pList.size());
					destp=pList.get(pIndex);
					movetaskList.clear();;
					for(int i=0;i<pList.size();i++)
						if(i!=pIndex)
							movetaskList.addAll(pList.get(i).getTasks());
					movet=movetaskList.get(rand.nextInt(movetaskList.size()));
					
				}
				Partition sourcep=movet.getPartition();
			
				pList.remove(sourcep);
				pList.remove(destp);
							
				sourcep.removeTask(movet);
				destp.addTask(movet);
				pList.add(sourcep);
				pList.add(destp);
							
				weights.put(sourcep, sourcep.getWeight(compute, bw));
				weights.put(destp, destp.getWeight(compute, bw));
								
				//moveGains.clear();
				for(Task task:tasks){
					for(Partition p:pList){//Log.printLine(task.getCloudletId()+"-"+task.getPartition());
						if(isNeibor(p, task.getPartition()))
							moveGains.put(task,MoveGain(task, p));
					}
				}

				if(bestCut>=sumCut(pList)){
					bestPlist.clear();
					bestCut=sumCut(pList);
					for(Partition p:pList)
						bestPlist.add(new ArrayList<>(p.getTasks()));
						
					stop=0;
	for(Partition p:pList){
		for(Task t:p.getTasks())
			System.out.print(t.getCloudletId()+"-");
		System.out.println("single");}
	System.out.println("bestCut"+bestCut+"------");

				}
				else
					stop++;	
			}
			pList.clear();
			List<Partition> bestPset=new ArrayList<>();
			for(int i=0;i<bestPlist.size();i++)
				bestPset.add(new Partition(bestPlist.get(i)));
			sumcut+=sumCut(bestPset);
			Log.printLine("sumcut: "+sumcut);
		}
	private boolean isNeibor(Partition p1, Partition p2){
		for (Task t:p1.getSortedTasks()) {
			for (DataDependency outgoingEdge : t.getWorkflow().getGraph().getOutEdges(t)) {
				Task child = t.getWorkflow().getGraph().getDest(outgoingEdge);
				if(p2.containTask(child))
					return true;
			}
			for (DataDependency ingoingEdge : t.getWorkflow().getGraph().getInEdges(t)) {
				Task parent = t.getWorkflow().getGraph().getSource(ingoingEdge);
				if(p2.containTask(parent))
					return true;
			}
		}
		return false;
	}

	public long sumCut(List<Partition> plist){
		long sum=0;
		/*for(int i=0;i<plist.size();i++){
			Partition p1=plist.get(i);
			for(int j=i+1;j<plist.size();j++){
				sum+=netcut(p1,plist.get(j));
				//Log.print(sum+"-");
				//Log.printLine(p1+"--"+plist.get(j)+"--"+p1.netcut(plist.get(j)));
			}
		}*/
		for(Partition p1:plist){
			for(Partition p2:plist){
				sum+=netcut(p1,p2);
				//Log.print(sum+"-");
				//Log.printLine(p1+"--"+plist.get(j)+"--"+p1.netcut(plist.get(j)));
			}
		}
		return sum/2;
	}
	
	public long netcut(Partition p1, Partition p2){
		long cut=0;
		for(Task t:p2.getSortedTasks())
			cut+=p1.taskCut(t);
		return cut;
	}
	private long MoveGain(Task t, Partition dest){//new-old=source_else->t-t->dest
		long cut=0;
		Partition source=t.getPartition();
		for(Task telse:source.getSortedTasks()){
			if(telse.getCloudletId()!=t.getCloudletId()){
				if(t.getInputFilelist().get(telse)!=null)
					cut+=t.getInputFilelist().get(telse).getSize();
				if(t.getOutputFilelist().get(telse)!=null)
					cut+=t.getOutputFilelist().get(telse).getSize();
			}				
		}
		return cut-dest.taskCut(t);
	}
	public void multiPartition(double compute, double bw){
		
		List<Task> tasks=getSortedTasks();
		
		List<Partition> listbig=bipartition(tasks);
		List<Partition> list2=new ArrayList<>();
		int size=listbig.get(0).getTasks().size();
		
		while(size>10){
			for(Partition p:listbig)
				list2.addAll(bipartition(p.getSortedTasks()));
				
			listbig.clear();
			listbig.addAll(list2);
			list2.clear();
			size=listbig.get(0).getTasks().size();
		}
		for(Partition p:listbig){
			for(Task t:p.getTasks())
				System.out.print(t.getCloudletId()+"-");
			System.out.println("mulltisingle");}
		sumcut=sumCut(listbig);
	//	for(Partition p:listbig)
		//	TabuPartition(p.getSortedTasks(),compute,bw);
	}
	public long getData(){
		long data=0;
		for(Task t:getSortedTasks())
			data+=t.getCloudletFileSize();//.getCloudletOutputSize();
		return data;
	}
}
