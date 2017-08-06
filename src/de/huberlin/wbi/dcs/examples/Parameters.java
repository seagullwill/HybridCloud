package de.huberlin.wbi.dcs.examples;

import java.util.Random;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.ExponentialDistr;
import org.cloudbus.cloudsim.distributions.GammaDistr;
import org.cloudbus.cloudsim.distributions.LognormalDistr;
import org.cloudbus.cloudsim.distributions.LomaxDistribution;
import org.cloudbus.cloudsim.distributions.ParetoDistr;
import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.cloudbus.cloudsim.distributions.WeibullDistr;
import org.cloudbus.cloudsim.distributions.ZipfDistr;

import de.huberlin.wbi.dcs.distributions.NormalDistribution;
//定义vm的cpu、内存、io、带宽的波动性，其中各种模型的各种参数，需要的话，可以改默认值
public class Parameters {

	public static boolean considerDataLocality = false;
	
	// datacenter params
	// Kb / s intercloud 10MBps,datasize in B cybershake 100 tasks 30G data movement
	public static long INBW = 20*1024*1024;
	public static long OUTBW = 10*1024*1024;
	public static double priceData= 0.000000000015;
	// Kb / s
	public static long iopsPerPe = 20 * 1024*8;

//used
	public static int nXeonE5430private = 1000;
	public static int nXeonE5430public = 10000;
	public static int nCusPerCoreXeonE5430 = 1;
	public static int nCoresXeonE5430 = 8;
	public static int mipsPerCoreXeonE5430 = 355;
//
	public static int PartitionSize=16;//16
	// vm params
	public static int nVms =16;
	public static int taskSlotsPerVm = 1;

	public static double numberOfCusPerPe = 1;
	public static int numberOfPes = 1;
	public static int ram = (int) (1.7 * 1024);

	public enum Experiment {//workflow输入列表
		MONTAGE_TRACE_1, MONTAGE_TRACE_12, EPIGENOMICS_997,ALIGNMENT_TRACE, CUNEIFORM_VARIANT_CALL, HETEROGENEOUS_TEST_WORKFLOW,
		MONTAGE_25,MONTAGE_50,MONTAGE_100,MONTAGE_200,MONTAGE_300,MONTAGE_400,MONTAGE_500,MONTAGE_600,MONTAGE_700,MONTAGE_800,MONTAGE_900, MONTAGE_1000,
		CYBERSHAKE_50,CYBERSHAKE_100,CYBERSHAKE_200,CYBERSHAKE_300,CYBERSHAKE_400,CYBERSHAKE_500,CYBERSHAKE_600,CYBERSHAKE_700,CYBERSHAKE_800,
		CYBERSHAKE_900, CYBERSHAKE_1000 ,GENOME_100,GENOME_1000
	
	}

//选择不同的调度task
	public static Experiment experiment = Experiment.CYBERSHAKE_100;
	//public static Experiment experiment = Experiment.
	public static boolean outputDatacenterEvents = true;
	public static boolean outputWorkflowGraph = false;
	public static boolean outputVmPerformanceLogs = false;

	// experiment parameters，不同的workflow调度方法，RR、HEFT、队列、LATE、C3、C20（后两个是怎么调度机制？所以这里可以拓展）
	//均extend AbstractWorkflowSchedule,AbstractWorkflowSchedule extend DCBroker
	public enum Scheduler {
		HCOC,OnlineTask,OnSub,DGreedy
	}

	public static Scheduler scheduler = Scheduler.OnlineTask;
	public static int numberOfRuns = 1;

	public enum Distribution {
		EXPONENTIAL, GAMMA, LOGNORMAL, LOMAX, NORMAL, PARETO, UNIFORM, WEIBULL, ZIPF
	}
//seconds
	public static int updateFrequency=2500;//100;///for100 50,100,200,400,800,1000,1200;/for 1000/50,100,200,500,1000,1500,2000
	public static double dlcybershake100=1000;//1250.0;
	public static double dlcybershake1000=1500;//1700.0;
	public static double dlmontage100=150;//75.0;
	
	// CPU Heterogeneity
	public static Distribution cpuHeterogeneityDistribution = Distribution.NORMAL;
	public static double cpuHeterogeneityCV = 0.4;
	public static int cpuHeterogeneityAlpha = 0;
	public static double cpuHeterogeneityBeta = 0d;
	public static double cpuHeterogeneityShape = 0d;
	public static double cpuHeterogeneityLocation = 0d;
	public static double cpuHeterogeneityShift = 0d;
	public static double cpuHeterogeneityMin = 0d;
	public static double cpuHeterogeneityMax = 0d;
	public static int cpuHeterogeneityPopulation = 0;

	// IO Heterogeneity
	public static Distribution ioHeterogeneityDistribution = Distribution.NORMAL;
	public static double ioHeterogeneityCV =0.15;
	public static int ioHeterogeneityAlpha = 0;
	public static double ioHeterogeneityBeta = 0d;
	public static double ioHeterogeneityShape = 0d;
	public static double ioHeterogeneityLocation = 0d;
	public static double ioHeterogeneityShift = 0d;
	public static double ioHeterogeneityMin = 0d;
	public static double ioHeterogeneityMax = 0d;
	public static int ioHeterogeneityPopulation = 0;

	// BW Heterogeneity
	public static Distribution bwHeterogeneityDistribution = Distribution.NORMAL;
	public static double bwHeterogeneityCV = 0.2;
	public static int bwHeterogeneityAlpha = 0;
	public static double bwHeterogeneityBeta = 0d;
	public static double bwHeterogeneityShape = 0d;
	public static double bwHeterogeneityLocation = 0d;
	public static double bwHeterogeneityShift = 0d;
	public static double bwHeterogeneityMin = 0d;
	public static double bwHeterogeneityMax = 0d;
	public static int bwHeterogeneityPopulation = 0;
 
	public static double Df=0.5;//变动频率, 每小时几次,2,1,0.5,default=0.5
	public static double Dv=0.25;//变动值,0.25,0.5
	// CPU Dynamics
	public static double cpuBaselineChangesPerHour = Df;//0.5;
	public static Distribution cpuDynamicsDistribution = Distribution.NORMAL;
	public static double cpuDynamicsCV = 0.054;//Dv;
	public static int cpuDynamicsAlpha = 0;
	public static double cpuDynamicsBeta = 0d;
	public static double cpuDynamicsShape = 0d;
	public static double cpuDynamicsLocation = 0d;
	public static double cpuDynamicsShift = 0d;
	public static double cpuDynamicsMin = 0d;
	public static double cpuDynamicsMax = 0d;
	public static int cpuDynamicsPopulation = 0;

	// IO Dynamics
	public static double ioBaselineChangesPerHour =Df;//0.5;
	public static Distribution ioDynamicsDistribution = Distribution.NORMAL;
	public static double ioDynamicsCV = 0.033;
	public static int ioDynamicsAlpha = 0;
	public static double ioDynamicsBeta = 0d;
	public static double ioDynamicsShape = 0d;
	public static double ioDynamicsLocation = 0d;
	public static double ioDynamicsShift = 0d;
	public static double ioDynamicsMin = 0d;
	public static double ioDynamicsMax = 0d;
	public static int ioDynamicsPopulation = 0;

	// BW Dynamics
	public static double bwBaselineChangesPerHour = Df;//0.5;
	public static Distribution bwDynamicsDistribution = Distribution.NORMAL;
	public static double bwDynamicsCV = 0.04;
	public static int bwDynamicsAlpha = 0;
	public static double bwDynamicsBeta = 0d;
	public static double bwDynamicsShape = 0d;
	public static double bwDynamicsLocation = 0d;
	public static double bwDynamicsShift = 0d;
	public static double bwDynamicsMin = 0d;
	public static double bwDynamicsMax = 0d;
	public static int bwDynamicsPopulation = 0;

	// CPU noise
	public static Distribution cpuNoiseDistribution = Distribution.NORMAL;
	public static double cpuNoiseCV = 0.028;
	public static int cpuNoiseAlpha = 0;
	public static double cpuNoiseBeta = 0d;
	public static double cpuNoiseShape = 0d;
	public static double cpuNoiseLocation = 0d;
	public static double cpuNoiseShift = 0d;
	public static double cpuNoiseMin = 0d;
	public static double cpuNoiseMax = 0d;
	public static int cpuNoisePopulation = 0;

	// IO noise
	public static Distribution ioNoiseDistribution = Distribution.NORMAL;
	public static double ioNoiseCV = 0.007;
	public static int ioNoiseAlpha = 0;
	public static double ioNoiseBeta = 0d;
	public static double ioNoiseShape = 0d;
	public static double ioNoiseLocation = 0d;
	public static double ioNoiseShift = 0d;
	public static double ioNoiseMin = 0d;
	public static double ioNoiseMax = 0d;
	public static int ioNoisePopulation = 0;

	// BW noise
	public static Distribution bwNoiseDistribution = Distribution.NORMAL;
	public static double bwNoiseCV = 0.01;
	public static int bwNoiseAlpha = 0;
	public static double bwNoiseBeta = 0d;
	public static double bwNoiseShape = 0d;
	public static double bwNoiseLocation = 0d;
	public static double bwNoiseShift = 0d;
	public static double bwNoiseMin = 0d;
	public static double bwNoiseMax = 0d;
	public static int bwNoisePopulation = 0;

	// straggler parameters
	//public static double likelihoodOfStraggler =0.015;//0.00625,0.0125,0.01875,0.025
	///public static double stragglerPerformanceCoefficient = 0.5;//0.1

	// the probability for a task to end in failure instead of success once it's
	// execution time has passed
	//public static double likelihoodOfFailure =0.07;//0.00625,0.0125,0.01875,0.025
	//public static double runtimeFactorInCaseOfFailure = 20d;

	// the coefficient of variation for information that is typically not
	// available in real-world scenarios
	// e.g., Task progress scores, HEFT runtime estimates
	public static double distortionCV = 0d;

	public static long seed = 0;
	public static Random numGen = new Random(seed);

	public static ContinuousDistribution getDistribution(
			Distribution distribution, double mean, int alpha, double beta,
			double dev, double shape, double location, double shift,
			double min, double max, int population) {
		ContinuousDistribution dist = null;
		switch (distribution) {
		case EXPONENTIAL:
			dist = new ExponentialDistr(mean);
			break;
		case GAMMA:
			dist = new GammaDistr(numGen, alpha, beta);
			break;
		case LOGNORMAL:
			dist = new LognormalDistr(numGen, mean, dev);
			break;
		case LOMAX:
			dist = new LomaxDistribution(numGen, shape, location, shift);
			break;
		case NORMAL:
			dist = new NormalDistribution(numGen, mean, dev);
			break;
		case PARETO:
			dist = new ParetoDistr(numGen, shape, location);
			break;
		case UNIFORM:
			dist = new UniformDistr(min, max);
			break;
		case WEIBULL:
			dist = new WeibullDistr(numGen, alpha, beta);
			break;
		case ZIPF:
			dist = new ZipfDistr(shape, population);
			break;
		}
		return dist;
	}

	public static void parseParameters(String[] args) {

		for (int i = 0; i < args.length; i++) {
			if (args[i].compareTo("-" + "outputVmPerformanceLogs") == 0) {
				outputVmPerformanceLogs = Boolean.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "scheduler") == 0) {
				scheduler = Scheduler.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "numberOfRuns") == 0) {
				numberOfRuns = Integer.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "heterogeneityCV") == 0) {
				cpuHeterogeneityCV = ioHeterogeneityCV = bwHeterogeneityCV = Double
						.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuHeterogeneityCV") == 0) {
				cpuHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioHeterogeneityCV") == 0) {
				ioHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwHeterogeneityCV") == 0) {
				bwHeterogeneityCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "baselineChangesPerHour") == 0) {
				cpuBaselineChangesPerHour = ioBaselineChangesPerHour = bwBaselineChangesPerHour = Double
						.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "baselineCV") == 0) {
				cpuDynamicsCV = ioDynamicsCV = bwDynamicsCV = Double
						.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuDynamicsCV") == 0) {
				cpuDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioDynamicsCV") == 0) {
				ioDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwDynamicsCV") == 0) {
				bwDynamicsCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "noiseCV") == 0) {
				cpuNoiseCV = ioNoiseCV = bwNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "cpuNoiseCV") == 0) {
				cpuNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "ioNoiseCV") == 0) {
				ioNoiseCV = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "bwNoiseCV") == 0) {
				bwNoiseCV = Double.valueOf(args[++i]);
			}
		/*	if (args[i].compareTo("-" + "likelihoodOfStraggler") == 0) {
				likelihoodOfStraggler = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "stragglerPerformanceCoefficient") == 0) {
				stragglerPerformanceCoefficient = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "likelihoodOfFailure") == 0) {
				likelihoodOfFailure = Double.valueOf(args[++i]);
			}
			if (args[i].compareTo("-" + "runtimeFactorInCaseOfFailure") == 0) {
				runtimeFactorInCaseOfFailure = Double.valueOf(args[++i]);
			}*/
		}

	}

}
