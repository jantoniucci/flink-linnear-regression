package poc.flink;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/**
 * calcs a simple linnear regression from a file with x|y series of values
 */
public class CalcSimpleLinnearRegressionJob implements ProgramDescription{

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Double, Double, Double>> tuplesXY = env.readCsvFile("file:///Users/javier/Downloads/flink-linnear-regression/linnear-regression/src/main/resources/data.csv")
				.fieldDelimiter(";")
				.lineDelimiter("\n")
				.types(Double.class, Double.class, Double.class);
				   
		DataSet<Tuple3<Double, Double, Integer>> sumAndcounXY = 
				tuplesXY
					.combineGroup(new GroupCombineFunction<Tuple3<Double,Double,Double>, Tuple3<Double,Double,Integer>>() {
						@Override
						public void combine(
								Iterable<Tuple3<Double, Double, Double>> values,
								Collector<Tuple3<Double, Double, Integer>> out)
								throws Exception {
							int count = 0;
							double x = 0;
							double y = 0;
							for (Tuple3<Double, Double, Double> value : values) {
								x+=value.f1;
								y+=value.f2;
								count++;
							}
							out.collect(new Tuple3<Double, Double, Integer>(x, y, count));
						}
					})
					.reduce(new ReduceFunction<Tuple3<Double, Double, Integer>>() {
						@Override
						public Tuple3<Double, Double, Integer> reduce(
								Tuple3<Double, Double, Integer> value1,
								Tuple3<Double, Double, Integer> value2)
								throws Exception {
							return new Tuple3<Double, Double, Integer>(value1.f0+value2.f0, value1.f1+value2.f1, value1.f2+value2.f2);
						}
						
					}
					);

		final Tuple3<Double, Double, Integer> sumAndcounXYresult = sumAndcounXY.collect().get(0);
		final double avgX = sumAndcounXYresult.f0/sumAndcounXYresult.f2;
		final double avgY = sumAndcounXYresult.f1/sumAndcounXYresult.f2;
		
		DataSet<Tuple2<Double, Double>> thetaCalculation = 
				tuplesXY
					.flatMap(new FlatMapFunction<Tuple3<Double, Double, Double>, Tuple2<Double, Double>>() {

						@Override
						public void flatMap(
								Tuple3<Double, Double, Double> value,
								Collector<Tuple2<Double, Double>> out)
								throws Exception {
							Double diffX = value.f1 - avgX;
							Double diffY = value.f2 - avgY;
							Double x = diffX*diffX;
							Double y = diffX*diffY;
							out.collect(new Tuple2<Double, Double>(x, y));
							
						} 
						
					})
					.reduce(new ReduceFunction<Tuple2<Double, Double>>() {

						@Override
						public Tuple2<Double, Double> reduce(
								Tuple2<Double, Double> value1,
								Tuple2<Double, Double> value2) throws Exception {
							return new Tuple2<Double, Double>(value1.f0+value2.f0, value1.f1+value2.f1);
						}
						
					})
					.map(new MapFunction<Tuple2<Double,Double>, Tuple2<Double,Double>>() {

						@Override
						public Tuple2<Double, Double> map(
								Tuple2<Double, Double> value) throws Exception {
							double b1 = value.f1 / value.f0;
							double b0 = avgY-avgX*b1;
							return new Tuple2<Double, Double>(b0, b1);
						}
					});
		
		thetaCalculation.writeAsCsv("file:///Users/javier/Downloads/flink-linnear-regression/linnear-regression/target/result.csv", "\n", ";", WriteMode.OVERWRITE);
		env.execute("Calc Simple Linnear Regression Job");
	}

	
	@Override
	public String getDescription() {
		return "Simple Linnear Regression calculation. Uses data.cvs file and generates result.csv in target directory.";
	}

}
