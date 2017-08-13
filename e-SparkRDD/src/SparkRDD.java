
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
 
public class SparkRDD {
  public static void main(String[] args) throws Exception {
	JavaSparkContext sc = new JavaSparkContext("local", "SparkRDD", System.getenv("SPARK_HOME"), System.getenv("JARS"));
	
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    
	JavaRDD<Integer> squared = rdd.map(
      new Function<Integer, Integer>() { public Integer call(Integer x) { return x*x;}});
    
	JavaRDD<Integer> result = squared.filter(
      new Function<Integer, Boolean>() { public Boolean call(Integer x) { return x != 1; }});
    
    System.out.println(StringUtils.join(result.collect(), ","));
    
    sc.close();
  }
}
