package com.sparkTutorial.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Demo {
	public static void main(String[] args) {
		SparkConf config= new SparkConf().setAppName("demo").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> lines= context.textFile("in/numbers.txt");
		JavaRDD<String> words= lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaRDD<Integer> numbers= words.map(word -> Integer.parseInt(word));
		JavaRDD<Integer> evenNumbers=numbers.filter(number-> number%2==0);
		System.out.println(evenNumbers.collect());


	}
}
