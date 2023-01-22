package bigdataman.gudini.ProgettoGudini;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

public class Operations {
	public Operations() {
	};

	public void showDataset(JavaRDD<String> rdd, int k, boolean random) {
		if (random == true) {
			System.out.println("\nHere you can see " + k + " random elements of the dataset:\n");
			for (String line : rdd.takeSample(false, k))
				System.out.println(line);
		} else {
			System.out.println("\nHere you can see the first " + k + " elements of the dataset:\n");
			for (String line : rdd.take(k))
				System.out.println(line);
		}
	}

	public void showDataset(JavaPairRDD<String, Double> pairdd, int j, boolean random) {
		if (random == true) {
			System.out.println("\nHere you can see " + j + " random elements of the dataset:\n");
			for (Tuple2<String, Double> tuple : pairdd.takeSample(false, j))
				System.out
						.println(tuple._1() + ": " + String.format("%.3f", tuple._2()) + " Registered covid-19 cases");
		} else {
			System.out.println("\nHere you can see the first " + j + " elements of the dataset:\n");
			for (Tuple2<String, Double> tuple : pairdd.take(j))
				System.out.println(tuple._1() + ": " + String.format("%.3f", tuple._2()) + " Registered covid-19 cases");
		}
	}

	public JavaPairRDD<String, Double> sortPairRDD(JavaPairRDD<String, Double> pairdd, boolean key, boolean ascending) {
		if (key == true)
			return pairdd.sortByKey(ascending);

		return pairdd.mapToPair(t -> new Tuple2<Double, String>(t._2(), t._1())).sortByKey(ascending)
				.mapToPair(t -> new Tuple2<String, Double>(t._2(), t._1()));
	}

	public void saveResults(JavaRDD<String> rdd, String folder_name) {
		rdd.saveAsTextFile(folder_name);
	}

	public void saveResults(JavaPairRDD<String, Double> pairdd, String folder_name) {
		pairdd.saveAsTextFile(folder_name);
	}

	public JavaPairRDD<String, Double> totalCasesByCountry(JavaRDD<String> rdd) {
		Function<String, Tuple2<String, Double>> mymapper = x -> (new Tuple2<String, Double>(x.split(",")[2],
				Double.parseDouble(x.split(",")[4])));

		return rdd.map(mymapper).mapToPair(tuple -> new Tuple2<String, Double>(tuple._1(), tuple._2()))
				.reduceByKey((x, y) -> x + y);
	}

	public void searchByCountry(JavaPairRDD<String, Double> pairdd, String country) {
		pairdd.filter(tuple -> tuple._1().equals(country))
				.foreach(tuple -> System.out.println("\nCountry: " + tuple._1() + "\n"
						+ "Number of COVID-19 registered cases " + "from December 2019 to November 2020" + ": "
						+ String.format("%.3f", tuple._2()) + "\n"));
	}

	public void totalRegisteredCases(JavaPairRDD<String, Double> pairdd) {
		JavaRDD<Double> values = pairdd.values();
		Double totalcases = values.reduce((x, y) -> x + y);
		System.out.println("Total number of COVID-19 registered cases from December 2019 to November 2020: "
				+ String.format("%.3f", totalcases) + "\n");
	}

	public void findMaxValue(JavaPairRDD<String, Double> pairdd) {
		JavaRDD<Double> values = pairdd.values();
		Double maxvalue = values.reduce((x, y) -> Math.max(x, y));
		JavaPairRDD<String, Double> max_value_country = pairdd.filter(tuple -> tuple._2().equals(maxvalue));
		max_value_country.foreach(tuple -> System.out
				.println("Most affected country:\n" + tuple._1() + ": " + String.format("%.3f", tuple._2()) + "\n"));
	}

	public JavaPairRDD<String, Double> computePercentage(JavaRDD<String> rdd) {
		Function<String, Tuple4<String, String, Double, Double>> mymapper2 = x -> (new Tuple4<String, String, Double, Double>(
				x.split(",")[2], (x.split(",")[3].split("-")[1] + "-" + x.split(",")[3].split("-")[0]),
				Double.parseDouble(x.split(",")[4]), Double.parseDouble(x.split(",")[6])));
		JavaRDD<Tuple4<String, String, Double, Double>> country_date_cases_population = rdd.map(mymapper2);
		JavaPairRDD<String, Double> countrymonthyear_cases = country_date_cases_population
				.mapToPair(tuple -> new Tuple2<String, Double>(tuple._1() + "_" + tuple._2(), tuple._3()))
				.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Double> countrymonthyear_population = country_date_cases_population
				.mapToPair(tuple -> new Tuple2<String, Double>(tuple._1() + "_" + tuple._2(), tuple._4())).distinct();
		JavaPairRDD<String, Double> union_rdd = countrymonthyear_cases.union(countrymonthyear_population);
		Function2<Double, Double, Double> myfunc = (x, y) -> Math.min(x, y) / Math.max(x, y);
		JavaPairRDD<String, Double> rdd_results = union_rdd.reduceByKey(myfunc)
				.mapToPair(t -> new Tuple2<String, Double>(t._1(), t._2() * 100));
		return rdd_results;
	}

	public void showPercentages(JavaPairRDD<String, Double> pairdd, int w) {
		System.out.println("\nHere you can see " + w + " random elements of the dataset:\n");
		for (Tuple2<String, Double> t : pairdd.takeSample(false, w))
			System.out.println(t._1() + ": " + String.format("%.3f", t._2()) + "%"
					+ " of new covid-19 cases with respect to the number of inhabitants");
	}

	public void findCountryPercentage(JavaPairRDD<String, Double> pairdd, String country) {
		System.out.println("\nHere you can see the number of new " + "COVID-19 cases as percentage of " + country
				+ " population by month");
		pairdd.filter(t -> t._1().startsWith(country))
				.foreach(t -> System.out.println(t._1() + ":" + String.format("%.3f", t._2()) + "%"));
	}
}