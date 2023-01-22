package bigdataman.gudini.ProgettoGudini;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
	public static void main(String[] args) {
		JavaSparkContext ctx = null;
		try {
			Logger.getLogger("org.apache").setLevel(Level.WARN);
			SparkConf conf = new SparkConf().setAppName("Data Exploration").setMaster("local[*]");
			ctx = new JavaSparkContext(conf);
			System.out.println("\nConnection established.. \n");
			JavaRDD<String> raw_dataset_rdd = ctx.textFile("new_dataset_covid.csv");
			String header = raw_dataset_rdd.first();
			JavaRDD<String> dataset_rdd = raw_dataset_rdd.filter(s -> !(s.equals(header)));
			Operations op = new Operations();
			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Please type any integer != 9 and press the [ENTER] key to explore the dataset..");
			while (Integer.parseInt(input.readLine()) != 9) {
				System.out.println("\nHi, What you are interested in? (1 or 2 or 9)\n" + "1: "
						+ "Details about covid-19 cases percentage " + "by country and by month\n"
						+ "2: General details about covid-19 cases by country\n" + "9: QUIT\n");
				int command = Integer.parseInt(input.readLine());
				if (command == 9) {
					break;
				} else {
					switch (command) {
					case 1:
						JavaPairRDD<String, Double> percentages = op.computePercentage(dataset_rdd);
						JavaPairRDD<String, Double> sorted_percentages = op.sortPairRDD(percentages, true, true);
						System.out.println("\nPlease choose one of the following options (1 or 2):\n" + "1: "
								+ "Details about a specific country\n" + "2: Take a look at the dataset\n");
						int command1 = Integer.parseInt(input.readLine());
						if (command1 == 1) {
							System.out.println("\nPlease type the name of a country\n" + "For example:\n" + "Italy\n"
									+ "United States\n" + "Ireland\n" + "...\n");
							String countryname = input.readLine();
							op.findCountryPercentage(sorted_percentages, countryname);
							System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
						} else if (command1 == 2) {
							System.out.println("\nPlease pick an integer x, 0 < x <= " + percentages.count());
							int x = Integer.parseInt(input.readLine());
							op.showPercentages(percentages, x);
							System.out.println("\nDo you want to save the dataset? (Y or N)\n");
							String answer = input.readLine();
							if (answer.equals("Y")) {
								op.saveResults(percentages, "OutputPercentages");
								System.out.println("\nOk, correctly saved in the OutputPercentages folder\n");
								System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
							} else if (answer.equals("N")) {
								System.out.println("\nOk, let's go on\n");
								System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
							}
						}
						break;
					case 2:
						JavaPairRDD<String, Double> totcbc = op.totalCasesByCountry(dataset_rdd);
						System.out.println("\nPlease choose one of the following options (1 or 2 or 3 or 4):\n"
								+ "1: Take a look at the dataset\n" + "2: Look for a specific country\n"
								+ "3: Look for the most affected country\n" + "4: Total number of registered cases\n");
						int command2 = Integer.parseInt(input.readLine());
						if (command2 == 1) {
							System.out.println("Please pick an integer x, 0 < x <= " + totcbc.count());
							int y = Integer.parseInt(input.readLine());
							System.out.println("\nDo you want to sort the dataset before? (Y or N)\n");
							String answer = input.readLine();
							if (answer.equals("N")) {
								op.showDataset(totcbc, y, true);
								System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
							} else if (answer.equals("Y")) {
								System.out.println("\nChoose one of the following type of sorting\n"
										+ "1: Alphabetical and ascending\n" + "2: Alphabetical and descending\n"
										+ "3: From most affected country to least affected country\n"
										+ "4: From least affected country to most affected country\n");
								int o = Integer.parseInt(input.readLine());
								if (o == 1) {
									JavaPairRDD<String, Double> sorted_alpha_asc = op.sortPairRDD(totcbc, true, true);
									op.showDataset(sorted_alpha_asc, y, false);
									System.out.println("\nDo you want to save the dataset? (Y or N)\n");
									String answer1 = input.readLine();
									if (answer1.equals("Y")) {
										op.saveResults(sorted_alpha_asc, "Output");
										System.out.println("\nOk, correctly saved in the Output folder\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									} else if (answer1.equals("N")) {
										System.out.println("\nOk, let's go on\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									}
								} else if (o == 2) {
									JavaPairRDD<String, Double> sorted_alpha_desc = op.sortPairRDD(totcbc, true, false);
									op.showDataset(sorted_alpha_desc, y, false);
									System.out.println("\nDo you want to save the dataset? (Y or N)\n");
									String answer2 = input.readLine();
									if (answer2.equals("Y")) {
										op.saveResults(sorted_alpha_desc, "Output1");
										System.out.println("\nOk, correctly saved in the Output1 folder\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									} else if (answer2.equals("N")) {
										System.out.println("\nOk, let's go on\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									}
								} else if (o == 3) {
									JavaPairRDD<String, Double> sorted_most_least = op.sortPairRDD(totcbc, false,
											false);
									op.showDataset(sorted_most_least, y, false);
									System.out.println("\nDo you want to save the dataset? (Y or N)\n");
									String answer3 = input.readLine();
									if (answer3.equals("Y")) {
										op.saveResults(sorted_most_least, "Output2");
										System.out.println("\nOk, correctly saved in the Output2 folder\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									} else if (answer3.equals("N")) {
										System.out.println("\nOk, let's go on\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									}
								} else if (o == 4) {
									JavaPairRDD<String, Double> sorted_least_most = op.sortPairRDD(totcbc, false, true);
									op.showDataset(sorted_least_most, y, false);
									System.out.println("\nDo you want to save the dataset? (Y or N)\n");
									String answer4 = input.readLine();
									if (answer4.equals("Y")) {
										op.saveResults(sorted_least_most, "Output3");
										System.out.println("\nOk, correctly saved in the Output3 folder\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									} else if (answer4.equals("N")) {
										System.out.println("\nOk, let's go on\n");
										System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
									}
								}
							}
						} else if (command2 == 2) {
							System.out.println("\nPlease type the name of a country\n" + "For example:\n" + "Italy\n"
									+ "United States\n" + "Ireland\n" + "...\n");
							String country = input.readLine();
							op.searchByCountry(totcbc, country);
							System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
						} else if (command2 == 3) {
							op.findMaxValue(totcbc);
							System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
						} else if (command2 == 4) {
							op.totalRegisteredCases(totcbc);
							System.out.println("\nType any integer (9 to QUIT) and press the [ENTER] key");
						}
						break;
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			if (ctx != null)
				ctx.close();
		}
	}
}