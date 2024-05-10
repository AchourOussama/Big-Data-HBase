/*
package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class HbaseSparkProcess {
    //pour transformer chaque ligne HBase (tableau d'octets) en valeur double représentant le montant
    public static final class AmountFunction implements Function<Tuple2<ImmutableBytesWritable, Result>, Double> {
        @Override
        public Double call(Tuple2<ImmutableBytesWritable, Result> row) throws Exception {
            // Extraction de la valeur de la colonne 'amount' de la famille de colonnes 'cf'
            byte[] amountBytes = row._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("amount"));
            if (amountBytes != null) {
                return Double.parseDouble(Bytes.toString(amountBytes));
            }
            return 0.0;
        }
    }

    public void createHbaseTable() {
        // Config HBase
        Configuration config = HBaseConfiguration.create();

        // Config Spark et création du contexte Spark
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseIntegration").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Définition de la table HBase à utiliser
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Création d'un RDD(Resilient Distributed Dataset) représentant la table HBase
        //ImmutableBytesWritable : la clé de la ligne HBase
        //Result: value de la ligne
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                config,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        // Comptage du nombre d'éléments dans le RDD
        long count = hBaseRDD.count(); //count du total des enregistrements
        System.out.println("Nombre d'enregistrements: " + count);

        // extraire les valeurs des montants
        JavaRDD<Double> amounts = hBaseRDD.map(new AmountFunction());

        // Calcul du total des ventes en additionnant les montants
        double totalSales = amounts.reduce((a, b) -> a + b);
        System.out.println("Total des ventes: " + totalSales);

        // Fermeture du contexte Spark
        jsc.close();
    }

    public static void main(String[] args) {
        HbaseSparkProcess admin = new HbaseSparkProcess();
        admin.createHbaseTable();
    }
}*/
