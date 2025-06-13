import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;

public class BarrierFreeAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BarrierFreeBusAnalysis")
                .enableHiveSupport() // Hive 연결
                .getOrCreate();

        // 한국 데이터 로드
        Dataset<Row> korea = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("hdfs://localhost:9000/user/hduser/project/input/korea_bus.csv");

        // 운영 대수 데이터만 추출
        Dataset<Row> koreaFiltered = korea.filter("type = 'operation'")
                .selectExpr("'Korea' as region", "total_bus", "low_floor_bus", "CAST(rate AS DOUBLE) as rate");

        // 일본 데이터 로드
        Dataset<Row> japan = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("hdfs://localhost:9000/user/hduser/project/input/japan_bus.csv");

        // 일본: 비어있는 rate 제외하고 평균 보급률 계산
        Dataset<Row> japanCleaned = japan
                .filter("rate IS NOT NULL")
                .selectExpr("CAST(rate AS DOUBLE) as rate");

        double japanAvg = japanCleaned.agg(functions.avg("rate")).first().getDouble(0);

        // 일본 데이터 하나의 Row로 만들기
        Dataset<Row> japanSummary = spark.createDataFrame(
                java.util.Collections.singletonList(
                        RowFactory.create("Japan", null, null, japanAvg)
                ),
                new StructType()
                        .add("region", "string")
                        .add("total_bus", "integer")
                        .add("low_floor_bus", "integer")
                        .add("rate", "double")
        );

        // 한국 + 일본 데이터 합치기
        Dataset<Row> finalResult = koreaFiltered.unionByName(japanSummary);

        // 결과 확인
        finalResult.show();

        // Hive에 테이블 저장
        finalResult.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("barrier_free_bus_result");
    }
}
