import statistics
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.table.udf import AggregateFunction, udaf


class MedianHumidity(AggregateFunction):
    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, value):
        if value is not None:
            accumulator.append(value)

    def retract(self, accumulator, value):
        if value is not None and value in accumulator:
            accumulator.remove(value)

    def get_value(self, accumulator):
        if not accumulator:
            return None
        return statistics.median(accumulator)

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            accumulator.extend(acc)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.3.0-1.20.jar",
        "file:///opt/flink/lib/postgresql-42.6.0.jar"
    )

    t_env = StreamTableEnvironment.create(env)

    row_type_info = Types.ROW_NAMED(
        ["device_id", "device_type_id", "temperature", "humidity", "event_time"],
        [
            Types.STRING(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
        ]
    )

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(row_type_info)
        .build()
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("iot-events")
        .set_group_id("flink-iot-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(deserialization_schema)
        .build()
    )

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaIotSource")

    kafka_table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
        .column("device_id", DataTypes.STRING())
        .column("device_type_id", DataTypes.INT())
        .column("temperature", DataTypes.DOUBLE())
        .column("humidity", DataTypes.DOUBLE())
        .column("event_time", DataTypes.STRING())
        .column_by_expression("ts", "TO_TIMESTAMP(LEFT(event_time, 19), 'yyyy-MM-dd''T''HH:mm:ss')")
        .column_by_expression("proc_time", "PROCTIME()")
        .watermark("ts", "ts - INTERVAL '5' SECOND")
        .build()
    )

    t_env.create_temporary_view("iot_events", kafka_table)

    t_env.execute_sql("""
        CREATE TABLE iot_device_types (
            id INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/iotdb',
            'table-name' = 'iot_device_types',
            'username' = 'iotuser',
            'password' = 'iotpass',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    median_humidity_udaf = udaf(
        MedianHumidity(),
        result_type=DataTypes.DOUBLE(),
        accumulator_type=DataTypes.ARRAY(DataTypes.DOUBLE()),
    )
    t_env.create_temporary_function("MEDIAN_HUMIDITY", median_humidity_udaf)

    t_env.execute_sql("""
        CREATE TABLE iot_results (
            time_str STRING,
            device_type STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iot-results',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    result_table = t_env.sql_query("""
        SELECT
            DATE_FORMAT(TUMBLE_END(e.ts, INTERVAL '1' MINUTE), 'HH:mm') AS time_str,
            d.type_name AS device_type,
            ROUND(AVG(e.temperature), 2) AS avg_temperature,
            ROUND(MEDIAN_HUMIDITY(e.humidity), 2) AS median_humidity
        FROM iot_events AS e
        JOIN iot_device_types FOR SYSTEM_TIME AS OF e.proc_time AS d ON e.device_type_id = d.id
        GROUP BY
            TUMBLE(e.ts, INTERVAL '1' MINUTE),
            d.type_name
    """)

    result_table.execute_insert("iot_results").wait()


if __name__ == "__main__":
    main()
