package xtdb.kafka.connect

import clojure.java.api.Clojure
import clojure.lang.IFn
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.LoggerFactory

private val LOGGER = LoggerFactory.getLogger(XtdbSinkTask::class.java)

class XtdbSinkTask : SinkTask(), AutoCloseable {
    companion object {
        private val submitSinkRecords: IFn

        init {
            Clojure.`var`("clojure.core/require").invoke(Clojure.read("xtdb.kafka.connect"))
            submitSinkRecords = Clojure.`var`("xtdb.kafka.connect/submit-sink-records")
        }
    }

    private lateinit var config: XtdbSinkConfig
    private lateinit var dataSource: HikariDataSource;

    override fun start(props: Map<String, String>) {
        config = XtdbSinkConfig.parse(props)

        dataSource = HikariDataSource().apply {
            jdbcUrl = config.connectionUrl
            poolName = "XtdbSinkTask-single-connection"
            maximumPoolSize = 1
            minimumIdle = 0
            idleTimeout = 10000
        };

        try {
            dataSource.getConnection().use {
                LOGGER.info("XTDB connection check succeeded")
            }
        } catch (e: Exception) {
            LOGGER.warn("XTDB connection check failed")
        }
    }

    override fun put(sinkRecords: Collection<SinkRecord>) {
        submitSinkRecords(dataSource, config, sinkRecords)
    }

    override fun version(): String = XtdbSinkConnector().version()

    override fun flush(offsets: Map<TopicPartition, OffsetAndMetadata>) {
    }

    override fun stop() {
        dataSource.close()
    }

    override fun close() {
        stop()
    }
}
