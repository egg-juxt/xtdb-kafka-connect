package xtdb.kafka.connect

import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import java.util.Properties

@Suppress("MemberVisibilityCanBePrivate")
class XtdbSinkConnector : SinkConnector() {

    lateinit var xtConfig: XtdbSinkConfig

    private val cachedVersion: String? by lazy {
        val props = Properties()
        XtdbSinkConnector::class.java.classLoader.getResourceAsStream("version.properties")?.use {
            props.load(it)
        } ?: return@lazy null
        props.getProperty("version")?.takeIf { it.isNotBlank() }
    }

    override fun version(): String = cachedVersion ?: "<unknown>";

    override fun taskClass(): Class<out Task> = XtdbSinkTask::class.java

    override fun config() = CONFIG_DEF

    override fun start(props: Map<String, String>) {
        xtConfig = XtdbSinkConfig.parse(props)
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String?>> =
        xtConfig.taskConfig.let { config -> List(maxTasks) { config } }

    override fun stop() {
    }
}
