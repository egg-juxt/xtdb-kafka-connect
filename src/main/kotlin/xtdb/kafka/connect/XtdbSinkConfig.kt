package xtdb.kafka.connect

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.*
import org.apache.kafka.common.config.ConfigDef.Type.INT
import org.apache.kafka.common.config.ConfigDef.Type.STRING
import org.apache.kafka.common.config.ConfigException

internal const val CONNECTION_URL_CONFIG: String = "connection.url"
internal const val INSERT_MODE_CONFIG: String = "insert.mode"
internal const val ID_MODE_CONFIG: String = "id.mode"
internal const val TABLE_NAME_FORMAT_CONFIG: String = "table.name.format"
internal const val MAX_CONCURRENT = "max.concurrent"
internal const val MAX_RETRIES = "max.retries"
internal const val RETRY_BACKOFF_MS = "retry.backoff.ms"

private class EnumValidator(private val validValues: Set<String>) : Validator {
    override fun ensureValid(key: String, value: Any?) {
        if (value != null && !validValues.contains(value)) {
            throw ConfigException(key, value, "Invalid enumerator")
        }
    }

    override fun toString(): String = validValues.toString()
}

internal val CONFIG_DEF: ConfigDef = ConfigDef()
    .define(
        CONNECTION_URL_CONFIG, STRING, NO_DEFAULT_VALUE, Importance.HIGH,
        "JDBC URL of XTDB server."
    )
    .define(
        INSERT_MODE_CONFIG, STRING, "insert",
        EnumValidator(setOf("insert", "patch")), Importance.HIGH,
        "The insertion mode to use. Supported modes are ``insert`` and ``patch``."
    )
    .define(
        ID_MODE_CONFIG, STRING, "record_value",
        EnumValidator(setOf("record_key", "record_value")), Importance.HIGH,
        "Where to get the `_id` from. Supported modes are `record_key` and `record_value`."
    )
    .define(
        TABLE_NAME_FORMAT_CONFIG, STRING, "\${topic}", Importance.MEDIUM,
        "A format string for the destination table name, which may contain `\${topic}` as a placeholder for the originating topic name."
    )
    .define(
        MAX_CONCURRENT, INT, 1, Importance.MEDIUM,
        "Number of concurrent write operations. Applies only if `insert.mode` = `patch`."
    )
    .define(
        MAX_RETRIES, INT, 2, Importance.LOW,
        "The maximum number of times to retry on errors before failing the task."
    )
    .define(
        RETRY_BACKOFF_MS, INT, 10000, Importance.LOW,
        "The time in milliseconds to wait following an error before a retry attempt is made."
    )

data class XtdbSinkConfig(
    val connectionUrl: String,
    val insertMode: String,
    val idMode: String,
    val tableNameFormat: String,
    val maxConcurrent: Int,
    val maxRetries: Int,
    val retryBackoffMs: Int,
) {
    companion object {
        @JvmStatic
        fun parse(props: Map<String, String>): XtdbSinkConfig {
            val parsedConfig = AbstractConfig(CONFIG_DEF, props)

            return XtdbSinkConfig(
                connectionUrl = parsedConfig.getString(CONNECTION_URL_CONFIG),
                insertMode = parsedConfig.getString(INSERT_MODE_CONFIG),
                idMode = parsedConfig.getString(ID_MODE_CONFIG),
                tableNameFormat = parsedConfig.getString(TABLE_NAME_FORMAT_CONFIG),
                maxConcurrent = parsedConfig.getInt(MAX_CONCURRENT),
                maxRetries = parsedConfig.getInt(MAX_RETRIES),
                retryBackoffMs = parsedConfig.getInt(RETRY_BACKOFF_MS),
            )
        }
    }
}
