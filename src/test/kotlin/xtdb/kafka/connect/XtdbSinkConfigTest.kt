package xtdb.kafka.connect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class XtdbSinkConfigTest {
    @Test
    fun `parses simple mapping`() {
        assertEquals(
            mapOf("topic1" to "table1", "topic2" to "table2"),
            parseToMap("topic1:table1, ,topic2:table2,")
        )
    }

    @Test
    fun `empty input returns empty map`() {
        assertTrue(parseToMap("   ").isEmpty())
    }

    val illegalArg = IllegalArgumentException::class.java

    @Test
    fun `throws on missing colon`() {
        assertThrows(illegalArg) { parseToMap("topic1table1,topic2:table2") }
    }

    @Test
    fun `throws on empty key`() {
        assertThrows(illegalArg) { parseToMap(":table1,topic2:table2") }
    }

    @Test
    fun `throws on empty value`() {
        assertThrows(illegalArg) { parseToMap("topic1:,topic2:table2") }
    }

    @Test
    fun `throws on duplicate keys`() {
        assertThrows(illegalArg) { parseToMap("topic1:table1,topic1:tableX") }
    }
}