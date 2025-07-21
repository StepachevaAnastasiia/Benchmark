import org.duckdb.DuckDBConnection
import org.junit.jupiter.api.Test
import org.rocksdb.*
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager
import java.sql.Statement
import java.time.LocalDateTime
import kotlin.random.Random
import kotlin.time.measureTime

class DatabaseBenchmarkTest {
    companion object {
        val numberOfRecords = 100000
        val chunk = 10000
    }

    @Test
    fun duckDBTest() {
        println("=== DuckDB Benchmark ===")
        val totalTime = measureTime {
            val connection = DriverManager.getConnection("jdbc:duckdb:mydatabase.db") as DuckDBConnection
            val stmt = connection.createStatement()

            try {
                stmt.executeUpdate("DROP TABLE IF EXISTS orders")
                stmt.executeUpdate("DROP TABLE IF EXISTS users")

                createTables(stmt)

                insertData(connection)

                executeSemiJoinQuery(stmt)
            } finally {
                stmt.close()
                connection.close()
            }
        }
        println("Total DuckDB execution time: $totalTime")
    }

    @Test
    fun rocksDBTest() {
        println("=== RocksDB Benchmark ===")

        val totalTime = measureTime {

            val usersDBPath = "/tmp/rocksdb_users"
            val ordersDBPath = "/tmp/rocksdb_orders"

            val rocksDbOptions = Options().apply {
                setCreateIfMissing(true)
                setTableFormatConfig(
                    BlockBasedTableConfig().apply {
                        setNoBlockCache(false)
                    }
                )
            }

            val batchOptions = WriteOptions().apply {
                setSync(false)
                setDisableWAL(true)
            }

            val usersDB = RocksDB.open(rocksDbOptions, usersDBPath)
            val usersBatch = WriteBatch()
            for (i in 1..numberOfRecords / chunk) {

                for (j in 1..chunk) {
                    val userId = (i - 1) * chunk + j
                    val username = "user_$userId"
                    val email = "user_$userId@example.com"

                    val userData = "$userId|$username|$email"

                    usersBatch.put(userId.toString().toByteArray(), userData.toByteArray())
                }

                usersDB.write(batchOptions, usersBatch)
            }
            usersBatch.close()

            val ordersBatch = WriteBatch()
            val ordersDB = RocksDB.open(rocksDbOptions, ordersDBPath)
            for (i in 1..numberOfRecords / chunk) {

                val orders = (1..chunk).map { n ->
                    val id = (i - 1) * chunk + n
                    val timestamp = LocalDateTime.now()
                    val amount = Random.nextDouble(10.0, 500000.0)
                    val orderData = "$id|$id|$timestamp|$amount"

                    id to orderData
                }.toList()

                val existedValues = usersDB
                    .multiGetAsList(orders.map { it.first.toString().toByteArray() })

                for (id in orders.indices) {
                    if (existedValues[id] != null) {
                        ordersBatch.put(
                            id.toString().toByteArray(),
                            orders[id].second.toByteArray()
                        )
                    }
                }

                ordersDB.write(batchOptions, ordersBatch)
            }

            ordersDB.newIterator().use { rdbEntries ->
                rdbEntries.seekToFirst()

                do {
                    val entry = rdbEntries.value()
                    val record = String(entry)

                    println(record)

                    rdbEntries.next()
                } while (rdbEntries.isValid)
            }

            ordersBatch.close()
            usersDB.close()
            ordersDB.close()
            deleteDirectory(usersDBPath)
            deleteDirectory(ordersDBPath)
        }
        println("Total RocksDB execution time: $totalTime")
    }

    fun createTables(stmt: Statement) {

        stmt.execute(
            """
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL
        )
    """
        )

        stmt.execute(
            """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date TIMESTAMP NOT NULL,
            amount DECIMAL(10, 2) NOT NULL
        )
    """
        )
    }

    fun insertData(conn: DuckDBConnection) {

        val usersAppender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "users")
        for (i in 1..numberOfRecords) {
            usersAppender.beginRow()
            usersAppender.append(i)
            usersAppender.append("user$i")
            usersAppender.append("user$i@example.com")
            usersAppender.endRow()

            if (i % chunk == 0) {
                usersAppender.flush()
            }
        }

        usersAppender.flush()
        usersAppender.close()

        val ordersAppender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "orders")
        for (i in 1..numberOfRecords) {
            ordersAppender.beginRow()
            ordersAppender.append(i)
            ordersAppender.append(i)
            ordersAppender.appendLocalDateTime(LocalDateTime.now())
            ordersAppender.append(Random.nextDouble(10.0, 500000.0))
            ordersAppender.endRow()

            if (i % chunk == 0) {
                ordersAppender.flush()
            }
        }

        ordersAppender.flush()
        ordersAppender.close()
    }

    fun executeSemiJoinQuery(stmt: Statement) {
        val query = """
        SELECT *
        FROM orders o
        SEMI JOIN users u ON u.user_id = o.user_id
    """

        val rs = stmt.executeQuery(query)
        val meta = rs.metaData
        val colCount = meta.columnCount

        while (rs.next()) {
            for (i in 1..colCount) {
                print("${meta.getColumnLabel(i)}=${rs.getObject(i)}  ")
            }
            println()
        }

    }

    fun deleteDirectory(pathStr: String) {
        val path = Paths.get(pathStr)
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
        }
    }

}