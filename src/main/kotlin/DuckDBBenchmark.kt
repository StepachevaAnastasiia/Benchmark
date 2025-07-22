package org.example

import org.duckdb.DuckDBConnection
import java.sql.DriverManager
import java.sql.Statement
import java.time.LocalDateTime
import kotlin.random.Random
import kotlinx.benchmark.*

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(BenchmarkTimeUnit.SECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Thread)
class DuckDBBenchmark {
    companion object {
        val numberOfRecords = 100000
        val chunk = 10000
    }

    @Benchmark
    fun duckDBTest(bh: Blackhole) {
        val connection = DriverManager.getConnection("jdbc:duckdb:mydatabase.db") as DuckDBConnection
        val stmt = connection.createStatement()

        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS orders")
            stmt.executeUpdate("DROP TABLE IF EXISTS users")

            createTables(stmt)

            insertData(connection)

            val query = """
        SELECT *
        FROM orders o
        SEMI JOIN users u ON u.user_id = o.user_id
    """

            val rs = stmt.executeQuery(query)
            val meta = rs.metaData

            while (rs.next()) {
                bh.consume(rs.getObject(1))
            }
        } finally {
            stmt.close()
            connection.close()
        }
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
}