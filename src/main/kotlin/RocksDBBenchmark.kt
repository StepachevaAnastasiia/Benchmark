package org.example

import org.rocksdb.*
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDateTime
import kotlin.random.Random
import kotlinx.benchmark.*

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(BenchmarkTimeUnit.SECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Thread)
class RocksDBBenchmark {
    companion object {
        val numberOfRecords = 100000
        val chunk = 10000
        val usersDBPath = "/tmp/rocksdb_users"
        val ordersDBPath = "/tmp/rocksdb_orders"
    }

    @TearDown
    fun cleanup() {
        deleteDirectory(usersDBPath)
        deleteDirectory(ordersDBPath)
    }

    @Benchmark
    fun rocksDBTest(bh: Blackhole) {

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

                bh.consume(record)

                rdbEntries.next()
            } while (rdbEntries.isValid)
        }
        ordersBatch.close()
        usersDB.close()
        ordersDB.close()

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