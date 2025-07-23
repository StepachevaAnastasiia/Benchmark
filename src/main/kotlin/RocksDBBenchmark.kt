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
@Measurement(iterations = 15)
@State(Scope.Thread)
class RocksDBBenchmark {
    companion object {
        val numberOfRecords = 1_000_000
        val chunk = 100_000
        val usersDBPath = "/tmp/rocksdb_users"
        val ordersDBPath = "/tmp/rocksdb_orders"
    }

    @Param("1.0", "0.5", "0.0")
    var selectivity: Double = 1.0

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
        val userIds = (1..numberOfRecords).toList()
        for (userId in userIds) {
            val userData = "$userId|user_$userId|user_$userId@example.com"
            usersBatch.put(userId.toString().toByteArray(), userData.toByteArray())

            if (userId % chunk == 0) {
                usersDB.write(batchOptions, usersBatch)
                usersBatch.clear()
            }
        }
        usersBatch.close()

        val ordersDB = RocksDB.open(rocksDbOptions, ordersDBPath)

        val numMatchingOrders = (numberOfRecords * selectivity).toInt()
        val numNonMatchingOrders = numberOfRecords - numMatchingOrders

        val selectiveUserIds = userIds.shuffled().take(numMatchingOrders)
        val nonExistingUserIds = (1..numNonMatchingOrders).map { Int.MAX_VALUE - it }

        val allUserIdsForOrders = selectiveUserIds + nonExistingUserIds
        val shuffledUserIds = allUserIdsForOrders.shuffled()

        for (i in 1..numberOfRecords / chunk) {
            val ordersBatch = WriteBatch()

            val orders = (1..chunk).map { n ->
                val id = shuffledUserIds[(i - 1) * chunk + n - 1]
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
            ordersBatch.close()
        }

        ordersDB.newIterator().use { rdbEntries ->
            rdbEntries.seekToFirst()

            while (rdbEntries.isValid) {
                val entry = rdbEntries.value()
                val record = String(entry)

                bh.consume(record)
                rdbEntries.next()
            }
        }

        rocksDbOptions.close()
        batchOptions.close()
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