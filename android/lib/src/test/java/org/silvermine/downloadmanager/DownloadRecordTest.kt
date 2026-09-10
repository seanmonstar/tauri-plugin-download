package org.silvermine.downloadmanager

import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlinx.serialization.SerializationException

class DownloadRecordTest {

   private fun sampleRecord(): DownloadRecord = DownloadRecord(
      url = "http://example.com/file.mp4",
      path = "/tmp/file.mp4",
      receivedBytes = 0L,
      totalBytes = null,
      status = DownloadStatus.Idle,
   )

   private val json = Json { encodeDefaults = true }

   // The emit-only DownloadItem must spell out every key regardless of the Json
   // config, so its serialization is asserted through a default encoder.
   private val defaultJson = Json

   // -- Mutation --

   @Test
   fun `withBytes sets byte counts and leaves the rest alone`() {
      val record = sampleRecord()
      val updated = record.withBytes(500L, 1000L)

      assertEquals(500L, updated.receivedBytes)
      assertEquals(1000L, updated.totalBytes)
      assertEquals(DownloadStatus.Idle, updated.status)
      assertEquals(record.url, updated.url)
      assertEquals(record.path, updated.path)
   }

   @Test
   fun `withStatus preserves byte counts`() {
      val record = sampleRecord().withBytes(500L, 1000L)

      val paused = record.withStatus(DownloadStatus.Paused)
      assertEquals(500L, paused.receivedBytes)
      assertEquals(1000L, paused.totalBytes)
      assertEquals(DownloadStatus.Paused, paused.status)

      // Status transitions preserve the factual byte counts, including completion.
      val completed = record.withStatus(DownloadStatus.Completed)
      assertEquals(500L, completed.receivedBytes)
      assertEquals(1000L, completed.totalBytes)
      assertEquals(DownloadStatus.Completed, completed.status)
      assertEquals(100.0, completed.toItem().progress, 0.0)
   }

   @Test
   fun `withStatus completed with unknown size preserves receivedBytes`() {
      val completed = sampleRecord()
         .withBytes(5000L, null)
         .withStatus(DownloadStatus.Completed)

      assertEquals(5000L, completed.receivedBytes)
      assertNull(completed.totalBytes)
   }

   // -- Progress derivation --

   @Test
   fun `toItem with known size`() {
      val item = sampleRecord().withBytes(500L, 1000L).toItem()

      assertEquals(50.0, item.progress, 0.0)
      assertEquals(500L, item.receivedBytes)
      assertEquals(1000L, item.totalBytes)
   }

   @Test
   fun `toItem clamps progress to 100 percent`() {
      val item = sampleRecord().withBytes(1500L, 1000L).toItem()

      assertEquals(100.0, item.progress, 0.0)
   }

   @Test
   fun `toItem with unknown size`() {
      val record = sampleRecord()
      assertEquals(0.0, record.toItem().progress, 0.0)

      // Completed with unknown size still reports 100%.
      val completed = record.withStatus(DownloadStatus.Completed)
      assertEquals(100.0, completed.toItem().progress, 0.0)
   }

   @Test
   fun `toItem with zero total does not divide by zero`() {
      val item = sampleRecord().withBytes(0L, 0L).toItem()

      assertEquals(0.0, item.progress, 0.0)
   }

   // -- Serialization --

   @Test
   fun `validator stays internal to the persisted record`() {
      val record = sampleRecord().copy(validator = ResumeValidator.ETag("\"v1\""))
      val stored = defaultJson.encodeToString(DownloadRecord.serializer(), record)
      val emitted = defaultJson.encodeToString(DownloadItem.serializer(), record.toItem())

      assertTrue(stored.contains("\"validator\""))
      assertFalse(emitted.contains("validator"))
      assertFalse(emitted.contains("v1"))
   }

   @Test
   fun `item serializes totalBytes as explicit null`() {
      // The key is always present, so the payload matches the frontend contract
      // without relying on the TypeScript layer to coalesce a missing key.
      val encoded = defaultJson.encodeToString(DownloadItem.serializer(), sampleRecord().toItem())

      assertTrue(encoded.contains("\"totalBytes\":null"))
   }

   @Test
   fun `item serializes byte fields`() {
      val encoded = defaultJson.encodeToString(
         DownloadItem.serializer(),
         sampleRecord().withBytes(500L, 1000L).toItem(),
      )

      assertTrue(encoded.contains("\"receivedBytes\":500"))
      assertTrue(encoded.contains("\"totalBytes\":1000"))
      assertTrue(encoded.contains("\"progress\":50.0"))
      assertTrue(encoded.contains("\"status\":\"idle\""))
   }

   @Test
   fun `record does not persist progress`() {
      val encoded = json.encodeToString(
         DownloadRecord.serializer(),
         sampleRecord().withBytes(500L, 1000L),
      )

      assertFalse(encoded.contains("progress"))
   }

   @Test
   fun `record decodes persisted wire format`() {
      // A literal payload, not a round trip, which would pass even if the
      // field names drifted on both sides together.
      val encoded = """
         {"url":"http://example.com/f.mp4","path":"/tmp/f.mp4",
         "options":{"allowMetered":true},
         "receivedBytes":500,"totalBytes":1000,"status":"paused"}
      """.trimIndent()

      val record = json.decodeFromString(DownloadRecord.serializer(), encoded)

      assertEquals(500L, record.receivedBytes)
      assertEquals(1000L, record.totalBytes)
      assertEquals(DownloadStatus.Paused, record.status)
      // progress is derived via toItem(), not stored.
      assertEquals(50.0, record.toItem().progress, 0.0)
   }

   @Test
   fun `record round trips through JSON`() {
      val record = sampleRecord().withBytes(500L, 1000L).withStatus(DownloadStatus.Paused)
      val decoded = json.decodeFromString(
         DownloadRecord.serializer(),
         json.encodeToString(DownloadRecord.serializer(), record),
      )

      assertEquals(500L, decoded.receivedBytes)
      assertEquals(1000L, decoded.totalBytes)
      assertEquals(DownloadStatus.Paused, decoded.status)
      // progress is derived via toItem(), not stored.
      assertEquals(50.0, decoded.toItem().progress, 0.0)
   }


   // -- Network policy --

   @Test
   fun `an unstated policy allows metered connections`() {
      // The API default, applied when a caller states no policy. @Required keeps
      // that default off the wire: a persisted record always states the value.
      assertTrue(sampleRecord().options.allowMetered)
      assertTrue(CreateOptions().allowMetered)
   }

   @Test
   fun `record without options fails to decode`() {
      // The policy is decided once, at creation. A record that has lost it cannot
      // be read back as anything trustworthy, so decoding refuses rather than
      // assuming the permissive default.
      val encoded = """
         {"url":"http://example.com/f.mp4","path":"/tmp/f.mp4",
         "receivedBytes":0,"totalBytes":null,"status":"idle"}
      """.trimIndent()

      assertThrows(SerializationException::class.java) {
         json.decodeFromString(DownloadRecord.serializer(), encoded)
      }
   }

   @Test
   fun `record with empty options fails to decode`() {
      val encoded = """
         {"url":"http://example.com/f.mp4","path":"/tmp/f.mp4","options":{},
         "receivedBytes":0,"totalBytes":null,"status":"idle"}
      """.trimIndent()

      assertThrows(SerializationException::class.java) {
         json.decodeFromString(DownloadRecord.serializer(), encoded)
      }
   }

   @Test
   fun `record decodes a restricted policy`() {
      val encoded = """
         {"url":"http://example.com/f.mp4","path":"/tmp/f.mp4",
         "options":{"allowMetered":false},
         "receivedBytes":0,"totalBytes":null,"status":"idle"}
      """.trimIndent()

      val record = json.decodeFromString(DownloadRecord.serializer(), encoded)

      assertFalse(record.options.allowMetered)
   }

   @Test
   fun `record round trips a restricted policy`() {
      val record = sampleRecord().copy(options = CreateOptions(allowMetered = false))
      val decoded = json.decodeFromString(
         DownloadRecord.serializer(),
         json.encodeToString(DownloadRecord.serializer(), record),
      )

      assertFalse(decoded.options.allowMetered)
   }

   @Test
   fun `item carries the resolved policy`() {
      val item = sampleRecord().copy(options = CreateOptions(allowMetered = false)).toItem()

      assertFalse(item.options.allowMetered)

      // The bridge encodes with encodeDefaults = true so allowMetered is explicit
      // inside the options object rather than left to the TypeScript fallback.
      val encoded = json.encodeToString(DownloadItem.serializer(), item)
      assertTrue(encoded.contains("\"options\":{\"allowMetered\":false}"))
   }

   @Test
   fun `item always writes the options key`() {
      // DownloadItem carries no defaults, so every key survives a default encoder.
      val encoded = defaultJson.encodeToString(DownloadItem.serializer(), sampleRecord().toItem())

      assertTrue(encoded.contains("\"options\""))
   }

   // -- Action response --

   @Test
   fun `action response reports expected status`() {
      val item = sampleRecord().toItem()

      // new() reports the status as expected.
      val response = DownloadActionResponse.new(item)
      assertTrue(response.isExpectedStatus)
      assertEquals(DownloadStatus.Idle, response.expectedStatus)

      // A matching expected status is still expected.
      val matching = DownloadActionResponse.withExpectedStatus(item, DownloadStatus.Idle)
      assertTrue(matching.isExpectedStatus)

      // A mismatched expected status is not.
      val mismatched = DownloadActionResponse.withExpectedStatus(item, DownloadStatus.InProgress)
      assertFalse(mismatched.isExpectedStatus)
      assertEquals(DownloadStatus.InProgress, mismatched.expectedStatus)
   }
}
