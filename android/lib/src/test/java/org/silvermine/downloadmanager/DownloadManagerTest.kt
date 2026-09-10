package org.silvermine.downloadmanager

import androidx.work.NetworkType
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.io.File

class DownloadManagerTest {

   private fun inProgressRecord(): DownloadRecord = DownloadRecord(
      url = "http://example.com/file.mp4",
      path = "/tmp/file.mp4",
      receivedBytes = 500L,
      totalBytes = 1000L,
      validator = ResumeValidator.ETag("\"v1\""),
      status = DownloadStatus.InProgress,
   )

   // -- Reconciliation --

   @Test
   fun `record with a temp file reverts to paused at the flushed length`() {
      val reverted = DownloadManager.revertInProgress(inProgressRecord(), tempFileLength = 480L)

      assertEquals(DownloadStatus.Paused, reverted?.status)
      // Only bytes flushed to disk are resumable, so the temp file's length wins
      // over the 500 the last progress tick reported.
      assertEquals(480L, reverted?.receivedBytes)
      assertEquals(1000L, reverted?.totalBytes)
      assertEquals(ResumeValidator.ETag("\"v1\""), reverted?.validator)
   }

   @Test
   fun `record without a temp file reverts to idle at zero`() {
      val reverted = DownloadManager.revertInProgress(inProgressRecord(), tempFileLength = null)

      assertEquals(DownloadStatus.Idle, reverted?.status)
      // Nothing to resume from, so the download restarts from scratch.
      assertEquals(0L, reverted?.receivedBytes)
      // Retain the last reported total until a new response replaces it.
      assertEquals(1000L, reverted?.totalBytes)
      assertNull(reverted?.validator)
   }

   @Test
   fun `an empty temp file still reverts to paused`() {
      val reverted = DownloadManager.revertInProgress(inProgressRecord(), tempFileLength = 0L)

      assertEquals(DownloadStatus.Paused, reverted?.status)
      assertEquals(0L, reverted?.receivedBytes)
   }

   @Test
   fun `records that are not in progress are left alone`() {
      // Derived from the enum so a status added later is covered automatically.
      val statuses = DownloadStatus.entries.filter { it != DownloadStatus.InProgress }

      for (status in statuses) {
         val record = inProgressRecord().withStatus(status)

         assertNull("$status should not be reverted", DownloadManager.revertInProgress(record, 480L))
         assertNull("$status should not be reverted", DownloadManager.revertInProgress(record, null))
      }
   }

   // -- Network policy --

   // Building the Constraints outside enqueueDownload() shrinks the untestable surface
   // to one delegating call. That delegation stays uncovered — enqueueDownload() needs a
   // Context — so these pin the policy the constraint carries, not that the enqueue path
   // uses it.

   @Test
   fun `an unrestricted download only requires a connection`() {
      assertEquals(
         NetworkType.CONNECTED,
         DownloadManager.constraintsFor(CreateOptions(allowMetered = true)).requiredNetworkType,
      )
   }

   @Test
   fun `a restricted download requires an unmetered network`() {
      assertEquals(
         NetworkType.UNMETERED,
         DownloadManager.constraintsFor(CreateOptions(allowMetered = false)).requiredNetworkType,
      )
   }

   // -- Work request input data --

   @Test
   fun `a configured user agent is captured in the work request`() {
      val data = DownloadManager.inputDataFor(
         inProgressRecord(),
         userAgent = "my-app/1.0",
         storeDir = File("/tmp/store"),
      )

      assertEquals("my-app/1.0", data.getString(DownloadWorker.KEY_USER_AGENT))
      assertEquals("http://example.com/file.mp4", data.getString(DownloadWorker.KEY_URL))
      assertEquals("/tmp/file.mp4", data.getString(DownloadWorker.KEY_PATH))
   }

   @Test
   fun `no user agent stores a null value`() {
      // The key is present with a null value rather than absent — asserted on `size`
      // below rather than claimed in a comment, since `getString` returns null either
      // way and the worker reads that as "send no User-Agent", leaving OkHttp's
      // default. If `workDataOf` ever drops null entries, this is what catches it.
      val data = DownloadManager.inputDataFor(
         inProgressRecord(),
         userAgent = null,
         storeDir = File("/tmp/store"),
      )

      assertEquals(4, data.size())
      assertNull(data.getString(DownloadWorker.KEY_USER_AGENT))
   }

   @Test
   fun `the store directory is captured in the work request`() {
      // The worker opens the store from this, not from the manager: a re-run after
      // process death has no loaded plugin to have configured one. Without the key it
      // would open the default store and write progress where the app never reads it.
      val data = DownloadManager.inputDataFor(
         inProgressRecord(),
         userAgent = null,
         storeDir = File("/data/user/0/com.example/files/downloads"),
      )

      assertEquals(
         "/data/user/0/com.example/files/downloads",
         data.getString(DownloadWorker.KEY_STORE_DIR),
      )
   }
}
