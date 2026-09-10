package org.silvermine.downloadmanager

import okhttp3.Headers
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody
import okhttp3.ResponseBody.Companion.toResponseBody
import okio.Buffer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.IOException
import java.io.InterruptedIOException
import java.net.ProtocolException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.security.cert.CertificateException
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException

class DownloadWorkerTest {

   // These pin the predicate, not the branches it drives: a CoroutineWorker cannot be
   // built without WorkManager's test artifact, so neither path in handleTransientError
   // is covered here. WorkManager counts the runs before the current one, so a
   // download's first run sees 0.

   @Test
   fun `a download has attempts left up to the cap`() {
      for (runAttemptCount in 0..4) {
         assertFalse(
            "run reporting $runAttemptCount should still have attempts",
            DownloadWorker.isOutOfAttempts(runAttemptCount),
         )
      }
   }

   @Test
   fun `a download is out of attempts once five are spent`() {
      // Above the cap is reachable, not merely defensive: a constraint interruption
      // increments the count without ever consulting the cap.
      assertTrue(DownloadWorker.isOutOfAttempts(5))
      assertTrue(DownloadWorker.isOutOfAttempts(6))
   }

   // -- Request construction --

   @Test
   fun `a configured user agent is set on the request`() {
      val request = DownloadWorker.requestFor("https://example.com/f.bin", "my-app/1.0", 0L)

      assertEquals("my-app/1.0", request.header("User-Agent"))
   }

   @Test
   fun `no user agent leaves the header unset`() {
      // Absent rather than empty: OkHttp then sends its own default.
      val request = DownloadWorker.requestFor("https://example.com/f.bin", null, 0L)

      assertNull(request.header("User-Agent"))
   }

   @Test
   fun `a fresh download sends no range header`() {
      // The common path, and the boundary of the resume condition: without this,
      // widening `downloadedSize > 0` to `>= 0` changes no test outcome.
      val request = DownloadWorker.requestFor(
         "https://example.com/f.bin", "my-app/1.0", 0L, ResumeValidator.ETag("\"v1\""),
      )

      assertNull(request.header("Range"))
      assertNull(request.header("If-Range"))
   }

   @Test
   fun `the user agent and range headers coexist on a resume`() {
      // Mirrors the Rust test_user_agent_and_range_header_are_both_sent_on_resume:
      // neither header may displace the other.
      val request = DownloadWorker.requestFor(
         "https://example.com/f.bin", "my-app/1.0", 4L, ResumeValidator.ETag("\"v1\""),
      )

      assertEquals("my-app/1.0", request.header("User-Agent"))
      assertEquals("bytes=4-", request.header("Range"))
      assertEquals("\"v1\"", request.header("If-Range"))
   }

   @Test
   fun `a partial file without a usable validator requests a full body`() {
      for (validator in listOf(null, ResumeValidator.ETag("W/\"v1\""), ResumeValidator.ETag("invalid"))) {
         val request = DownloadWorker.requestFor("https://example.com/f.bin", null, 4L, validator)

         assertNull(request.header("Range"))
         assertNull(request.header("If-Range"))
      }
   }

   @Test
   fun `fresh and resumed requests use identity encoding`() {
      for (size in listOf(0L, 4L)) {
         val request = DownloadWorker.requestFor(
            "https://example.com/f.bin", null, size, ResumeValidator.ETag("\"v1\""),
         )
         assertEquals("identity", request.header("Accept-Encoding"))
      }
   }

   private fun partialRecord(): DownloadRecord = DownloadRecord(
      url = "https://example.com/f.bin",
      path = "/tmp/f.bin",
      receivedBytes = 3L,
      totalBytes = 10L,
      validator = ResumeValidator.ETag("\"old\""),
      status = DownloadStatus.InProgress,
   )

   private fun response(code: Int, headers: Headers = Headers.Builder().build(), unknownLength: Boolean = false): Response {
      val body = if (unknownLength) object : ResponseBody() {
         private val buffer = Buffer().writeUtf8("abcdef")
         override fun contentType() = null
         override fun contentLength() = -1L
         override fun source() = buffer
      } else "abcdef".toResponseBody()

      return Response.Builder()
         .request(Request.Builder().url("https://example.com/f.bin").build())
         .protocol(Protocol.HTTP_1_1)
         .code(code)
         .message("test response")
         .headers(headers)
         .body(body)
         .build()
   }

   private fun reload(record: DownloadRecord): DownloadRecord =
      DownloadStore.decodeRecords(DownloadStore.encodeRecords(listOf(record))).single()

   @Test
   fun `partial responses retain the validator and use actual file size`() {
      for (unknownLength in listOf(false, true)) {
         response(206, unknownLength = unknownLength).use { response ->
            val saved = reload(DownloadWorker.responseRecord(partialRecord(), response, 4L))
            val request = DownloadWorker.requestFor(saved.url, null, saved.receivedBytes, saved.validator)

            assertEquals(4L, saved.receivedBytes)
            assertEquals(10L, saved.totalBytes)
            assertEquals("bytes=4-", request.header("Range"))
            assertEquals("\"old\"", request.header("If-Range"))
         }
      }
   }

   @Test
   fun `full replacement saves new validator before a subsequent resume`() {
      for (validator in listOf<ResumeValidator>(
         ResumeValidator.ETag("\"new\""),
         ResumeValidator.LastModified(1445412480000L),
      )) {
         val headers = when (validator) {
            is ResumeValidator.ETag -> Headers.headersOf("ETag", validator.value)
            is ResumeValidator.LastModified -> Headers.headersOf(
               "Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT",
               "Date", "Wed, 21 Oct 2015 07:29:00 GMT",
            )
         }
         response(200, headers, unknownLength = true).use { response ->
            val updated = DownloadWorker.responseRecord(partialRecord(), response, 4L)
            assertEquals(0L, updated.receivedBytes)
            assertNull(updated.totalBytes)

            val saved = reload(updated.withBytes(2L).withStatus(DownloadStatus.Paused))
            val request = DownloadWorker.requestFor(saved.url, null, 2L, saved.validator)
            assertEquals(validator, saved.validator)
            assertEquals("bytes=2-", request.header("Range"))
            assertEquals(validator.ifRange(), request.header("If-Range"))
         }
      }
   }

   @Test
   fun `full response without validator clears the old one`() {
      response(200).use { response ->
         val updated = DownloadWorker.responseRecord(partialRecord(), response, 4L)
         assertEquals(0L, updated.receivedBytes)
         assertEquals(6L, updated.totalBytes)

         val saved = reload(updated.withBytes(2L).withStatus(DownloadStatus.Paused))
         val request = DownloadWorker.requestFor(saved.url, null, 2L, saved.validator)
         assertNull(saved.validator)
         assertNull(request.header("Range"))
         assertNull(request.header("If-Range"))
      }
   }

   // -- Total size --

   @Test
   fun `an unstated content length has no total`() {
      // OkHttp's -1. The download runs on the coarse byte cadence and reports
      // indeterminate progress.
      assertNull(DownloadWorker.totalSizeFor(-1L, 0L))
      assertNull(DownloadWorker.totalSizeFor(-1L, 512L))
   }

   @Test
   fun `a stated zero content length is a known total`() {
      // An empty body is a complete download, not one of unknown length. Desktop
      // reports 0 for the same response, and collapsing it to null disagreed.
      assertEquals(0L, DownloadWorker.totalSizeFor(0L, 0L))
   }

   @Test
   fun `a content length that overflows the sum has no total`() {
      // The header is the server's to choose. Without the guard the wrapped Long
      // reaches the caller as a negative total.
      assertNull(DownloadWorker.totalSizeFor(Long.MAX_VALUE, 1L))
   }

   @Test
   fun `a resumed download adds the bytes already held`() {
      // The Range response counts only what is left to send.
      assertEquals(1000L, DownloadWorker.totalSizeFor(600L, 400L))
   }

   // -- Resume failure outcome --

   @Test
   fun `a 416 stating a total equal to the partial completes it`() {
      assertEquals(
         DownloadWorker.PartialFileOutcome.Complete,
         DownloadWorker.partialFileOutcomeFor(416, "bytes */1000", 1000L),
      )
   }

   @Test
   fun `any other 416 discards the partial`() {
      for (contentRange in listOf(null, "bytes */999", "bytes 0-499/1000", "1000", "bytes */abc")) {
         assertEquals(
            "Content-Range $contentRange",
            DownloadWorker.PartialFileOutcome.Discard,
            DownloadWorker.partialFileOutcomeFor(416, contentRange, 1000L),
         )
      }
   }

   @Test
   fun `other failures on a resume keep the partial`() {
      for (responseCode in listOf(503, 500, 404, 403)) {
         assertEquals(
            "HTTP $responseCode",
            DownloadWorker.PartialFileOutcome.KeepPartial,
            DownloadWorker.partialFileOutcomeFor(responseCode, "bytes */1000", 1000L),
         )
      }
   }

   // -- Failure classification --
   //
   // Transient means the partial survives and the work is retried.

   @Test
   fun `a connect timeout is transient whatever the platform calls it`() {
      // The JVM's wording and Android libcore's. Neither says "timeout".
      assertTrue(DownloadWorker.isTransient(SocketTimeoutException("Connect timed out")))
      assertTrue(
         DownloadWorker.isTransient(
            SocketTimeoutException(
               "failed to connect to example.com/93.184.216.34 (port 443) from /10.0.2.15 (port 41234) after 30000ms",
            ),
         ),
      )
   }

   @Test
   fun `a read timeout is transient from either racing source`() {
      // OkHttp sets the socket timeout to the same interval as Okio's watchdog, so
      // which message arrives is a race. Both must classify alike.
      assertTrue(DownloadWorker.isTransient(SocketTimeoutException("timeout")))
      assertTrue(DownloadWorker.isTransient(SocketTimeoutException("Read timed out")))
   }

   @Test
   fun `an interrupt that is not a timeout is permanent`() {
      // This process tearing the read down, not the network failing.
      assertFalse(DownloadWorker.isTransient(InterruptedIOException("thread interrupted")))
   }

   @Test
   fun `a DNS failure is permanent`() {
      assertFalse(DownloadWorker.isTransient(UnknownHostException("example.invalid")))
   }

   @Test
   fun `a mid-stream TLS failure is transient`() {
      // Conscrypt reports a reset inside an established TLS session this way.
      assertTrue(DownloadWorker.isTransient(SSLException("Read error: ssl=0x0: I/O error during system call, Connection reset by peer")))
   }

   @Test
   fun `a certificate failure is permanent`() {
      // Retrying cannot make an untrusted certificate trusted.
      val handshake = SSLHandshakeException("Trust anchor for certification path not found")

      handshake.initCause(CertificateException("untrusted root"))

      assertFalse(DownloadWorker.isTransient(handshake))
      assertFalse(DownloadWorker.isTransient(SSLPeerUnverifiedException("Hostname example.com not verified")))
   }

   @Test
   fun `a handshake failure with no certificate cause is transient`() {
      // Matches OkHttp's isRecoverable, which refuses only the certificate case.
      assertTrue(DownloadWorker.isTransient(SSLHandshakeException("Connection closed by peer")))
   }

   @Test
   fun `an ordinary network failure is transient`() {
      assertTrue(DownloadWorker.isTransient(SocketException("Connection reset")))
      assertTrue(DownloadWorker.isTransient(IOException("unexpected end of stream")))
      assertTrue(DownloadWorker.isTransient(ProtocolException("unexpected status line")))
   }
}
