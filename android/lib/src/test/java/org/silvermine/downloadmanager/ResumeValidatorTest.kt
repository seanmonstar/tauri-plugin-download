package org.silvermine.downloadmanager

import okhttp3.Headers
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ResumeValidatorTest {
   private val modified = "Wed, 21 Oct 2015 07:28:00 GMT"

   private fun datedHeaders(date: String = "Wed, 21 Oct 2015 07:29:00 GMT"): Headers.Builder =
      Headers.Builder().add("Last-Modified", modified).add("Date", date)

   @Test
   fun `strong etag takes precedence over last modified`() {
      val validator = ResumeValidator.fromHeaders(datedHeaders().add("ETag", "\"version-1\"").build())

      assertEquals(ResumeValidator.ETag("\"version-1\""), validator)
      assertEquals("\"version-1\"", validator?.ifRange())
   }

   @Test
   fun `weak or malformed etags do not fall back to dates`() {
      for (tag in listOf("W/\"version-1\"", "unquoted", "\"has space\"", "\"one\", \"two\"", "")) {
         assertNull(tag, ResumeValidator.fromHeaders(datedHeaders().add("ETag", tag).build()))
         assertNull(tag, ResumeValidator.ETag(tag).ifRange())
      }
   }

   @Test
   fun `multiple etags are rejected`() {
      val headers = datedHeaders().add("ETag", "\"one\"").add("ETag", "\"two\"").build()

      assertNull(ResumeValidator.fromHeaders(headers))
   }

   @Test
   fun `empty opaque tag and a comma inside a tag are valid`() {
      for (tag in listOf("\"\"", "\"one,two\"")) {
         assertEquals(tag, ResumeValidator.fromHeaders(Headers.headersOf("ETag", tag))?.ifRange())
      }
   }

   @Test
   fun `last modified requires the conservative sixty second margin`() {
      assertNull(ResumeValidator.fromHeaders(datedHeaders("Wed, 21 Oct 2015 07:28:59 GMT").build()))
      assertNull(ResumeValidator.fromHeaders(datedHeaders("Wed, 21 Oct 2015 07:27:59 GMT").build()))
      val validator = ResumeValidator.fromHeaders(datedHeaders().build())

      assertEquals(ResumeValidator.LastModified(1445412480000L), validator)
      assertEquals(modified, validator?.ifRange())
   }

   @Test
   fun `missing malformed or duplicate dates cannot validate a resume`() {
      for (headers in listOf(
         Headers.Builder().build(),
         Headers.headersOf("Last-Modified", modified),
         Headers.headersOf("Date", "Wed, 21 Oct 2015 07:29:00 GMT"),
         datedHeaders("invalid").build(),
         datedHeaders().set("Last-Modified", "invalid").build(),
         datedHeaders().add("Last-Modified", modified).build(),
         datedHeaders().add("Date", "Wed, 21 Oct 2015 07:30:00 GMT").build(),
      )) {
         assertNull(ResumeValidator.fromHeaders(headers))
      }
   }
}
