package org.silvermine.downloadmanager

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import okhttp3.Headers
import java.util.Date

/** Identifies the representation saved in the temporary file. Internal to the store. */
@Serializable
internal sealed class ResumeValidator {
   @Serializable
   @SerialName("etag")
   data class ETag(val value: String) : ResumeValidator()

   @Serializable
   @SerialName("lastModified")
   data class LastModified(val epochMillis: Long) : ResumeValidator()

   fun ifRange(): String? = when (this) {
      is ETag -> value.takeIf { isStrongETag(it) }
      is LastModified -> Headers.Builder()
         .add("If-Range", Date(epochMillis))
         .build()["If-Range"]
   }

   companion object {
      fun fromHeaders(headers: Headers): ResumeValidator? {
         val tags = headers.values("ETag")
         if (tags.isNotEmpty()) {
            // A weak entity tag also forbids falling back to a date in If-Range.
            val tag = tags.singleOrNull() ?: return null
            return if (isStrongETag(tag)) ETag(tag) else null
         }

         if (headers.values("Last-Modified").size != 1 || headers.values("Date").size != 1) {
            return null
         }
         val modified = headers.getDate("Last-Modified") ?: return null
         val date = headers.getDate("Date") ?: return null
         // Match Rust's conservative clock-skew margin for a strong date validator.
         if (modified.time > Long.MAX_VALUE - 60_000L || date.time < modified.time + 60_000L) {
            return null
         }
         return LastModified(modified.time)
      }

      private fun isStrongETag(value: String): Boolean =
         value.length >= 2 && value.first() == '"' && value.last() == '"' &&
            value.substring(1, value.length - 1).all { it == '!' || it in '#'..'~' }
   }
}
