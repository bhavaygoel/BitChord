package com.music.bitchord

import com.music.bitchord.data.sources.StreamFormat
import com.music.bitchord.playback.AddonPrefetchLedger
import com.music.bitchord.playback.NextTrackPrefetcher
import com.music.bitchord.playback.NextTrackPrefetcher.Candidate
import com.music.bitchord.playback.NextTrackPrefetcher.FetchFailure
import com.music.bitchord.playback.NextTrackPrefetcher.Lookup
import com.music.bitchord.playback.NextTrackPrefetcher.Outcome
import com.music.bitchord.playback.NextTrackPrefetcher.Prebuffered
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The decisions behind addon read-ahead, driven against a fake [NextTrackPrefetcher.Io]
 * that models the one rule that matters: a cache entry holds bytes from exactly
 * one stream, and the pin says which.
 */
class NextTrackPrefetchTest {

    private val flac = StreamFormat(codec = "flac", sampleRateHz = 44_100, bitDepth = 16)
    private val hiRes = StreamFormat(codec = "flac", sampleRateHz = 96_000, bitDepth = 24)

    private var clock = 0L
    private val ledger = AddonPrefetchLedger { clock }
    private val prefetcher = NextTrackPrefetcher<String>(ledger)

    /**
     * A track's pin and cache entry. [entryWrittenBy] is the stream whose bytes
     * are on disk — the invariant every test checks is that it is never
     * different from [pin] while both are set.
     */
    private inner class FakeIo(
        override val videoId: String = "vid",
        var lookups: MutableList<Lookup<String>> = mutableListOf(),
    ) : NextTrackPrefetcher.Io<String> {
        var pin: String? = null
        var entryWrittenBy: String? = null
        var cached = 0L
        var lookUpCalls = 0
        var wanted = true
        var opened = false
        var locked = false
        val fetchFailures = ArrayDeque<FetchFailure?>()
        val prebufferRequests = mutableListOf<Long>()

        override suspend fun lookUp(): Lookup<String> {
            lookUpCalls++
            return lookups.removeFirstOrNull() ?: Lookup.Miss
        }

        override fun currentPin(): String? = pin

        override fun pin(candidate: Candidate<String>) {
            pin = candidate.stream
        }

        override fun unpin() {
            pin = null
        }

        override suspend fun prebuffer(bytes: Long): Prebuffered {
            prebufferRequests += bytes
            val pinned = checkNotNull(pin) { "bytes fetched with nothing pinned" }
            check(entryWrittenBy == null || entryWrittenBy == pinned) {
                "$pinned written into an entry holding $entryWrittenBy"
            }
            val failure = fetchFailures.removeFirstOrNull()
            if (failure == null) {
                entryWrittenBy = pinned
                cached = maxOf(cached, bytes)
            }
            return Prebuffered(cached, failure)
        }

        override fun cachedBytes(): Long = cached

        override fun discard(): Boolean {
            if (locked) return false
            cached = 0
            entryWrittenBy = null
            return true
        }

        override fun stillWanted(): Boolean = wanted

        override fun openedByPlayer(): Boolean = opened

        fun assertConsistent() {
            if (cached > 0) assertEquals("cached bytes must belong to the pinned stream", pin, entryWrittenBy)
        }
    }

    private fun found(url: String, format: StreamFormat = flac, configId: String = "nebula") =
        Lookup.Found(Candidate(url, format, configId))

    // ── Successful prefetch ──────────────────────────────────────────────

    @Test
    fun `a found lossless stream is pinned and its opening cached`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))

        val outcome = prefetcher.prefetch(io)

        assertTrue(outcome is Outcome.Ready)
        assertEquals("flac-1", io.pin)
        assertEquals(NextTrackPrefetcher.prebufferBytes(flac), (outcome as Outcome.Ready).cachedBytes)
        io.assertConsistent()
    }

    @Test
    fun `a second run tops up the same pin without asking the addon again`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1"), found("flac-2")))

        prefetcher.prefetch(io)
        val again = prefetcher.prefetch(io)

        assertTrue(again is Outcome.Ready)
        assertEquals(1, io.lookUpCalls)
        assertEquals("flac-1", io.pin)
        // Sized from the pinned stream's format, not a guess.
        assertEquals(io.prebufferRequests[0], io.prebufferRequests[1])
        io.assertConsistent()
    }

    @Test
    fun `a run already in flight for the track is not duplicated`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        assertTrue(ledger.claim("vid"))

        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals(0, io.lookUpCalls)
        ledger.release("vid")
    }

    // ── Skipped tracks and misses ────────────────────────────────────────

    @Test
    fun `a track the queue moved past while the addon searched is never pinned`() = runTest {
        val io = FakeIo()
        io.lookups = mutableListOf(found("flac-1"))
        io.wanted = false

        assertEquals(Outcome.Abandoned, prefetcher.prefetch(io))
        assertNull(io.pin)
        assertEquals(0L, io.cached)
    }

    @Test
    fun `a miss leaves no pin, so playback falls back to YouTube, and is not asked again`() = runTest {
        val io = FakeIo(lookups = mutableListOf(Lookup.Miss, found("flac-1")))

        assertEquals(Outcome.Missed, prefetcher.prefetch(io))
        assertNull(io.pin)
        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals(1, io.lookUpCalls)

        clock += AddonPrefetchLedger.MISS_MEMORY_MS
        assertTrue(prefetcher.prefetch(io) is Outcome.Ready)
    }

    @Test
    fun `nothing opted in changes nothing and is not remembered as a miss`() = runTest {
        val io = FakeIo(lookups = mutableListOf(Lookup.NotApplicable, found("flac-1")))

        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertNull(io.pin)
        assertFalse(ledger.recentlyMissed("vid"))
    }

    @Test
    fun `a track the player already opened is left to the player`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.opened = true

        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals(0, io.lookUpCalls)
        assertNull(io.pin)
    }

    @Test
    fun `a pin someone else made is never replaced`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.pin = "jiosaavn-320"
        io.entryWrittenBy = "jiosaavn-320"
        io.cached = 1024

        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals("jiosaavn-320", io.pin)
        assertEquals(0, io.lookUpCalls)
        io.assertConsistent()
    }

    // ── Rate limits ──────────────────────────────────────────────────────

    @Test
    fun `a rate limited addon is cooled down, not remembered as missing the track`() = runTest {
        val io = FakeIo(lookups = mutableListOf(Lookup.RateLimited("nebula", retryAfterMs = 5_000)))

        assertEquals(Outcome.RateLimited, prefetcher.prefetch(io))
        assertTrue(ledger.coolingDown("nebula"))
        assertFalse(ledger.recentlyMissed("vid"))
        assertNull(io.pin)

        clock += AddonPrefetchLedger.MIN_COOL_DOWN_MS
        assertFalse(ledger.coolingDown("nebula"))
    }

    @Test
    fun `a 429 on the file itself before any byte landed releases the pin`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.fetchFailures += FetchFailure.RateLimited(retryAfterMs = null)

        assertEquals(Outcome.RateLimited, prefetcher.prefetch(io))
        assertNull(io.pin)
        assertTrue(ledger.coolingDown("nebula"))
    }

    // ── Expired URLs ─────────────────────────────────────────────────────

    @Test
    fun `an expired URL is replaced by one fresh lookup`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-old"), found("flac-new")))
        io.fetchFailures += FetchFailure.Expired

        val outcome = prefetcher.prefetch(io)

        assertTrue(outcome is Outcome.Ready)
        assertEquals("flac-new", io.pin)
        assertEquals(2, io.lookUpCalls)
        io.assertConsistent()
    }

    @Test
    fun `a URL that expires twice is given up on rather than retried forever`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("a"), found("b"), found("c")))
        io.fetchFailures += FetchFailure.Expired
        io.fetchFailures += FetchFailure.Expired

        assertEquals(Outcome.Failed, prefetcher.prefetch(io))
        assertEquals(2, io.lookUpCalls)
        assertNull(io.pin)
    }

    @Test
    fun `a stale pin is replaced while the track still waits`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1"), found("flac-2")))
        prefetcher.prefetch(io)

        clock += AddonPrefetchLedger.PIN_REFRESH_MS
        val outcome = prefetcher.prefetch(io)

        assertTrue(outcome is Outcome.Ready)
        assertEquals("flac-2", io.pin)
        io.assertConsistent()
    }

    @Test
    fun `keepFresh renews a waiting pin and stops once the player has the track`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1"), found("flac-2"), found("flac-3")))
        prefetcher.prefetch(io)

        val watch = launch { prefetcher.keepFresh(io, checkEveryMs = 1_000) }
        clock += AddonPrefetchLedger.PIN_REFRESH_MS
        advanceTimeBy(1_001)
        runCurrent()
        assertEquals("flac-2", io.pin)

        io.opened = true
        clock += AddonPrefetchLedger.PIN_REFRESH_MS
        advanceTimeBy(1_001)
        runCurrent()
        assertFalse(watch.isActive)
        assertEquals("flac-2", io.pin)
        io.assertConsistent()
    }

    // ── Cache consistency ────────────────────────────────────────────────

    @Test
    fun `a stale pin the player is reading is never swapped out`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1"), found("flac-2")))
        prefetcher.prefetch(io)
        io.opened = true

        clock += AddonPrefetchLedger.PIN_REFRESH_MS
        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals("flac-1", io.pin)
        io.assertConsistent()
    }

    @Test
    fun `a stale pin whose bytes cannot be deleted is kept`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1"), found("flac-2")))
        prefetcher.prefetch(io)
        io.locked = true

        clock += AddonPrefetchLedger.PIN_REFRESH_MS
        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertEquals("flac-1", io.pin)
        io.assertConsistent()
    }

    @Test
    fun `bytes left by an earlier play are cleared before a new pin goes over them`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.entryWrittenBy = "yesterdays-jiosaavn"
        io.cached = 3_000_000

        assertTrue(prefetcher.prefetch(io) is Outcome.Ready)
        assertEquals("flac-1", io.entryWrittenBy)
        io.assertConsistent()
    }

    @Test
    fun `old bytes that cannot be cleared mean no pin at all`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.entryWrittenBy = "yesterdays-jiosaavn"
        io.cached = 3_000_000
        io.locked = true

        assertEquals(Outcome.Skipped, prefetcher.prefetch(io))
        assertNull(io.pin)
    }

    @Test
    fun `a fetch that fails after writing some bytes keeps the pin those bytes belong to`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        // A first run caches part of the opening; the top-up then fails.
        prefetcher.prefetch(io)
        clock += 1
        io.fetchFailures += FetchFailure.Other

        prefetcher.prefetch(io)

        assertEquals("flac-1", io.pin)
        io.assertConsistent()
    }

    @Test
    fun `a fetch that fails before writing anything releases the pin`() = runTest {
        val io = FakeIo(lookups = mutableListOf(found("flac-1")))
        io.fetchFailures += FetchFailure.Other

        assertEquals(Outcome.Failed, prefetcher.prefetch(io))
        assertNull(io.pin)
    }

    // ── Sizing ───────────────────────────────────────────────────────────

    @Test
    fun `prebuffer covers the opening of a lossless track without downloading all of it`() {
        val cd = NextTrackPrefetcher.prebufferBytes(flac)
        val high = NextTrackPrefetcher.prebufferBytes(hiRes)
        assertTrue(cd in NextTrackPrefetcher.MIN_PREBUFFER_BYTES..NextTrackPrefetcher.MAX_PREBUFFER_BYTES)
        assertTrue("hi-res needs more bytes for the same seconds", high > cd)
        assertEquals(NextTrackPrefetcher.MAX_PREBUFFER_BYTES, high)
        // ~25s of CD FLAC is roughly 2.6MB of audio plus the header allowance.
        assertTrue(cd in 2_900_000L..3_300_000L)
    }

    @Test
    fun `an undescribed stream is sized as CD quality`() {
        assertEquals(
            NextTrackPrefetcher.prebufferBytes(flac),
            NextTrackPrefetcher.prebufferBytes(StreamFormat(codec = "flac")),
        )
        assertEquals(NextTrackPrefetcher.prebufferBytes(flac), NextTrackPrefetcher.prebufferBytes(null))
    }
}
