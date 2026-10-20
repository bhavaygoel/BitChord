package com.music.bitchord.playback

import com.music.bitchord.data.sources.StreamFormat
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.delay

/**
 * Reads the next track ahead from an addon that opted in to it — see
 * [SourceConfig.prefetchNext][com.music.bitchord.data.sources.SourceConfig.prefetchNext].
 *
 * Without this, a YouTube-queued track only ever *starts* on what can be found
 * in the gap between songs, and an addon serving FLAC rarely can be: its search
 * is slower than a warmed YouTube URL, so the track begins on Opus and
 * [QualityUpgrade] cuts the FLAC in a few seconds later — the "Upgrading
 * quality" seam. Asking the addon while the previous track is still playing
 * turns that into a pinned stream and a cached opening, and the track starts
 * on the lossless copy with no swap at all.
 *
 * ### Who owns what
 *
 * The decisions live here and nothing else: what to do on a miss, a rate limit,
 * an expired URL, a queue change. Everything with side effects — the addon
 * lookup, [StreamChoice], the disk cache — is behind [Io], which [AudioCache]
 * implements. That split is what lets every one of those cases be tested
 * without a player, a cache or a network.
 *
 * Two invariants carry the safety argument, and each case below keeps both:
 *
 *  1. **No bytes without a pin.** Read-ahead writes under the same `#alt` key
 *     playback will read from, and only a pin in [StreamChoice] makes both
 *     writers agree on which file that key holds. So [Io.pin] always comes
 *     before [Io.prebuffer].
 *  2. **No unpinning over bytes.** A cached prefix belongs to the stream that
 *     wrote it; dropping the pin while that prefix is on disk frees playback to
 *     finish the entry from a different file — the seam [StreamChoice] exists
 *     to prevent. A pin is only released once its bytes are gone ([Io.discard])
 *     or were never written.
 */
internal class NextTrackPrefetcher<S>(private val ledger: AddonPrefetchLedger) {

    /** One prefetchable copy of the track: what [Io.lookUp] found. */
    data class Candidate<S>(val stream: S, val format: StreamFormat, val configId: String?)

    sealed interface Lookup<out S> {
        data class Found<S>(val candidate: Candidate<S>) : Lookup<S>
        /** Asked, and no opted-in addon holds a lossless copy. */
        data object Miss : Lookup<Nothing>
        /** An addon is rate limiting; nothing was learned about the track. */
        data class RateLimited(val configId: String, val retryAfterMs: Long?) : Lookup<Nothing>
        /** Not asked at all — no addon opted in, a capped connection, a video row. */
        data object NotApplicable : Lookup<Nothing>
    }

    /** What one [Io.prebuffer] call left on disk, and why it stopped if it did. */
    data class Prebuffered(val cachedBytes: Long, val failure: FetchFailure? = null)

    sealed interface FetchFailure {
        /** The URL answered 401/403/404/410: signed and expired, or revoked. */
        data object Expired : FetchFailure
        data class RateLimited(val retryAfterMs: Long?) : FetchFailure
        data object Other : FetchFailure
    }

    /** The side of a prefetch that touches the world. One instance per track. */
    interface Io<S> {
        val videoId: String

        /** Asks the opted-in addons for the track. */
        suspend fun lookUp(): Lookup<S>

        /** The stream [StreamChoice] currently holds for the track, if any. */
        fun currentPin(): S?

        fun pin(candidate: Candidate<S>)

        fun unpin()

        /** Caches the opening [bytes] of the pinned stream under playback's own key. */
        suspend fun prebuffer(bytes: Long): Prebuffered

        /** How much of the track's entry is on disk, contiguously from byte 0. */
        fun cachedBytes(): Long

        /** Deletes the track's entry. False when a reader holds it. */
        fun discard(): Boolean

        /** Whether the track is still the one read-ahead is for. */
        fun stillWanted(): Boolean

        /** Whether the player itself has opened the track — from then on it owns the entry. */
        fun openedByPlayer(): Boolean
    }

    sealed interface Outcome {
        /** Pinned, with this much of its opening on disk. */
        data class Ready(val cachedBytes: Long) : Outcome
        data object Missed : Outcome
        data object RateLimited : Outcome
        /** Nothing to do: not applicable, recently missed, cooling down, or already running. */
        data object Skipped : Outcome
        /** The queue moved on mid-way. */
        data object Abandoned : Outcome
        /** The lookup worked and the bytes did not; whatever playback does next is unchanged. */
        data object Failed : Outcome
    }

    /**
     * Pins and prebuffers [io]'s track, or leaves everything as it found it.
     *
     * Safe to call again for the same track: a second call while one runs is
     * skipped, and a call after one finished tops up the same pinned stream
     * rather than asking the addon again — unless that pin has grown old enough
     * to be replaced, see [AddonPrefetchLedger.isStale].
     */
    suspend fun prefetch(io: Io<S>): Outcome {
        val videoId = io.videoId
        if (!ledger.claim(videoId)) return Outcome.Skipped
        try {
            // The player got there first. Whatever it is reading — a pin of its
            // own, or bytes cached by an earlier play — is no longer
            // read-ahead's to change.
            if (io.openedByPlayer()) return Outcome.Skipped
            reuseExistingPin(io)?.let { return it }
            if (ledger.recentlyMissed(videoId)) return Outcome.Skipped
            var refreshed = false
            while (true) {
                val found = when (val lookup = io.lookUp()) {
                    is Lookup.Found -> lookup.candidate
                    Lookup.Miss -> {
                        ledger.recordMiss(videoId)
                        return Outcome.Missed
                    }
                    is Lookup.RateLimited -> {
                        ledger.coolDown(lookup.configId, lookup.retryAfterMs)
                        return Outcome.RateLimited
                    }
                    Lookup.NotApplicable -> return Outcome.Skipped
                }
                // The queue can move while an addon thinks; a pin for a track
                // nobody is about to play is a promise nothing will keep.
                if (!io.stillWanted()) return Outcome.Abandoned
                // Bytes already on disk with no pin over them came from some
                // earlier play — JioSaavn's copy kept from yesterday, say. A
                // pin would have the player read that opening and continue it
                // from this file. Cleared first, or not pinned at all.
                if (io.cachedBytes() > 0 && !io.discard()) return Outcome.Skipped
                io.pin(found)
                ledger.recordPin(videoId, found.format)
                val result = io.prebuffer(prebufferBytes(found.format))
                when (val failure = result.failure) {
                    null -> return Outcome.Ready(result.cachedBytes)
                    FetchFailure.Expired -> {
                        // Dead before it was ever played. One fresh lookup is
                        // worth it — the addon mints a new URL for the same
                        // file — but only while nothing has started reading the
                        // old one, and only once: a URL that dies twice in a
                        // row is a server problem, not an expiry.
                        if (refreshed || io.openedByPlayer() || !releasePin(io)) {
                            releasePinIfEmpty(io)
                            return Outcome.Failed
                        }
                        refreshed = true
                    }
                    is FetchFailure.RateLimited -> {
                        found.configId?.let { ledger.coolDown(it, failure.retryAfterMs) }
                        releasePinIfEmpty(io)
                        return Outcome.RateLimited
                    }
                    FetchFailure.Other -> {
                        releasePinIfEmpty(io)
                        return if (io.stillWanted()) Outcome.Failed else Outcome.Abandoned
                    }
                }
            }
        } finally {
            ledger.release(videoId)
        }
    }

    /**
     * Keeps [io]'s pin young enough to trust for as long as its track waits.
     *
     * Read-ahead pins the next track seconds into the current one, and the wait
     * can be long — a twelve-minute track, a paused player. A pin is only as
     * good as its URL, so once one is [stale][AddonPrefetchLedger.isStale] and
     * the player has not yet touched the track, its bytes and pin are dropped
     * and [prefetch] fetches both afresh. Returns when the track stops being wanted,
     * or once the player has it.
     */
    suspend fun keepFresh(io: Io<S>, checkEveryMs: Long = FRESHNESS_CHECK_MS) {
        while (true) {
            delay(checkEveryMs)
            if (!io.stillWanted() || io.openedByPlayer()) return
            if (ledger.pinnedAt(io.videoId) == null) return
            if (ledger.isStale(io.videoId)) prefetch(io)
        }
    }

    /**
     * Handles a track that is already pinned. Null means "go and look it up".
     */
    private suspend fun reuseExistingPin(io: Io<S>): Outcome? {
        if (io.currentPin() == null) {
            // Our record outlived the pin — [StreamChoice] expired or forgot it.
            ledger.forgetPin(io.videoId)
            return null
        }
        // Someone else's pin: playback's own choice, or the quick-source
        // warm-up. Theirs to keep, whatever it is.
        if (ledger.pinnedAt(io.videoId) == null) return Outcome.Skipped
        if (!ledger.isStale(io.videoId)) {
            // Ours and current: top up the opening without asking the addon
            // again. [Io.prebuffer] only fetches what is missing.
            val result = io.prebuffer(prebufferBytes(ledger.pinnedFormat(io.videoId)))
            return Outcome.Ready(result.cachedBytes)
        }
        // Ours but old. The player has it, or its bytes cannot be dropped:
        // leave it alone, invariant 2.
        if (io.openedByPlayer() || !releasePin(io)) return Outcome.Skipped
        return null
    }

    /** Drops bytes then pin, in that order. False when the bytes could not go. */
    private fun releasePin(io: Io<S>): Boolean {
        if (io.cachedBytes() > 0 && !io.discard()) return false
        io.unpin()
        ledger.forgetPin(io.videoId)
        return true
    }

    /** After a failed fetch: release the pin only if no bytes were written under it. */
    private fun releasePinIfEmpty(io: Io<S>) {
        if (io.cachedBytes() > 0 || io.openedByPlayer()) return
        io.unpin()
        ledger.forgetPin(io.videoId)
    }

    companion object {
        /**
         * How much audio to have on disk before the track is reached.
         *
         * Enough that the player can parse the container and play well past the
         * point where its own continuation fetch has caught up — FLAC's header,
         * seek table and embedded cover alone can run to hundreds of kilobytes —
         * and no more. A track read ahead may be skipped, and on a lossless
         * stream every second costs ten times what YouTube's does.
         */
        const val PREBUFFER_SECONDS = 25

        /** Allowance for a FLAC's metadata blocks and embedded artwork. */
        const val HEADER_ALLOWANCE_BYTES = 512L * 1024

        /** Floor: a CD-quality FLAC's opening is never smaller than this is worth asking for. */
        const val MIN_PREBUFFER_BYTES = 2L * 1024 * 1024

        /**
         * Ceiling: about 23 seconds of 24-bit/96kHz. Never the whole track —
         * a lossless album track runs to 30-60MB and the listener may skip it.
         */
        const val MAX_PREBUFFER_BYTES = 8L * 1024 * 1024

        /** How often [keepFresh] looks at a waiting pin. */
        const val FRESHNESS_CHECK_MS = 60_000L

        /**
         * Bytes worth caching ahead of a track in [format], within
         * [MIN_PREBUFFER_BYTES]..[MAX_PREBUFFER_BYTES].
         *
         * Lossless is estimated from the PCM rate at FLAC's typical 60%: an
         * addon states sample rate and depth far more reliably than bitrate,
         * and nothing states a bitrate for FLAC at all. Unknown parameters are
         * read as CD quality. A lossy stream with a bitrate is sized from it.
         */
        fun prebufferBytes(format: StreamFormat?): Long {
            val kbps = when {
                format == null -> null
                format.isLossless == true -> {
                    val rate = format.sampleRateHz ?: 44_100
                    val depth = format.bitDepth ?: 16
                    (rate.toLong() * depth * 2 * 6 / 10 / 1000).toInt()
                }
                else -> format.kbps
            } ?: CD_FLAC_KBPS
            val bytes = kbps.toLong() * 1000 / 8 * PREBUFFER_SECONDS + HEADER_ALLOWANCE_BYTES
            return bytes.coerceIn(MIN_PREBUFFER_BYTES, MAX_PREBUFFER_BYTES)
        }

        /** 16-bit/44.1kHz stereo at FLAC's typical 60% of PCM. */
        private const val CD_FLAC_KBPS = (44_100L * 16 * 2 * 6 / 10 / 1000).toInt()
    }
}

/**
 * What read-ahead remembers between calls, so the same question is never asked
 * of an addon twice in a row for nothing.
 *
 *  - **One lookup per track at a time** — [claim]. Read-ahead is re-run on
 *    every play, pause and queue edit, and an addon search is not free.
 *  - **Misses are remembered** for [MISS_MEMORY_MS]. A track an addon does not
 *    hold will not appear because the listener paused and resumed.
 *  - **Rate limits are respected per addon** for at least [MIN_COOL_DOWN_MS],
 *    or as long as the addon said. A limited debrid account has a quota, and
 *    read-ahead spending it is the track actually being played losing it.
 *  - **Our own pins are dated**, so a pin made for a track that is still not
 *    playing [PIN_REFRESH_MS] later is replaced rather than trusted.
 */
internal class AddonPrefetchLedger(private val now: () -> Long) {

    private val inFlight = ConcurrentHashMap.newKeySet<String>()
    private val missedAt = ConcurrentHashMap<String, Long>()
    private val quietUntil = ConcurrentHashMap<String, Long>()
    private class Pin(val at: Long, val format: StreamFormat)

    private val pins = ConcurrentHashMap<String, Pin>()

    fun claim(videoId: String): Boolean = inFlight.add(videoId)

    fun release(videoId: String) {
        inFlight.remove(videoId)
    }

    fun recordMiss(videoId: String) {
        if (missedAt.size >= MAX_REMEMBERED) missedAt.clear()
        missedAt[videoId] = now()
    }

    fun recentlyMissed(videoId: String): Boolean {
        val at = missedAt[videoId] ?: return false
        if (now() - at < MISS_MEMORY_MS) return true
        missedAt.remove(videoId)
        return false
    }

    fun coolDown(configId: String, retryAfterMs: Long?) {
        val wait = (retryAfterMs ?: 0L).coerceIn(MIN_COOL_DOWN_MS, MAX_COOL_DOWN_MS)
        quietUntil[configId] = now() + wait
    }

    fun coolingDown(configId: String): Boolean = (quietUntil[configId] ?: return false) > now()

    fun recordPin(videoId: String, format: StreamFormat) {
        if (pins.size >= MAX_REMEMBERED) pins.clear()
        pins[videoId] = Pin(now(), format)
    }

    fun pinnedAt(videoId: String): Long? = pins[videoId]?.at

    fun pinnedFormat(videoId: String): StreamFormat? = pins[videoId]?.format

    fun isStale(videoId: String): Boolean = (pins[videoId]?.at ?: return false) + PIN_REFRESH_MS <= now()

    fun forgetPin(videoId: String) {
        pins.remove(videoId)
    }

    /** Clears per-track memory, for a cache wipe. Rate limits stand: they are about the server. */
    fun clearTracks() {
        missedAt.clear()
        pins.clear()
    }

    companion object {
        const val MISS_MEMORY_MS = 10 * 60 * 1000L
        const val MIN_COOL_DOWN_MS = 2 * 60 * 1000L
        const val MAX_COOL_DOWN_MS = 15 * 60 * 1000L

        /**
         * Comfortably inside [StreamChoice]'s own limits, so a waiting pin is
         * replaced by read-ahead before playback would have to distrust it.
         */
        const val PIN_REFRESH_MS = 10 * 60 * 1000L

        private const val MAX_REMEMBERED = 64
    }
}
