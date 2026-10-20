package com.music.bitchord

import com.music.bitchord.data.sources.SourceConfig
import com.music.bitchord.data.sources.SourceKind
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** The per-addon read-ahead switch, as it is stored and as it rebuilds (or doesn't) a source. */
class AddonPrefetchSettingTest {

    // Configured like the registry's own.
    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    @Test
    fun `an addon stored before the switch existed reads as off`() {
        val stored = """{"id":"a","kind":"ADDON","label":"Nebula","baseUrl":"https://nebula.example"}"""
        val config = json.decodeFromString(SourceConfig.serializer(), stored)
        assertFalse(config.prefetchNext)
    }

    @Test
    fun `the switch survives a round trip`() {
        val on = SourceConfig(id = "a", kind = SourceKind.ADDON, baseUrl = "https://nebula.example", prefetchNext = true)
        val back = json.decodeFromString(SourceConfig.serializer(), json.encodeToString(SourceConfig.serializer(), on))
        assertTrue(back.prefetchNext)
    }

    @Test
    fun `flipping the switch is not an edit that rebuilds the source`() {
        val built = SourceConfig(id = "a", kind = SourceKind.ADDON, baseUrl = "https://nebula.example")
        val flipped = built.copy(prefetchNext = true)
        // [SourceRegistry.configuredBy]'s test: equal means the warm instance,
        // and its client's rate-limit memory, are kept.
        assertEquals(flipped, built.withPolicyOf(flipped))
    }

    @Test
    fun `moving the address still does`() {
        val built = SourceConfig(id = "a", kind = SourceKind.ADDON, baseUrl = "https://nebula.example")
        val moved = built.copy(baseUrl = "https://elsewhere.example", prefetchNext = true)
        assertFalse(moved == built.withPolicyOf(moved))
    }
}
