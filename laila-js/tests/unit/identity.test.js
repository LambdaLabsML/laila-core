/**
 * Identifiable objects: port of
 *   tests/functional/atomics/identifiable_object/unit_tests/test_identifiable_object.py
 *
 * Creation timestamps, the ``LAILA:scopes:<uuid>[@key=value,...]`` global-id
 * grammar, nickname -> UUID-5 derivation and the active namespace.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";

const { default: laila, S } = await import("./fixtures/laila_root.js");
const E = await import(S + "_compat/errors.js");
const T = await import(S + "_compat/pytypes.js");
const UU = await import(S + "_compat/uuid.js");
const DT = await import(S + "_compat/datetime.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const { loads } = await import(S + "_compat/pyjson.js");
const { AtomicDict, AtomicDotMap, AtomicFlag, AtomicInt, AtomicList, AtomicStr } = await import(S + "atomic/index.js");
const { _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT } = await import(S + "atomic/definitions/index.js");
const { _LAILA_IDENTIFIABLE_OBJECT } = await import(S + "basics/definitions/identifiable_object.js");
const { _LAILA_OBJECT } = await import(S + "basics/definitions/laila_object.js");
const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

/** ``str(uuid.uuid4())`` */
const uuid4 = () => String(UU.uuid4());
/** ``uuid.UUID(s)`` -- raises ``ValueError`` when *s* is not a uuid. */
const UUID = (s) => new UU.UUID(s);
/** ``datetime.fromisoformat(s)`` -- raises ``ValueError`` when malformed. */
const fromisoformat = (s) => DT.fromisoformat(s);
/** ``datetime.fromisoformat(s).utcoffset() == UTC.utcoffset(None)`` (offset zero). */
const has_utc_offset = (s) => /(?:Z|[+-]00:?00)$/.test(s);

// ── TestCreationTimestamp ────────────────────────────────────────────────

describe("TestCreationTimestamp", () => {
  test("test_root_object_is_stamped_with_iso_utc", () => {
    const obj = new _LAILA_OBJECT();
    assert.equal(typeof obj.creation_timestamp, "string");
    fromisoformat(obj.creation_timestamp);
    assert.ok(has_utc_offset(obj.creation_timestamp));
  });

  test("test_identifiable_object_inherits_stamp", () => {
    assert.ok(_LAILA_IDENTIFIABLE_OBJECT.prototype instanceof _LAILA_OBJECT); // issubclass
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    fromisoformat(obj.creation_timestamp);
  });

  test("test_sequential_construction_is_non_decreasing", () => {
    const a = new _LAILA_IDENTIFIABLE_OBJECT();
    const b = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.ok(a.creation_timestamp <= b.creation_timestamp);
  });

  test("test_stamp_is_read_only", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    // Python raises ``AttributeError`` (property without a setter). Strict-mode
    // JS rejects assignment to a getter-only accessor with the language's own
    // ``TypeError`` -- the same contract, spelled by the runtime.
    assert.throws(
      () => {
        obj.creation_timestamp = "2000-01-01T00:00:00.000+00:00";
      },
      (e) => e instanceof E.AttributeError || e instanceof TypeError,
    );
    assert.notEqual(obj.creation_timestamp, "2000-01-01T00:00:00.000+00:00");
  });

  test("test_identity_still_applied_through_atomic_diamond", () => {
    // _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT(LAO, IDO): the staged uuid
    // must survive Pydantic's injected model_post_init on the LAO side.
    const uid = uuid4();
    const obj = new _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT({ uuid: uid, evolution: 3 });
    assert.equal(obj.uuid, uid);
    assert.equal(obj.evolution, 3);
    fromisoformat(obj.creation_timestamp);
  });

  test("test_atomic_types_are_stamped", () => {
    for (const cls of [AtomicInt, AtomicDict, AtomicList, AtomicFlag, AtomicStr, AtomicDotMap]) {
      // subTest(cls=cls.__name__)
      const obj = new cls();
      assert.ok(obj instanceof _LAILA_OBJECT, cls.name);
      fromisoformat(obj.creation_timestamp);
    }
  });

  test("test_atomic_dotmap_stamp_not_leaked_into_data", () => {
    const m = new AtomicDotMap();
    m.foo = 1;
    assert.deepEqual(m.to_dict(), { foo: 1 });
    assert.ok(!m.keys().includes("_creation_timestamp"));
  });
});

// ── TestGlobalIdFormat ───────────────────────────────────────────────────

describe("TestGlobalIdFormat", () => {
  test("test_valid_global_id_without_evolution", () => {
    const uid = uuid4();
    const gid = `LAILA:OBJECT:${uid}`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid), true);
  });

  test("test_valid_global_id_with_evolution", () => {
    const uid = uuid4();
    const gid = `LAILA:OBJECT:${uid}@evolution=0`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid), true);
  });

  test("test_invalid_global_id_empty_string", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(""), false);
  });

  test("test_invalid_global_id_random_string", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource("not-a-global-id"), false);
  });

  test("test_invalid_global_id_no_scopes", () => {
    const uid = uuid4();
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(uid), false);
  });

  test("test_invalid_global_id_missing_colon", () => {
    const uid = uuid4();
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(`LAILA${uid}`), false);
  });

  test("test_process_global_id_bare_string_is_entry_nickname", () => {
    // A scope-less reference is an ENTRY nickname, not an error.
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id("garbage");
    assert.deepEqual(parsed.scopes, ["ENTRY"]);
    assert.equal(parsed.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("garbage"));
    assert.deepEqual(parsed, _LAILA_IDENTIFIABLE_OBJECT.process_global_id("ENTRY:garbage"));
  });

  test("test_process_global_id_structurally_malformed_raises", () => {
    const uid = uuid4();
    for (const bad of ["A::b", ":x", "LAILA:", `LAILA:ENTRY:${uid.slice(0, -1)}`, `LAILA:ENTRY:${uid}-3`]) {
      // subTest(bad=bad)
      assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(bad), E.ValueError, bad);
    }
  });

  test("test_process_global_id_empty_raises", () => {
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(""), E.ValueError);
  });

  test("test_process_global_id_none_raises", () => {
    assert.throws(
      () => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(null),
      (e) => e instanceof TypeError || e instanceof E.ValueError,
    );
  });

  test("test_process_global_id_extracts_uuid", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}`;
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.equal(parsed.uuid, uid);
  });

  test("test_process_global_id_extracts_scopes", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}`;
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.ok(parsed.scopes.includes("ENTRY"));
  });

  test("test_process_global_id_extracts_evolution", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@evolution=42`;
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.equal(parsed.evolution, 42);
  });

  test("test_process_global_id_no_evolution_returns_none", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}`;
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.equal(parsed.evolution, null);
  });
});

// ── TestToGlobalId ───────────────────────────────────────────────────────

describe("TestToGlobalId", () => {
  test("test_basic_to_global_id", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: ["ENTRY"] });
    assert.ok(gid.includes(uid));
    assert.ok(gid.startsWith("LAILA:"));
  });

  test("test_to_global_id_with_evolution", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: ["ENTRY"], evolution: 5 });
    assert.ok(gid.endsWith("@evolution=5"));
  });

  test("test_to_global_id_none_scopes_uses_default", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: null });
    assert.ok(gid.includes("OBJECT"));
  });

  test("test_to_global_id_empty_scopes_uses_default", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: [] });
    assert.ok(gid.includes("OBJECT"));
  });

  test("test_to_global_id_with_nickname", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ nickname: "test-nick", scopes: ["ENTRY"] });
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid), true);
  });

  test("test_to_global_id_round_trip", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: ["ENTRY"], evolution: 3 });
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.equal(parsed.uuid, uid);
    assert.equal(parsed.evolution, 3);
  });
});

// ── TestGenerateUuidFromNickname ─────────────────────────────────────────

describe("TestGenerateUuidFromNickname", () => {
  test("test_deterministic", () => {
    const a = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("hello");
    const b = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("hello");
    assert.equal(a, b);
  });

  test("test_different_nicknames_differ", () => {
    const a = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("alpha");
    const b = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("beta");
    assert.notEqual(a, b);
  });

  test("test_result_is_valid_uuid", () => {
    const result = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("test");
    UUID(result);
  });

  test("test_empty_string_nickname", () => {
    const result = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("");
    UUID(result);
  });

  test("test_none_nickname_raises", () => {
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(null), E.TypeError);
  });

  test("test_unicode_nickname", () => {
    const result = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("\u2603snowman");
    UUID(result);
  });
});

// ── TestIdentifiableObjectInit ───────────────────────────────────────────

describe("TestIdentifiableObjectInit", () => {
  test("test_default_creates_random_uuid", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    UUID(obj.uuid);
  });

  test("test_two_defaults_differ", () => {
    const a = new _LAILA_IDENTIFIABLE_OBJECT();
    const b = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.notEqual(a.uuid, b.uuid);
  });

  test("test_explicit_uuid", () => {
    const uid = uuid4();
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: uid });
    assert.equal(obj.uuid, uid);
  });

  test("test_nickname_overrides_uuid", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: "should-be-overridden", nickname: "winner" });
    const expected = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("winner");
    assert.equal(obj.uuid, expected);
  });

  test("test_evolution_stored", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 7 });
    assert.equal(obj.evolution, 7);
  });

  test("test_evolution_none_by_default", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.equal(obj.evolution, null);
  });

  test("test_has_evolution_true", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 0 });
    assert.equal(obj.has_evolution(), true);
  });

  test("test_has_evolution_false", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.equal(obj.has_evolution(), false);
  });

  test("test_global_id_property", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    const gid = obj.global_id;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid), true);
  });

  test("test_global_id_setter", () => {
    const uid = uuid4();
    const gid = `LAILA:CUSTOM:${uid}@evolution=10`;
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    obj.global_id = gid;
    assert.equal(obj.uuid, uid);
    assert.equal(obj.evolution, 10);
  });

  test("test_global_id_setter_bare_string_rebinds_to_entry_nickname", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    obj.global_id = "not-valid";
    assert.deepEqual(obj.scopes, ["ENTRY"]);
    assert.equal(obj.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("not-valid"));
  });

  test("test_global_id_setter_malformed_raises", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.throws(() => {
      obj.global_id = "A::b";
    }, E.ValueError);
  });

  test("test_str_and_repr", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.equal(T.str(obj), obj.global_id);
    assert.equal(repr(obj), obj.global_id);
  });

  test("test_hash_consistent", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    // ``hash(obj) == hash(obj.global_id)``: the port hashes by the global-id
    // string itself (``__hash__`` returns the ``Map`` key).
    assert.equal(obj.__hash__(), obj.global_id);
  });

  test("test_identity_dict", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 3 });
    const ident = obj.identity();
    assert.ok("uuid" in ident);
    assert.ok("evolution" in ident);
    assert.equal(ident.evolution, 3);
  });

  test("test_identity_as_json", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT();
    const j = obj.identity_as_json();
    const parsed = loads(j);
    assert.equal(parsed.uuid, obj.uuid);
  });

  test("test_type_variable", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@evolution=0`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(gid), "variable");
  });

  test("test_type_constant", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(gid), "constant");
  });

  test("test_from_global_id_valid", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@evolution=5`;
    const obj = _LAILA_IDENTIFIABLE_OBJECT.from_global_id(gid);
    assert.equal(obj.uuid, uid);
    assert.equal(obj.evolution, 5);
  });

  test("test_from_global_id_bare_string_is_entry_nickname", () => {
    const obj = _LAILA_IDENTIFIABLE_OBJECT.from_global_id("bad-id");
    assert.deepEqual(obj.scopes, ["ENTRY"]);
    assert.equal(obj.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("bad-id"));
  });

  test("test_from_global_id_malformed_raises", () => {
    const uid = uuid4();
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.from_global_id(`LAILA:ENTRY:${uid}-5`), E.ValueError);
  });

  test("test_bare_reference_defaults_to_entry_scope", () => {
    const base_gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("run-3");
    assert.equal(base_gid, _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:run-3"));
    assert.ok(base_gid.startsWith("LAILA:ENTRY:"));
    // A bare *object* keeps the OBJECT scope; only references default to ENTRY.
    assert.deepEqual(new _LAILA_IDENTIFIABLE_OBJECT().scopes, ["OBJECT"]);
    // Subclasses keep their own default for scope-less references.
    assert.ok(Manifest.resolve_global_id("x").startsWith("LAILA:MANIFEST:"));
  });

  test("test_truncated_uuid_tail_is_rejected", () => {
    const uid = uuid4();
    for (const tail of [uid.slice(0, -1), uid + "0", uid.replace(/-/g, "")]) {
      // subTest(tail=tail)
      assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(`ENTRY:${tail}`), E.ValueError, tail);
    }
  });

  test("test_negative_evolution_allowed_in_reference_only", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:x@evolution=-1");
    assert.ok(gid.endsWith("@evolution=-1"));
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid), E.ValueError);
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:x@evolution=--1"), E.ValueError);
  });
});

// ── TestGlobalIdAttributes ───────────────────────────────────────────────

describe("TestGlobalIdAttributes", () => {
  // ``LAILA:scopes:<uuid>[@key=value,...]`` -- attribute suffix grammar.
  const HB = "2026-09-15T17:50:00.123+00:00";

  test("test_no_global_id_marker_in_emitted_ids", () => {
    const obj = new _LAILA_IDENTIFIABLE_OBJECT({ scopes: ["ENTRY"] });
    assert.equal(obj.global_id, `LAILA:ENTRY:${obj.uuid}`);
    assert.ok(!obj.global_id.includes("GLOBAL_ID"));
  });

  test("test_evolution_round_trip", () => {
    const uid = uuid4();
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: uid, scopes: ["A", "B"], evolution: 7 });
    assert.equal(gid, `LAILA:A:B:${uid}@evolution=7`);
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.deepEqual(parsed, { uuid: uid, scopes: ["A", "B"], evolution: 7 });
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.to_global_id(parsed), gid);
  });

  test("test_unknown_attribute_is_ignored_for_identity_but_exposed", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@evolution=2,creation_timestamp=${HB}`;
    const parsed = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid);
    assert.equal(parsed.evolution, 2);
    const attrs = _LAILA_IDENTIFIABLE_OBJECT.get_attributes_from_global_id(gid);
    assert.deepEqual(attrs, { evolution: "2", creation_timestamp: HB });
    // Re-emitting never carries foreign attributes.
    const obj = _LAILA_IDENTIFIABLE_OBJECT.from_global_id(gid);
    assert.equal(obj.global_id, `LAILA:ENTRY:${uid}@evolution=2`);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.strip_global_id_attributes(gid), `LAILA:ENTRY:${uid}`);
  });

  test("test_creation_timestamp_only_attribute_is_a_constant_identity", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@creation_timestamp=${HB}`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid), true);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid).evolution, null);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(gid), "constant");
  });

  test("test_malformed_suffixes_raise", () => {
    const uid = uuid4();
    for (const bad of [
      `LAILA:ENTRY:${uid}@`,
      `LAILA:ENTRY:${uid}@evolution`,
      `LAILA:ENTRY:${uid}@=3`,
      `LAILA:ENTRY:${uid}@evolution=3,`,
      `LAILA:ENTRY:${uid}@evolution=x`,
      `LAILA:ENTRY:${uid}@evolution=1,evolution=2`,
      `LAILA:ENTRY:${uid}@1abc=2`,
    ]) {
      // subTest(bad=bad)
      assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(bad), E.ValueError, bad);
    }
  });

  test("test_legacy_dash_suffix_is_rejected", () => {
    const uid = uuid4();
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(`LAILA:ENTRY:${uid}-3`), false);
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${uid}-3`), E.ValueError);
  });

  test("test_shorthand_with_evolution_attribute", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:counter@evolution=3");
    const expected_uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("counter");
    assert.equal(gid, `LAILA:ENTRY:${expected_uid}@evolution=3`);
  });

  test("test_shorthand_passes_search_attributes_through", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(`ENTRY:counter@creation_timestamp=${HB}`);
    const expected_uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("counter");
    assert.equal(gid, `LAILA:ENTRY:${expected_uid}@creation_timestamp=${HB}`);
  });

  test("test_explicit_evolution_kwarg_overrides_attribute", () => {
    const uid = uuid4();
    let gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(`LAILA:ENTRY:${uid}`, { evolution: 4 });
    assert.equal(gid, `LAILA:ENTRY:${uid}@evolution=4`);
    gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(`ENTRY:${uid}@evolution=1`, { evolution: 4 });
    assert.equal(gid, `LAILA:ENTRY:${uid}@evolution=4`);
  });

  test("test_nickname_with_dash_digits_is_literal", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:run-3");
    const expected_uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("run-3");
    assert.equal(gid, `LAILA:ENTRY:${expected_uid}`);
  });

  test("test_parse_evolution_false_takes_tail_literally", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id("ENTRY:run@evolution=3", { parse_evolution: false });
    const expected_uid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("run@evolution=3");
    assert.equal(gid, `LAILA:ENTRY:${expected_uid}`);
  });

  test("test_full_gid_with_attributes_is_returned_unchanged", () => {
    const uid = uuid4();
    const gid = `LAILA:ENTRY:${uid}@evolution=2,creation_timestamp=${HB}`;
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(gid), gid);
  });
});

// ── TestSetActiveNamespace ───────────────────────────────────────────────

describe("TestSetActiveNamespace", () => {
  afterEach(() => {
    laila._active_namespace = null;
  });

  test("test_set_active_namespace_changes_uuid_generation", () => {
    const default_uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("ns-test");
    laila.set_active_namespace("custom-namespace");
    const custom_uuid = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("ns-test");
    assert.notEqual(default_uuid, custom_uuid);
  });

  test("test_set_active_namespace_is_deterministic", () => {
    laila.set_active_namespace("ns1");
    const a = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("test");
    laila.set_active_namespace("ns1");
    const b = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("test");
    assert.equal(a, b);
  });
});
