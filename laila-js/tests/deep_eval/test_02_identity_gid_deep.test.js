/**
 * Port of ``tests/deep_eval/test_02_identity_gid_deep.py``.
 *
 * Deep tests for identity: UUIDs, nicknames, scopes and global-id grammar.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { S, laila, macrotask } from "./_fixtures.js";

const T = await import(S + "_compat/pytypes.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const _uuid = await import(S + "_compat/uuid.js");
const { repr } = await import(S + "_compat/pyrepr.js");
const {
  _LAILA_IDENTIFIABLE_OBJECT,
  EVOLUTION_ATTRIBUTE,
  GLOBAL_ID_REGEX_PATTERN,
  format_global_id_attributes,
  parse_global_id_attributes,
  split_global_id_attributes,
} = await import(S + "basics/definitions/identifiable_object.js");
const { Entry } = await import(S + "entry/entry.js");
const { _ENTRY_SCOPE, _OBJECT_SCOPE, _TOPMOST_SCOPE } = await import(S + "macros/strings.js");

const U1 = "11111111-2222-3333-4444-555555555555";
const U2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

const str = (x) => T.str(x);
const t = (name, fn) => test(name, () => macrotask(fn));
const raises_match = (cls, re) => (e) => e instanceof cls && re.test(e.message);

// ---------------------------------------------------------------------------
// Attribute helpers
// ---------------------------------------------------------------------------

describe("TestParseAttributes", () => {
  for (const raw of [null, ""]) {
    test(`test_empty_inputs[${repr(raw)}]`, () => {
      assert.deepEqual(parse_global_id_attributes(raw), {});
    });
  }

  test("test_single", () => {
    assert.deepEqual(parse_global_id_attributes("evolution=3"), { evolution: "3" });
  });

  test("test_multiple_preserve_order", () => {
    const out = parse_global_id_attributes("b=2,a=1");
    assert.deepEqual(Object.entries(out), [
      ["b", "2"],
      ["a", "1"],
    ]);
  });

  test("test_value_may_be_empty", () => {
    assert.deepEqual(parse_global_id_attributes("k="), { k: "" });
  });

  test("test_timestamp_value_with_colons_and_plus", () => {
    const ts = "2026-01-02T03:04:05.678+00:00";
    assert.deepEqual(parse_global_id_attributes(`creation_timestamp=${ts}`), { creation_timestamp: ts });
  });

  test("test_negative_evolution_kept_raw", () => {
    assert.deepEqual(parse_global_id_attributes("evolution=-1"), { evolution: "-1" });
  });

  for (const bad of ["evolution", "=3", "1abc=2", "a=1,", ",a=1", "a=1,,b=2", "a b=1", "a=1@x", "a=x,y"]) {
    test(`test_malformed[${repr(bad)}]`, () => {
      assert.throws(() => parse_global_id_attributes(bad), E.ValueError);
    });
  }

  test("test_duplicate_key", () => {
    assert.throws(() => parse_global_id_attributes("a=1,a=2"), raises_match(E.ValueError, /Duplicate/));
  });

  test("test_underscore_key", () => {
    assert.deepEqual(parse_global_id_attributes("_k=1"), { _k: "1" });
  });
});

describe("TestFormatAttributes", () => {
  test("test_empty", () => {
    assert.equal(format_global_id_attributes({}), "");
  });

  test("test_skips_none", () => {
    assert.equal(format_global_id_attributes({ evolution: null, a: 1 }), "a=1");
  });

  test("test_order", () => {
    assert.equal(format_global_id_attributes({ b: 2, a: 1 }), "b=2,a=1");
  });

  test("test_roundtrip", () => {
    const src = { evolution: "4", creation_timestamp: "2026-01-01T00:00:00.000+00:00" };
    assert.deepEqual(parse_global_id_attributes(format_global_id_attributes(src)), src);
  });

  test("test_zero_is_not_none", () => {
    assert.equal(format_global_id_attributes({ evolution: 0 }), "evolution=0");
  });
});

describe("TestSplitAttributes", () => {
  test("test_no_at", () => {
    assert.deepEqual(split_global_id_attributes("LAILA:ENTRY:x"), ["LAILA:ENTRY:x", {}]);
  });

  test("test_with_attributes", () => {
    const [head, attrs] = split_global_id_attributes(`LAILA:ENTRY:${U1}@evolution=2`);
    assert.equal(head, `LAILA:ENTRY:${U1}`);
    assert.deepEqual(attrs, { evolution: "2" });
  });

  test("test_trailing_at_rejected", () => {
    assert.throws(() => split_global_id_attributes("LAILA:ENTRY:x@"), E.ValueError);
  });

  test("test_double_at_rejected", () => {
    assert.throws(() => split_global_id_attributes("x@a=1@b=2"), E.ValueError);
  });
});

// ---------------------------------------------------------------------------
// Regex
// ---------------------------------------------------------------------------

describe("TestRegex", () => {
  for (const gid of [
    `LAILA:ENTRY:${U1}`,
    `LAILA:ENTRY:${U1}@evolution=0`,
    `LAILA:A:B:C:${U1}@evolution=10,creation_timestamp=2026-01-01T00:00:00.000+00:00`,
    `X:${U1}`,
    `LAILA:ENTRY:${U1.toUpperCase()}`,
  ]) {
    test(`test_accepts[${gid}]`, () => {
      assert.ok(GLOBAL_ID_REGEX_PATTERN.exec(gid));
      assert.ok(_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid));
    });
  }

  for (const gid of [
    U1,
    `LAILA:ENTRY:${U1.slice(0, -1)}`,
    `LAILA:ENTRY:${U1}x`,
    `LAILA:ENTRY:${U1}@`,
    `LAILA:ENTRY:${U1}@evolution`,
    `LAILA::${U1}`,
    `LAILA:EN TRY:${U1}`,
    `LAILA:ENTRY:${U1}@evolution=1,`,
    "",
    `LAILA:ENTRY:${U1} `,
  ]) {
    test(`test_rejects[${repr(gid)}]`, () => {
      assert.equal(GLOBAL_ID_REGEX_PATTERN.exec(gid), null);
      assert.ok(!_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid));
    });
  }

  test("test_regex_accepts_malformed_dash_layout", () => {
    // Documents that the uuid group is a 36-char hex/dash class, not RFC-4122.
    assert.ok(GLOBAL_ID_REGEX_PATTERN.exec("LAILA:ENTRY:" + "-".repeat(36)));
  });
});

// ---------------------------------------------------------------------------
// process_global_id / getters
// ---------------------------------------------------------------------------

describe("TestProcessGlobalId", () => {
  test("test_basic", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${U1}`);
    assert.deepEqual(out, { uuid: U1, scopes: ["ENTRY"], evolution: null });
  });

  test("test_evolution", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${U1}@evolution=7`);
    assert.equal(out.evolution, 7);
  });

  test("test_nested_scopes", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:A:B:C:${U1}`);
    assert.deepEqual(out.scopes, ["A", "B", "C"]);
  });

  test("test_other_attributes_ignored", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${U1}@creation_timestamp=2026-01-01T00:00:00.000+00:00`);
    assert.equal(out.evolution, null);
  });

  test("test_negative_evolution_rejected", () => {
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${U1}@evolution=-1`), E.ValueError);
  });

  test("test_non_numeric_evolution_rejected", () => {
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(`LAILA:ENTRY:${U1}@evolution=x`), E.ValueError);
  });

  for (const bad of ["", null, 5, "LAILA:", ":x", U1.slice(0, 30)]) {
    test(`test_invalid_inputs[${repr(bad)}]`, () => {
      assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.process_global_id(bad), E.ValueError);
    });
  }

  test("test_shorthand_scope_nickname", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id("POLICY:trainer");
    assert.deepEqual(out.scopes, ["POLICY"]);
    assert.equal(out.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("trainer"));
  });

  test("test_bare_nickname_is_entry", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.process_global_id("counter");
    assert.deepEqual(out.scopes, [_ENTRY_SCOPE]);
  });

  test("test_getters", () => {
    const gid = `LAILA:X:Y:${U1}@evolution=2`;
    assert.deepEqual(_LAILA_IDENTIFIABLE_OBJECT.get_scopes_from_global_id(gid), ["X", "Y"]);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.get_uuid_from_global_id(gid), U1);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.get_evolution_from_global_id(gid), 2);
  });

  test("test_get_evolution_none", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.get_evolution_from_global_id(`LAILA:ENTRY:${U1}`), null);
  });

  test("test_get_attributes_keeps_all", () => {
    const gid = `LAILA:ENTRY:${U1}@evolution=-1,creation_timestamp=2026`;
    const attrs = _LAILA_IDENTIFIABLE_OBJECT.get_attributes_from_global_id(gid);
    assert.deepEqual(attrs, { evolution: "-1", creation_timestamp: "2026" });
  });

  test("test_get_attributes_shorthand", () => {
    const attrs = _LAILA_IDENTIFIABLE_OBJECT.get_attributes_from_global_id("counter@evolution=3");
    assert.deepEqual(attrs, { evolution: "3" });
  });

  test("test_strip_attributes", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.strip_global_id_attributes(`LAILA:ENTRY:${U1}@evolution=1`), `LAILA:ENTRY:${U1}`);
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.strip_global_id_attributes(`LAILA:ENTRY:${U1}`), `LAILA:ENTRY:${U1}`);
  });

  test("test_type_variable_constant", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(`LAILA:ENTRY:${U1}@evolution=0`), "variable");
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(`LAILA:ENTRY:${U1}`), "constant");
  });
});

// ---------------------------------------------------------------------------
// to_global_id
// ---------------------------------------------------------------------------

describe("TestToGlobalId", () => {
  test("test_basic", () => {
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes: ["ENTRY"] }), `LAILA:ENTRY:${U1}`);
  });

  test("test_evolution", () => {
    assert.ok(_LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes: ["ENTRY"], evolution: 3 }).endsWith("@evolution=3"));
  });

  test("test_evolution_zero_emitted", () => {
    assert.ok(_LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes: ["E"], evolution: 0 }).endsWith("@evolution=0"));
  });

  for (const scopes of [null, []]) {
    test(`test_default_scope_object[${repr(scopes)}]`, () => {
      assert.equal(_LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes }), `LAILA:${_OBJECT_SCOPE}:${U1}`);
    });
  }

  test("test_nickname_overrides_uuid", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes: ["E"], nickname: "n" });
    assert.ok(!gid.includes(U1));
    assert.ok(gid.includes(_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("n")));
  });

  test("test_roundtrip_with_process", () => {
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: U1, scopes: ["A", "B"], evolution: 5 });
    assert.deepEqual(_LAILA_IDENTIFIABLE_OBJECT.process_global_id(gid), { uuid: U1, scopes: ["A", "B"], evolution: 5 });
  });

  test("test_no_validation_of_uuid", () => {
    // Documents that ``to_global_id`` does not validate the uuid segment.
    const gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id({ uuid: "not-a-uuid", scopes: ["E"] });
    assert.equal(gid, "LAILA:E:not-a-uuid");
    assert.ok(!_LAILA_IDENTIFIABLE_OBJECT.is_laila_resource(gid));
  });
});

// ---------------------------------------------------------------------------
// resolve_global_id
// ---------------------------------------------------------------------------

describe("TestResolveGlobalId", () => {
  const R = (ref, opts = {}) => _LAILA_IDENTIFIABLE_OBJECT.resolve_global_id(ref, opts);

  test("test_full_gid_unchanged", () => {
    const gid = `LAILA:ENTRY:${U1}@evolution=3`;
    assert.equal(R(gid), gid);
  });

  test("test_full_gid_with_search_attrs_unchanged", () => {
    const gid = `LAILA:ENTRY:${U1}@evolution=-1,creation_timestamp=2026`;
    assert.equal(R(gid), gid);
  });

  test("test_scope_nickname", () => {
    const out = R("MANIFEST:my_dataset");
    assert.equal(out, `LAILA:MANIFEST:${_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("my_dataset")}`);
  });

  test("test_scope_uuid_verbatim", () => {
    assert.equal(R(`POLICY:${U1}`), `LAILA:POLICY:${U1}`);
  });

  test("test_scope_nickname_with_evolution", () => {
    const out = R("ENTRY:counter@evolution=3");
    assert.ok(out.endsWith("@evolution=3"));
    assert.ok(out.startsWith("LAILA:ENTRY:"));
  });

  test("test_negative_evolution_reference", () => {
    assert.ok(R("ENTRY:counter@evolution=-1").endsWith("@evolution=-1"));
  });

  test("test_evolution_kwarg_overrides_attribute", () => {
    assert.ok(R("ENTRY:counter@evolution=3", { evolution: 9 }).endsWith("@evolution=9"));
  });

  test("test_evolution_kwarg_on_full_gid_overrides", () => {
    const out = R(`LAILA:ENTRY:${U1}@evolution=0`, { evolution: 4 });
    assert.equal(out, `LAILA:ENTRY:${U1}@evolution=4`);
  });

  test("test_evolution_kwarg_matching_full_gid_unchanged", () => {
    const gid = `LAILA:ENTRY:${U1}@evolution=4`;
    assert.equal(R(gid, { evolution: 4 }), gid);
  });

  test("test_bare_nickname_defaults_to_entry", () => {
    assert.ok(R("run-3").startsWith("LAILA:ENTRY:"));
  });

  test("test_bare_nickname_default_scopes_override", () => {
    assert.ok(R("run-3", { default_scopes: ["Q"] }).startsWith("LAILA:Q:"));
  });

  test("test_subclass_default_scopes", async () => {
    assert.ok(Entry.resolve_global_id("x").startsWith("LAILA:ENTRY:"));
    const { Manifest } = await import(S + "policy/central/memory/schema/manifest.js");

    assert.ok(Manifest.resolve_global_id("x").startsWith("LAILA:MANIFEST:"));
  });

  test("test_explicit_prefix_not_doubled", () => {
    const out = R("LAILA:MANIFEST:my_dataset");
    assert.equal(out.split("LAILA").length - 1, 1);
    assert.ok(out.startsWith("LAILA:MANIFEST:"));
  });

  test("test_custom_prefix_scopes", () => {
    const out = R("POLICY:trainer", { prefix_scopes: ["ROOT"] });
    assert.ok(out.startsWith("ROOT:POLICY:"));
  });

  test("test_custom_prefix_stripped_when_present", () => {
    const out = R("ROOT:POLICY:trainer", { prefix_scopes: ["ROOT"] });
    assert.equal(out.split("ROOT").length - 1, 1);
  });

  test("test_parse_evolution_false_literal_nickname", () => {
    const out = R("run@evolution=3", { parse_evolution: false });
    assert.ok(!out.includes("@"));
    assert.ok(out.endsWith(_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("run@evolution=3")));
  });

  for (const bad of ["POLICY:", ":trainer", "A::b", "", "x@", "x@evolution=abc"]) {
    test(`test_invalid[${repr(bad)}]`, () => {
      assert.throws(() => R(bad), E.ValueError);
    });
  }

  test("test_non_string", () => {
    assert.throws(() => R(123), E.ValueError);
  });

  test("test_uuid_like_truncated_rejected", () => {
    assert.throws(() => R(`ENTRY:${U1.slice(0, -2)}`), E.ValueError);
  });

  test("test_uuid_like_extended_rejected", () => {
    assert.throws(() => R(`ENTRY:${U1}ab`), E.ValueError);
  });

  test("test_hex_only_nickname_of_length_32_rejected", () => {
    // Documents: a legitimate 32-hex nickname (e.g. md5 digest) is rejected.
    assert.throws(() => R("ENTRY:" + "a".repeat(32)), E.ValueError);
  });

  test("test_other_attribute_carried", () => {
    const out = R("ENTRY:x@creation_timestamp=2026-01-01T00:00:00.000+00:00");
    assert.ok(out.includes("creation_timestamp=2026-01-01T00:00:00.000+00:00"));
  });

  test("test_attribute_order_preserved", () => {
    const out = R("ENTRY:x@creation_timestamp=t,evolution=2");
    assert.ok(out.endsWith("@creation_timestamp=t,evolution=2"));
  });

  test("test_deep_scopes", () => {
    const out = R("A:B:C:nick");
    assert.ok(out.startsWith("LAILA:A:B:C:"));
  });

  test("test_full_gid_with_only_prefix_is_not_a_gid", () => {
    // ``LAILA:<uuid>`` has no domain scope -> treated as shorthand and re-scoped.
    const out = R(`LAILA:${U1}`);
    assert.equal(out, `LAILA:ENTRY:${U1}`);
  });

  test("test_module_level_resolve", () => {
    assert.equal(laila.resolve_global_id("POLICY:p"), R("POLICY:p"));
  });
});

// ---------------------------------------------------------------------------
// Nickname hashing / namespaces
// ---------------------------------------------------------------------------

describe("TestNicknames", () => {
  test("test_deterministic", () => {
    const a = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("x");
    const b = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("x");
    assert.equal(a, b);
  });

  test("test_matches_uuid5", () => {
    const ns = laila.get_active_namespace();
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("abc"), str(_uuid.uuid5(ns, "abc")));
  });

  test("test_different_nicknames_differ", () => {
    assert.notEqual(_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("a"), _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("b"));
  });

  test("test_case_sensitive", () => {
    assert.notEqual(_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("A"), _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("a"));
  });

  test("test_namespace_switch_changes_uuid", () => {
    const original = laila.get_active_namespace();
    try {
      const before = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("x");
      laila.set_active_namespace("deep.eval.ns");
      const after = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("x");
      assert.notEqual(before, after);
      assert.ok(T.eq(laila.get_active_namespace(), _uuid.uuid5(_uuid.NAMESPACE_DNS, "deep.eval.ns")));
    } finally {
      // ``laila._active_namespace = original``: the module-level pointer is
      // not writable from outside in JS; the default namespace is
      // ``uuid5(NAMESPACE_DNS, "laila")`` so re-deriving it restores it.
      laila.set_active_namespace("laila");
      assert.ok(T.eq(laila.get_active_namespace(), original));
    }
  });

  test("test_namespace_is_uuid", () => {
    assert.ok(laila.get_active_namespace() instanceof _uuid.UUID);
  });

  test("test_unicode_nickname", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("名前");
    assert.equal(new _uuid.UUID(out).version, 5);
  });

  test("test_empty_nickname_allowed", () => {
    const out = _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("");
    assert.ok(new _uuid.UUID(out));
  });
});

// ---------------------------------------------------------------------------
// Instances
// ---------------------------------------------------------------------------

class _Scoped extends _LAILA_IDENTIFIABLE_OBJECT {
  static _DEFAULT_SCOPES = ["SCOPED"];
}

describe("TestIdentifiableObjectInstances", () => {
  test("test_default_uuid4", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.equal(new _uuid.UUID(o.uuid).version, 4);
    assert.deepEqual(o.scopes, [_OBJECT_SCOPE]);
    assert.equal(o.evolution, null);
  });

  test("test_explicit_uuid", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 }).uuid, U1);
  });

  test("test_uuid_object_coerced_to_str", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: new _uuid.UUID(U1) });
    assert.equal(o.uuid, U1);
    assert.equal(typeof o.uuid, "string");
  });

  test("test_nickname", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ nickname: "n" });
    assert.equal(o.uuid, _LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname("n"));
  });

  test("test_nickname_wins_over_uuid", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ nickname: "n", uuid: U1 });
    assert.notEqual(o.uuid, U1);
  });

  test("test_explicit_scopes", () => {
    assert.deepEqual(new _LAILA_IDENTIFIABLE_OBJECT({ scopes: ["A", "B"] }).scopes, ["A", "B"]);
  });

  test("test_scopes_copied", () => {
    const s = ["A"];
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ scopes: s });
    s.push("B");
    assert.deepEqual(o.scopes, ["A"]);
  });

  test("test_evolution", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 3 }).evolution, 3);
  });

  test("test_evolution_zero", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 0 });
    assert.equal(o.evolution, 0);
    assert.ok(o.global_id.endsWith("@evolution=0"));
    assert.ok(o.has_evolution());
  });

  test("test_has_evolution_false", () => {
    assert.ok(!new _LAILA_IDENTIFIABLE_OBJECT().has_evolution());
  });

  test("test_subclass_default_scopes", () => {
    assert.deepEqual(new _Scoped().scopes, ["SCOPED"]);
    assert.ok(new _Scoped().global_id.startsWith("LAILA:SCOPED:"));
  });

  test("test_subclass_default_scopes_not_shared", () => {
    const a = new _Scoped();
    a.scopes.push("X");
    assert.deepEqual(new _Scoped().scopes, ["SCOPED"]);
  });

  test("test_from_global_id", () => {
    const o = _LAILA_IDENTIFIABLE_OBJECT.from_global_id(`LAILA:A:${U1}@evolution=2`);
    assert.deepEqual([o.uuid, o.scopes, o.evolution], [U1, ["A"], 2]);
    assert.equal(o.global_id, `LAILA:A:${U1}@evolution=2`);
  });

  test("test_from_global_id_shorthand", () => {
    const o = _LAILA_IDENTIFIABLE_OBJECT.from_global_id("POLICY:p");
    assert.deepEqual(o.scopes, ["POLICY"]);
  });

  test("test_from_global_id_invalid", () => {
    assert.throws(() => _LAILA_IDENTIFIABLE_OBJECT.from_global_id("LAILA:"), E.ValueError);
  });

  test("test_global_id_setter", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT();
    o.global_id = `LAILA:Z:${U2}@evolution=1`;
    assert.deepEqual([o.uuid, o.scopes, o.evolution], [U2, ["Z"], 1]);
  });

  test("test_global_id_setter_invalid", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.throws(() => {
      o.global_id = "garbage:";
    }, E.ValueError);
  });

  test("test_property_setters", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT();
    o.uuid = U2;
    o.scopes = ["Q"];
    o.evolution = 5;
    assert.equal(o.global_id, `LAILA:Q:${U2}@evolution=5`);
  });

  test("test_str_repr", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });
    assert.equal(str(o), o.global_id);
    assert.equal(o.global_id, repr(o));
  });

  test("test_hash_equals_hash_of_gid", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });
    // ``hash(str)`` is the string itself in the JS port
    assert.equal(o.__hash__(), o.global_id);
  });

  test("test_hash_same_for_same_identity", () => {
    assert.equal(new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 }).__hash__(), new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 }).__hash__());
  });

  t("test_equality_follows_identity", () => {
    const a = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });

    time.sleep(0.002);
    const b = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });
    assert.ok(T.eq(a, b));
  });

  test("test_identity_dict", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1, scopes: ["A"], evolution: 2 });
    assert.deepEqual(o.identity(), { uuid: U1, scopes: ["A"], evolution: 2 });
  });

  test("test_identity_omits_none_evolution", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });
    assert.ok(!("evolution" in o.identity()));
  });

  test("test_identity_json", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1 });
    assert.equal(JSON.parse(o.identity_as_json()).uuid, U1);
  });

  test("test_identity_roundtrip", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U1, scopes: ["A"], evolution: 2 });
    const o2 = new _LAILA_IDENTIFIABLE_OBJECT(o.identity());
    assert.equal(o2.global_id, o.global_id);
  });

  test("test_creation_timestamp_set", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT();
    assert.notEqual(o.creation_timestamp, null);
    assert.ok(o.creation_timestamp.endsWith("+00:00"));
  });

  test("test_creation_timestamp_millisecond_precision", () => {
    const ts = new _LAILA_IDENTIFIABLE_OBJECT().creation_timestamp;
    const frac = ts.split(".")[1].split("+")[0];
    assert.equal(frac.length, 3);
  });

  t("test_thread_isolation_of_init_pending", () => {
    // Constructing in many threads must never cross-contaminate identities.
    const results = {};

    const worker = (i) => {
      const o = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: str(_uuid.uuid5(_uuid.NAMESPACE_DNS, `t${i}`)), scopes: [`S${i}`] });
      results[i] = [o.uuid, o.scopes];
    };

    const threads = T.range(32).map((i) => new TH.Thread({ target: worker, args: [i] }));
    for (const th of threads) th.start();
    for (const th of threads) th.join();
    for (const th of threads) if (th.exception) throw th.exception;
    for (const [i, [u, s]] of Object.entries(results)) {
      assert.equal(u, str(_uuid.uuid5(_uuid.NAMESPACE_DNS, `t${i}`)));
      assert.deepEqual(s, [`S${i}`]);
    }
  });

  test("test_nested_construction_restores_pending", () => {
    // Constructing an identifiable inside another's init must not leak identity.

    class Outer extends _LAILA_IDENTIFIABLE_OBJECT {
      model_post_init(__context) {
        super.model_post_init(__context);
        this._inner = new _LAILA_IDENTIFIABLE_OBJECT({ uuid: U2, scopes: ["INNER"] });
      }
    }

    const o = new Outer({ uuid: U1, scopes: ["OUTER"] });
    assert.equal(o.uuid, U1);
    assert.deepEqual(o.scopes, ["OUTER"]);
    assert.equal(o._inner.uuid, U2);
  });

  test("test_evolution_attribute_name_constant", () => {
    assert.equal(EVOLUTION_ATTRIBUTE, "evolution");
  });

  test("test_topmost_scope_constant", () => {
    assert.equal(_TOPMOST_SCOPE, "LAILA");
    assert.ok(new _LAILA_IDENTIFIABLE_OBJECT().global_id.startsWith("LAILA:"));
  });

  test("test_type_of_instance_gid", () => {
    const o = new _LAILA_IDENTIFIABLE_OBJECT({ evolution: 1 });
    assert.equal(_LAILA_IDENTIFIABLE_OBJECT.type(o.global_id), "variable");
  });
});
