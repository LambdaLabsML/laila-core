/**
 * Cross-implementation vectors shared with ``laila-c``
 * (``laila-c/tests/vectors/compdata.json``): serialized entries written by
 * Python laila. The JS codecs must rebuild every payload through its
 * constitution codes *and* re-serialise the value to the identical bytes.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import * as rc from "../../src/_codecs/recovery_codes.js";
import * as pickle from "../../src/_codecs/pickle.js";
import * as msgpack from "../../src/_codecs/msgpack.js";
import * as npy from "../../src/_codecs/npy.js";
import * as b64 from "../../src/_codecs/base64.js";
import * as pyjson from "../../src/_compat/pyjson.js";
import { eq } from "../../src/_compat/pytypes.js";
import { repr } from "../../src/_compat/pyrepr.js";
import { NDArray } from "../../src/_compat/ndarray.js";
import { LAILA_C_VECTORS } from "./_fixture.js";

const file = path.join(LAILA_C_VECTORS, "compdata.json");
const available = fs.existsSync(file);

describe("laila-c compdata vectors", { skip: available ? false : `missing ${file}` }, () => {
  const vecs = available ? pyjson.loads(fs.readFileSync(file, "utf8")) : [];
  for (const v of vecs) {
    test(`${v.name} (${v.check})`, () => {
      const codes = v.serialized.constitution.codes;
      if (v.check === "none") {
        assert.equal(v.serialized.payload, null);
        assert.equal(codes.length, 0);
        return;
      }
      const infos = codes.map(rc.recognize);
      assert.ok(infos.every((i) => i !== null), "every code recognised");
      let cur = v.serialized.payload;
      for (const c of codes) cur = rc.compile_backward(c)(cur);

      switch (v.check) {
        case "value":
          assert.ok(eq(cur, v.value), `${repr(cur)} != ${repr(v.value)}`);
          break;
        case "bytes":
          assert.ok(Buffer.isBuffer(cur) && cur.equals(Buffer.from(v.b64, "base64")));
          break;
        case "nested_bytes":
          assert.ok(Buffer.from(cur[v.key]).equals(Buffer.from(v.b64, "base64")));
          break;
        case "bignum_unsupported":
          assert.equal(typeof cur, "bigint");
          break;
        case "numpy":
          assert.ok(cur instanceof NDArray);
          assert.equal(cur.dtype, v.dtype);
          assert.ok(eq([...cur.shape], v.shape));
          assert.ok(cur.tobytes().equals(Buffer.from(v.raw_b64, "base64")));
          break;
        default:
          assert.fail(`unknown check ${v.check}`);
      }

      // forward identity: value -> serializer -> base64 == stored payload
      const last = infos[infos.length - 1];
      let bytes = null;
      if (last.name === "pickle") bytes = pickle.dumps(cur);
      else if (last.name === "msgpack") bytes = msgpack.packb(cur, { use_bin_type: true });
      else if (last.name === "numpy") bytes = npy.save(cur);
      if (bytes !== null && (last.name !== "msgpack" || v.mp_identity)) {
        assert.equal(infos[0].name, "base64");
        assert.equal(b64.b64encode(bytes).toString(), v.serialized.payload);
      }
    });
  }
});
