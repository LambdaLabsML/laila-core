//! The stdlib shims (`os`, `tempfile`, `threading`, `np`, `time`) spelled
//! as in Python inside `laila::python! { .. }`.
#![allow(non_snake_case, unused_variables)]

use laila::prelude::*;

laila::python! {
    #![unittest]

    py_class!(TestStdlibShims);
    impl TestStdlibShims {
        fn test_tempdir_and_os(&self) {
            let kept = None;
            with tempfile.TemporaryDirectory() as tmp {
                let p = os.path.join(tmp, "sub", "f.txt");
                os.makedirs(os.path.dirname(p), exist_ok=True);
                self.assertTrue(os.path.isdir(os.path.join(tmp, "sub")));
                self.assertFalse(os.path.exists(p));
                self.assertEqual(os.path.basename(p), "f.txt");
                self.assertGreater(os.getpid(), 0);
                self.assertEqual(len(os.urandom(8)), 8);
                kept = str(tmp);
            }
            self.assertFalse(os.path.exists(kept));
        }

        fn test_threads(&self) {
            let results = [];
            let lock = threading.Lock();
            let worker = lambda i: results.append(i * 2);
            let threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)];
            for t in threads {
                t.start();
            }
            for t in threads {
                t.join();
            }
            with lock {
                self.assertEqual(sorted(results), [0, 2, 4, 6]);
            }
            self.assertGreaterEqual(threading.active_count(), 1);
            self.assertGreaterEqual(len(threading.enumerate()), 1);
            let ev = threading.Event();
            self.assertFalse(ev.is_set());
            ev.set();
            self.assertTrue(ev.wait(timeout=0.01));
        }

        fn test_numpy_facade(&self) {
            let a = np.array([[1, 2], [3, 4]], dtype=np.int32);
            self.assertEqual(a.shape, (2, 2));
            self.assertEqual(a.tolist(), [[1, 2], [3, 4]]);
            let b = np.asfortranarray(a);
            self.assertTrue(np.array_equal(a, b));
            self.assertEqual(b.tolist(), [[1, 2], [3, 4]]);
            let z = np.zeros((2, 3), dtype="<f4");
            self.assertEqual(z.dtype, "<f4");
            let l = np.linspace(0.0, 1.0, 5);
            self.assertEqual(l.tolist(), [0.0, 0.25, 0.5, 0.75, 1.0]);
            self.assertEqual(str(np.dtype("<f8")), "float64");
            self.assertTrue(np.isnan(np.nan));
            np.testing.assert_array_equal(np.arange(3), np.array([0, 1, 2]));
            self.assertGreater(time.time_ns(), 0);
        }
    }

    py_class!(TestCodecShims);
    impl TestCodecShims {
        fn test_pickle_roundtrip(&self) {
            let obj = {"a": [1, 2.5, None], "b": (1, 2), "c": b"\x00\xff"};
            let blob = pickle.dumps(obj);
            self.assertEqual(blob[0], 0x80);
            self.assertEqual(pickle.loads(blob), obj);
            self.assertEqual(pickle.loads(pickle.dumps(obj, protocol=4)), obj);
        }

        fn test_msgpack(&self) {
            let blob = msgpack.packb({"k": [1, 2], 3: "x"}, use_bin_type=True);
            self.assertEqual(msgpack.unpackb(blob, raw=False, strict_map_key=False), {"k": [1, 2], 3: "x"});
            with self.assertRaises(TypeError) {
                msgpack.unpackb(blob, bogus=1);
            }
        }

        fn test_base64_zlib(&self) {
            self.assertEqual(base64.b64encode(b"hello"), b"aGVsbG8=");
            self.assertEqual(base64.b64decode("aGVsbG8="), b"hello");
            self.assertEqual(base64.urlsafe_b64encode(b"\xfb\xff"), b"-_8=");
            let data = b"abc" * 100;
            self.assertEqual(zlib.decompress(zlib.compress(data)), data);
            self.assertEqual(zlib.decompress(zlib.compress(data, level=9)), data);
            self.assertEqual(zlib.crc32(b"hello"), 907060870);
            self.assertEqual(binascii.hexlify(b"\x01\xab"), b"01ab");
        }

        fn test_bytesio_np(&self) {
            let buf = io.BytesIO();
            np.save(buf, np.arange(6).reshape(2, 3));
            self.assertGreater(len(buf.getvalue()), 64);
            let arr = np.load(io.BytesIO(buf.getvalue()));
            self.assertEqual(arr.tolist(), [[0, 1, 2], [3, 4, 5]]);
            let b2 = io.BytesIO(b"hello world");
            self.assertEqual(b2.read(5), b"hello");
            self.assertEqual(b2.tell(), 5);
            self.assertEqual(b2.read(), b" world");
        }

        fn test_json(&self) {
            self.assertEqual(json.dumps({"a": [1, 2.5, None, True]}), "{\"a\": [1, 2.5, null, true]}");
            self.assertEqual(json.dumps({"b": 1, "a": 2}, sort_keys=True), "{\"a\": 2, \"b\": 1}");
            self.assertEqual(json.dumps("\u{e9}"), "\"\\u00e9\"");
            self.assertEqual(json.loads("{\"a\": 1}"), {"a": 1});
        }

        fn test_datetime(&self) {
            let d = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC);
            self.assertEqual(d.isoformat(timespec="milliseconds"), "2024-01-02T03:04:05.000+00:00");
            self.assertEqual(d.year, 2024);
            let epoch = datetime.fromtimestamp(0, tz=UTC);
            self.assertEqual(epoch.isoformat().replace("+00:00", "Z"), "1970-01-01T00:00:00Z");
            self.assertEqual(timedelta(hours=1).total_seconds(), 3600.0);
            self.assertEqual(str(timedelta(days=1, seconds=5)), "1 day, 0:00:05");
            self.assertGreaterEqual(datetime.now(UTC).year, 2024);
            self.assertEqual(datetime.fromisoformat("2024-01-02T03:04:05+00:00").timestamp(), 1704164645.0);
        }

        fn test_dotmap(&self) {
            let d = DotMap({"a": {"b": 1}});
            self.assertEqual(d.a.b, 1);
            d.c = 2;
            self.assertEqual(d.toDict(), {"a": {"b": 1}, "c": 2});
            self.assertEqual(d["c"], 2);
            self.assertTrue(isinstance(d, DotMap));
        }

        fn test_fernet(&self) {
            let key = Fernet.generate_key();
            let f = Fernet(key);
            self.assertEqual(f.decrypt(f.encrypt(b"secret")), b"secret");
            with self.assertRaises(InvalidToken) {
                f.decrypt(b"not-a-token");
            }
        }

        fn test_literal_eval(&self) {
            self.assertEqual(ast.literal_eval("{'a': [1, 2], 'b': b'x'}"), {"a": [1, 2], "b": b"x"});
        }
    }
}
