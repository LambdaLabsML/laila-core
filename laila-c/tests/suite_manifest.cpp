// Manifest: blueprint-as-Entry-payload, top-level keys() vs leaf iteration,
// remember() over referenced entries, and realized() rebuilding the structure.
#include <iterator>

#include "laila_test.hpp"
#include "laila/laila.hpp"

using namespace laila_c;

TEST("manifest", "blueprint_keys_iteration_and_identity") {
  auto home = get_active_policy();
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);

  auto a = laila->constant(LailaValue::from_string("paramA"));
  auto b = laila->constant(LailaValue::from_string("paramB"));
  auto opt = laila->constant(LailaValue::from_string("adam"));

  // Blueprint: { model_params: [gidA, gidB], optimizer: gidOpt }.
  Json bp = Json::object();
  Json params = Json::array();
  params.push_back(Json(a->global_id()));
  params.push_back(Json(b->global_id()));
  bp["model_params"] = params;
  bp["optimizer"] = Json(opt->global_id());

  auto manifest = laila->manifest({.data = LailaValue::from_json(bp),
                                   .nickname = std::string("ckpt_manifest")});

  // keys() = top-level keys; iteration yields every leaf gid (DFS).
  CHECK_EQ(manifest->keys().size(), (size_t)2);
  CHECK_EQ(manifest->size(), (size_t)2);
  size_t leaf_count = 0;
  for (const auto& gid : *manifest) { (void)gid; ++leaf_count; }
  CHECK_EQ(leaf_count, (size_t)3);  // sum(1 for _ in manifest)
  CHECK_EQ((size_t)std::distance(manifest->begin(), manifest->end()), (size_t)3);

  // nickname -> deterministic gid in the MANIFEST scope.
  CHECK(is_laila_resource(manifest->global_id()));
  auto m2 = laila->manifest({.nickname = std::string("ckpt_manifest")});
  CHECK_EQ(m2->global_id(), manifest->global_id());

  activate_policy(home);
}

TEST("manifest", "remember_and_realized") {
  auto home = get_active_policy();
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);

  auto a = laila->constant(LailaValue::from_string("paramA"));
  auto b = laila->constant(LailaValue::from_string("paramB"));
  auto opt = laila->constant(LailaValue::from_string("adam"));
  laila->memorize(a)->wait();
  laila->memorize(b)->wait();
  laila->memorize(opt)->wait();

  Json bp = Json::object();
  Json params = Json::array();
  params.push_back(Json(a->global_id()));
  params.push_back(Json(b->global_id()));
  bp["model_params"] = params;
  bp["optimizer"] = Json(opt->global_id());
  auto manifest = laila->manifest({.data = LailaValue::from_json(bp)});

  // remember() returns a future over the referenced entries.
  {
    auto g = laila->guarantee();
    manifest->remember({});
  }

  // realized(): nested structure mirroring the blueprint with resolved entries.
  auto realized = manifest->realized();
  CHECK(realized.is_object());
  CHECK(realized["model_params"].is_list());
  CHECK_EQ(realized["model_params"].size(), (size_t)2);
  CHECK(realized["optimizer"].is_entry());
  CHECK_EQ(realized["optimizer"].entry()->data().as_string(), std::string("adam"));
  CHECK_EQ(realized["model_params"][(size_t)0].entry()->data().as_string(), std::string("paramA"));
  CHECK_EQ(realized["model_params"][(size_t)1].entry()->data().as_string(), std::string("paramB"));

  activate_policy(home);
}
