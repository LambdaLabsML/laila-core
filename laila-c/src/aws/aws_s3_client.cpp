// aws-sdk-cpp-backed BotoClient (compiled only with -DLAILA_WITH_S3 on the
// posix backend). This is the C++ analog of boto3's S3 client that pool/s3/s3.py
// builds: SigV4 ("s3v4"), 3-attempt standard retries, and max_pool_connections.
// It implements the BotoClient seam that BotoPool drives, so memorize/remember/
// forget round-trip against real S3 on the host. MCU targets never compile this.
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>

#include "laila/pools.hpp"
#include "laila/status.hpp"

namespace laila_c {
namespace {

// Initialize the AWS SDK exactly once. We intentionally do not call ShutdownAPI:
// S3 clients are owned by long-lived S3Pool objects, and tying ShutdownAPI to
// static destruction would risk running it while clients are still alive. For a
// host process the OS reclaims at exit (this code never runs on MCU targets).
void ensure_aws_init() {
  static const bool inited = [] {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    return true;
  }();
  (void)inited;
}

class AwsS3Client : public BotoClient {
public:
  AwsS3Client(const std::string& access_key_id, const std::string& secret_access_key,
              const std::string& region_name, int max_pool_connections) {
    ensure_aws_init();
    Aws::S3::S3ClientConfiguration cfg;
    if (!region_name.empty()) cfg.region = region_name.c_str();
    cfg.maxConnections = max_pool_connections > 0 ? max_pool_connections : 128;
    cfg.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>("laila", 3);
    // Dev/test override (laila-c backend knob, not part of the verbatim S3Pool
    // API): point at a local S3-compatible server (moto/MinIO). Uses path-style
    // addressing and honors an http:// scheme so localhost mocks work.
    if (const char* endpoint = std::getenv("LAILA_S3_ENDPOINT_URL")) {
      if (*endpoint) {
        std::string ep(endpoint);
        cfg.endpointOverride = ep.c_str();
        cfg.useVirtualAddressing = false;
        if (ep.rfind("http://", 0) == 0) cfg.scheme = Aws::Http::Scheme::HTTP;
      }
    }
    if (!access_key_id.empty() || !secret_access_key.empty()) {
      Aws::Auth::AWSCredentials creds(access_key_id.c_str(), secret_access_key.c_str());
      client_ = std::make_unique<Aws::S3::S3Client>(creds, nullptr, cfg);
    } else {
      // No explicit creds -> default AWS credential provider chain (env, config,
      // instance role), mirroring boto3's behavior when creds are omitted.
      client_ = std::make_unique<Aws::S3::S3Client>(cfg);
    }
  }

  std::optional<std::string> get_object(const std::string& bucket, const std::string& key) override {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket.c_str());
    req.SetKey(key.c_str());
    auto out = client_->GetObject(req);
    if (!out.IsSuccess()) {
      const auto& err = out.GetError();
      if (err.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) return std::nullopt;
      raise(Status::Error, std::string("S3 GetObject failed: ") + err.GetMessage().c_str());
      return std::nullopt;
    }
    auto result = out.GetResultWithOwnership();
    std::ostringstream oss;
    oss << result.GetBody().rdbuf();
    return oss.str();
  }

  void put_object(const std::string& bucket, const std::string& key, const std::string& body,
                  const std::string& content_type) override {
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket.c_str());
    req.SetKey(key.c_str());
    req.SetContentType(content_type.c_str());
    auto stream = Aws::MakeShared<Aws::StringStream>("laila");
    stream->write(body.data(), static_cast<std::streamsize>(body.size()));
    req.SetBody(stream);
    auto out = client_->PutObject(req);
    if (!out.IsSuccess())
      raise(Status::Error, std::string("S3 PutObject failed: ") + out.GetError().GetMessage().c_str());
  }

  void delete_object(const std::string& bucket, const std::string& key) override {
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(bucket.c_str());
    req.SetKey(key.c_str());
    auto out = client_->DeleteObject(req);
    if (!out.IsSuccess())
      raise(Status::Error, std::string("S3 DeleteObject failed: ") + out.GetError().GetMessage().c_str());
  }

  bool head_object(const std::string& bucket, const std::string& key) override {
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket.c_str());
    req.SetKey(key.c_str());
    // Mirror boto.py _exists: any failure (incl. 404) -> False.
    return client_->HeadObject(req).IsSuccess();
  }

  std::vector<std::string> list_objects_v2(const std::string& bucket) override {
    std::vector<std::string> keys;
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(bucket.c_str());
    Aws::String token;
    do {
      if (!token.empty()) req.SetContinuationToken(token);
      auto out = client_->ListObjectsV2(req);
      if (!out.IsSuccess())
        raise(Status::Error,
              std::string("S3 ListObjectsV2 failed: ") + out.GetError().GetMessage().c_str());
      const auto& result = out.GetResult();
      for (const auto& obj : result.GetContents()) keys.emplace_back(obj.GetKey().c_str());
      token = result.GetIsTruncated() ? result.GetNextContinuationToken() : Aws::String();
    } while (!token.empty());
    return keys;
  }

private:
  std::unique_ptr<Aws::S3::S3Client> client_;
};

}  // namespace

std::unique_ptr<BotoClient> make_aws_s3_client(const std::string& access_key_id,
                                               const std::string& secret_access_key,
                                               const std::string& region_name,
                                               int max_pool_connections) {
  return std::make_unique<AwsS3Client>(access_key_id, secret_access_key, region_name,
                                       max_pool_connections);
}

}  // namespace laila_c
