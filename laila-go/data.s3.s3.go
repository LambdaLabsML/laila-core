package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// data/s3/s3.py -- AWS S3 pool implementation.
//
// Thin specialisation of BotoPool that points at the AWS S3 endpoint
// with AWS-style v4 signatures. The bulk of the read/write/delete logic
// lives in the parent class -- this file just plugs in the right
// ``boto3.client`` factory and the matching ``aioboto3.Session`` for the
// async path.

// S3Pool is the AWS S3-backed pool.
//
// Uploads and downloads serialised entries against a single S3 bucket --
// one bucket per pool, objects keyed by entry ``global_id``.
//
//   - access_key_id: AWS access key id. Falls back to the standard boto3
//     credential provider chain (env vars, ``~/.aws/credentials``, IAM
//     role metadata, ...) when omitted.
//   - secret_access_key: AWS secret access key. See access_key_id for
//     the fallback rules.
//   - region_name: AWS region for the bucket. When omitted, boto3 picks
//     the region from its standard configuration sources.
type S3Pool struct {
	BotoPool

	Access_key_id     *string `py:"access_key_id"`
	Secret_access_key *string `py:"secret_access_key"`
	Region_name       *string `py:"region_name"`
}

// S3Pool_class is the class descriptor.
var S3Pool_class = py.NewClass("S3Pool", (*S3Pool)(nil), BotoPool_class)

// New_S3Pool is ``S3Pool(**data)``.
func New_S3Pool(kw ...py.Kw) (*S3Pool, error) {
	p := S3Pool_class.New().(*S3Pool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Get_client_ returns a cached ``boto3.client`` instance for AWS S3.
//
// Configured with SigV4 signing, a three-attempt standard retry policy,
// and a connection pool sized by ``max_pool_connections``. Subsequent
// calls return the cached client.
func (p *S3Pool) Get_client_() (Boto3_s3_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Client_ != nil {
		return p.Client_, nil
	}
	if boto3_() == nil {
		return nil, py.ImportError.New("boto3 is required for S3Pool")
	}
	kwargs := Boto3_client_kwargs{
		Config: Boto3_client_config{
			Signature_version:    "s3v4",
			Retries_max_attempts: 3,
			Retries_mode:         "standard",
			Max_pool_connections: p.Max_pool_connections,
		},
	}
	if p.Access_key_id != nil {
		kwargs.Access_key_id = p.Access_key_id
	}
	if p.Secret_access_key != nil {
		kwargs.Secret_access_key = p.Secret_access_key
	}
	if p.Region_name != nil {
		kwargs.Region_name = p.Region_name
	}
	client, err := boto3_().Client(kwargs)
	if err != nil {
		return nil, err
	}
	p.Client_ = client
	return client, nil
}

// Get_aio_session_ returns a cached ``aioboto3.Session`` carrying this
// pool's AWS credentials. Used by the async path; ImportError with an
// actionable hint if the optional dependency is missing.
func (p *S3Pool) Get_aio_session_() (Boto3_client_kwargs, error) {
	if aioboto3_() == nil {
		return Boto3_client_kwargs{}, py.ImportError.New(
			"aioboto3 is required for the async S3 path; install with `pip install aioboto3`")
	}
	if p.Aio_session_ == nil {
		kwargs := Boto3_client_kwargs{}
		if p.Access_key_id != nil {
			kwargs.Access_key_id = p.Access_key_id
		}
		if p.Secret_access_key != nil {
			kwargs.Secret_access_key = p.Secret_access_key
		}
		if p.Region_name != nil {
			kwargs.Region_name = p.Region_name
		}
		p.Aio_session_ = &kwargs
	}
	return *p.Aio_session_, nil
}
