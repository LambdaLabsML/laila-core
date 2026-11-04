package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// data/cloudflare/cloudflare.py -- Cloudflare R2 pool implementation.

// CloudflarePool is the Cloudflare R2-backed pool. Uploads and downloads
// key-value data from R2.
//
// Uses the boto3 S3 API with the R2 endpoint. Objects are keyed by entry
// global_id directly (no pool directory). Use one bucket per pool.
type CloudflarePool struct {
	BotoPool

	Account_id        string `py:"account_id,required"`
	Access_key_id     string `py:"access_key_id,required"`
	Secret_access_key string `py:"secret_access_key,required"`
}

// CloudflarePool_class is the class descriptor.
var CloudflarePool_class = py.NewClass("CloudflarePool", (*CloudflarePool)(nil), BotoPool_class)

// New_CloudflarePool is ``CloudflarePool(**data)``.
func New_CloudflarePool(kw ...py.Kw) (*CloudflarePool, error) {
	p := CloudflarePool_class.New().(*CloudflarePool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Endpoint_url is ``https://<account_id>.r2.cloudflarestorage.com``.
func (p *CloudflarePool) Endpoint_url() string {
	return "https://" + p.Account_id + ".r2.cloudflarestorage.com"
}

// Get_client_ returns the cached boto3 S3 client pointed at R2.
func (p *CloudflarePool) Get_client_() (Boto3_s3_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Client_ != nil {
		return p.Client_, nil
	}
	if boto3_() == nil {
		return nil, py.ImportError.New("boto3 is required for CloudflarePool")
	}
	endpoint := p.Endpoint_url()
	client, err := boto3_().Client(Boto3_client_kwargs{
		Endpoint_url:      &endpoint,
		Access_key_id:     &p.Access_key_id,
		Secret_access_key: &p.Secret_access_key,
		Config: Boto3_client_config{
			Signature_version:    "s3v4",
			Retries_max_attempts: 3,
			Retries_mode:         "standard",
		},
	})
	if err != nil {
		return nil, err
	}
	p.Client_ = client
	return client, nil
}

// Get_aio_session_ returns the cached ``aioboto3.Session`` carrying the
// R2 credentials.
func (p *CloudflarePool) Get_aio_session_() (Boto3_client_kwargs, error) {
	if aioboto3_() == nil {
		return Boto3_client_kwargs{}, py.ImportError.New(
			"aioboto3 is required for the async R2 path; install with `pip install aioboto3`")
	}
	if p.Aio_session_ == nil {
		p.Aio_session_ = &Boto3_client_kwargs{
			Access_key_id:     &p.Access_key_id,
			Secret_access_key: &p.Secret_access_key,
		}
	}
	return *p.Aio_session_, nil
}

// Aio_client_kwargs_ points the aioboto3 client at the R2 endpoint.
func (p *CloudflarePool) Aio_client_kwargs_() Boto3_client_kwargs {
	endpoint := p.Endpoint_url()
	return Boto3_client_kwargs{Endpoint_url: &endpoint}
}

func init() { py.Register_property("endpoint_url") }
