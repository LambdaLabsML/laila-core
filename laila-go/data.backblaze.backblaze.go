package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// data/backblaze/backblaze.py -- Backblaze B2 pool implementation using
// the S3-compatible API.

// BackblazePool is the Backblaze B2-backed pool using the S3-compatible
// API.
type BackblazePool struct {
	BotoPool

	Application_key_id string `py:"application_key_id,required"`
	Application_key    string `py:"application_key,required"`
	Endpoint_url       string `py:"endpoint_url" default:"https://s3.us-west-004.backblazeb2.com"`
}

// BackblazePool_class is the class descriptor.
var BackblazePool_class = py.NewClass("BackblazePool", (*BackblazePool)(nil), BotoPool_class)

// New_BackblazePool is ``BackblazePool(**data)``.
func New_BackblazePool(kw ...py.Kw) (*BackblazePool, error) {
	p := BackblazePool_class.New().(*BackblazePool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// No_such_key_codes_ is ``_no_such_key_codes = {"NoSuchKey", "404"}``.
func (p *BackblazePool) No_such_key_codes_() []string { return []string{"NoSuchKey", "404"} }

// Get_client_ returns a boto3 S3 client configured for Backblaze B2.
func (p *BackblazePool) Get_client_() (Boto3_s3_client, error) {
	p.Client_mu_.Lock()
	defer p.Client_mu_.Unlock()
	if p.Client_ != nil {
		return p.Client_, nil
	}
	if boto3_() == nil {
		return nil, py.ImportError.New("boto3 is required for BackblazePool")
	}
	client, err := boto3_().Client(Boto3_client_kwargs{
		Endpoint_url:      &p.Endpoint_url,
		Access_key_id:     &p.Application_key_id,
		Secret_access_key: &p.Application_key,
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

// Get_aio_session_ returns a cached ``aioboto3.Session`` carrying the B2
// application key.
func (p *BackblazePool) Get_aio_session_() (Boto3_client_kwargs, error) {
	if aioboto3_() == nil {
		return Boto3_client_kwargs{}, py.ImportError.New(
			"aioboto3 is required for the async B2 path; install with `pip install aioboto3`")
	}
	if p.Aio_session_ == nil {
		p.Aio_session_ = &Boto3_client_kwargs{
			Access_key_id:     &p.Application_key_id,
			Secret_access_key: &p.Application_key,
		}
	}
	return *p.Aio_session_, nil
}

// Aio_client_kwargs_ points the aioboto3 client at the B2 endpoint
// instead of AWS.
func (p *BackblazePool) Aio_client_kwargs_() Boto3_client_kwargs {
	return Boto3_client_kwargs{Endpoint_url: &p.Endpoint_url}
}
