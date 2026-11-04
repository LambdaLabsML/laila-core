// Package s3 is the ``s3`` / ``cloudflare`` / ``backblaze`` extra (``pip
// install laila-core[s3]``, all three share ``boto3``): it binds
// laila.BotoPool and its vendors (S3Pool, CloudflarePool, BackblazePool)
// to the AWS SDK for Go v2. Import it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/s3"
package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Driver implements laila.Boto3_driver (and stands in for ``aioboto3``).
type Driver struct{}

// Client is ``boto3.client("s3", **kwargs)``.
func (Driver) Client(kw laila.Boto3_client_kwargs) (laila.Boto3_s3_client, error) {
	ctx := context.Background()
	loadOpts := []func(*config.LoadOptions) error{}
	if kw.Access_key_id != nil && kw.Secret_access_key != nil {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(*kw.Access_key_id, *kw.Secret_access_key, "")))
	}
	if kw.Region_name != nil {
		loadOpts = append(loadOpts, config.WithRegion(*kw.Region_name))
	} else if kw.Endpoint_url != nil {
		// S3-compatible vendors sign with a fixed region ("auto" for R2;
		// B2 accepts any region matching its endpoint host).
		loadOpts = append(loadOpts, config.WithRegion("auto"))
	}
	attempts := kw.Config.Retries_max_attempts
	if attempts <= 0 {
		attempts = 3
	}
	loadOpts = append(loadOpts, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = attempts })
	}))
	maxConns := kw.Config.Max_pool_connections
	if maxConns <= 0 {
		maxConns = 10
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConns
	transport.MaxIdleConnsPerHost = maxConns
	transport.MaxConnsPerHost = maxConns
	loadOpts = append(loadOpts, config.WithHTTPClient(&http.Client{Transport: transport}))

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, py.FromGo(err)
	}
	s3Opts := []func(*awss3.Options){}
	if kw.Endpoint_url != nil {
		endpoint := *kw.Endpoint_url
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}
	return &client{s3: awss3.NewFromConfig(cfg, s3Opts...), transport: transport}, nil
}

type client struct {
	s3        *awss3.Client
	transport *http.Transport
}

// wrap converts an SDK error into ``botocore.exceptions.ClientError``
// carrying the service error code (``NoSuchKey``, ``404``, ...).
func wrap(err error) error {
	if err == nil {
		return nil
	}
	code := ""
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code = apiErr.ErrorCode()
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.Response != nil {
		status := strconv.Itoa(respErr.Response.StatusCode)
		if code == "" || code == "NotFound" {
			// botocore reports head_object misses as "404" and get_object
			// misses as "NoSuchKey"; keep the status code reachable for
			// vendors (Backblaze) whose get_object also answers "404".
			if code == "NotFound" {
				code = "404"
			} else if code == "" {
				code = status
			}
		}
	}
	return laila.ClientError.New(err.Error()).With("code", code).From(err)
}

func (c *client) Get_object(bucket string, key string) ([]byte, error) {
	resp, err := c.s3.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, wrap(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, py.FromGo(err)
	}
	return body, nil
}

func (c *client) Put_object(bucket string, key string, body []byte, content_type string) error {
	_, err := c.s3.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentType: aws.String(content_type),
		ContentLength: aws.Int64(int64(len(body))),
	})
	return wrap(err)
}

func (c *client) Delete_object(bucket string, key string) error {
	_, err := c.s3.DeleteObject(context.Background(), &awss3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	return wrap(err)
}

func (c *client) Head_object(bucket string, key string) error {
	_, err := c.s3.HeadObject(context.Background(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	return wrap(err)
}

func (c *client) List_objects_v2(bucket string) ([]string, error) {
	keys := []string{}
	paginator := awss3.NewListObjectsV2Paginator(c.s3, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, wrap(err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}
	return keys, nil
}

func (c *client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

func init() { laila.Register_driver("boto3", Driver{}) }
