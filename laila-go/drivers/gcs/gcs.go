// Package gcs is the ``gcs`` extra (``pip install laila-core[gcs]``): it
// binds laila.GCSPool to cloud.google.com/go/storage. Import it for its
// side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/gcs"
package gcs

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Driver implements laila.Gcs_driver.
type Driver struct{}

// Client is ``storage.Client(credentials=service_account.Credentials.
// from_service_account_info(info), project=project_id)``; without
// explicit service-account info Application Default Credentials apply.
func (Driver) Client(service_account_info *py.Dict, project_id *string) (laila.Gcs_client, error) {
	opts := []option.ClientOption{}
	if service_account_info != nil {
		raw, err := py.Json_dumps(service_account_info)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentialsJSON([]byte(raw)))
	}
	c, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, py.FromGo(err)
	}
	return &client{c: c}, nil
}

type client struct{ c *storage.Client }

func wrap(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
		return laila.NotFound.New(err.Error()).From(err)
	}
	return py.FromGo(err)
}

func (c *client) Download_as_text(bucket string, name string) (string, error) {
	r, err := c.c.Bucket(bucket).Object(name).NewReader(context.Background())
	if err != nil {
		return "", wrap(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return "", wrap(err)
	}
	return string(data), nil
}

func (c *client) Upload_from_string(bucket string, name string, data string, content_type string) error {
	w := c.c.Bucket(bucket).Object(name).NewWriter(context.Background())
	w.ContentType = content_type
	if _, err := w.Write([]byte(data)); err != nil {
		_ = w.Close()
		return wrap(err)
	}
	return wrap(w.Close())
}

func (c *client) Delete(bucket string, name string) error {
	return wrap(c.c.Bucket(bucket).Object(name).Delete(context.Background()))
}

func (c *client) Exists(bucket string, name string) (bool, error) {
	_, err := c.c.Bucket(bucket).Object(name).Attrs(context.Background())
	if err == nil {
		return true, nil
	}
	if werr := wrap(err); errors.Is(werr, laila.NotFound) {
		return false, nil
	} else {
		return false, werr
	}
}

func (c *client) List_blobs(bucket string) ([]string, error) {
	names := []string{}
	it := c.c.Bucket(bucket).Objects(context.Background(), nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, wrap(err)
		}
		names = append(names, attrs.Name)
	}
	return names, nil
}

func (c *client) Close() error { return wrap(c.c.Close()) }

func init() { laila.Register_driver("google.cloud.storage", Driver{}) }
