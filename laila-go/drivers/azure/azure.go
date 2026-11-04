// Package azure is the ``azure`` extra (``pip install laila-core[azure]``):
// it binds laila.AzurePool to the Azure Blob Storage SDK for Go. Import
// it for its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/azure"
package azure

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// Driver implements laila.Azure_driver.
type Driver struct{}

// From_connection_string is ``BlobServiceClient.from_connection_string``.
func (Driver) From_connection_string(connection_string string) (laila.Azure_blob_service_client, error) {
	c, err := azblob.NewClientFromConnectionString(connection_string, nil)
	if err != nil {
		return nil, py.ValueError.New(err.Error()).From(err)
	}
	return &serviceClient{c: c}, nil
}

type serviceClient struct{ c *azblob.Client }

func (s *serviceClient) Get_container_client(container_name string) (laila.Azure_container_client, error) {
	return &containerClient{c: s.c.ServiceClient().NewContainerClient(container_name)}, nil
}

func (s *serviceClient) Close() error { return nil }

type containerClient struct{ c *container.Client }

// wrap maps the SDK's error codes onto ``azure.core.exceptions``.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		switch {
		case bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound, bloberror.ResourceNotFound) || respErr.StatusCode == 404:
			return laila.ResourceNotFoundError.New(err.Error()).From(err)
		case bloberror.HasCode(err, bloberror.ContainerAlreadyExists, bloberror.BlobAlreadyExists, bloberror.ResourceAlreadyExists) || respErr.StatusCode == 409:
			return laila.ResourceExistsError.New(err.Error()).From(err)
		}
	}
	return py.FromGo(err)
}

func (c *containerClient) Create_container() error {
	_, err := c.c.Create(context.Background(), nil)
	return wrap(err)
}

func (c *containerClient) Download_blob(name string) ([]byte, error) {
	resp, err := c.c.NewBlobClient(name).DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, wrap(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, py.FromGo(err)
	}
	return data, nil
}

func (c *containerClient) Upload_blob(name string, data []byte, overwrite bool, content_type string) error {
	opts := &azblob.UploadStreamOptions{HTTPHeaders: &blob.HTTPHeaders{BlobContentType: &content_type}}
	if !overwrite {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: toETag("*")},
		}
	}
	_, err := c.c.NewBlockBlobClient(name).UploadStream(context.Background(), bytes.NewReader(data), opts)
	return wrap(err)
}

func toETag(s string) *azcore.ETag {
	e := azcore.ETag(s)
	return &e
}

func (c *containerClient) Delete_blob(name string) error {
	_, err := c.c.NewBlobClient(name).Delete(context.Background(), nil)
	return wrap(err)
}

func (c *containerClient) Exists(name string) (bool, error) {
	_, err := c.c.NewBlobClient(name).GetProperties(context.Background(), nil)
	if err == nil {
		return true, nil
	}
	if werr := wrap(err); errors.Is(werr, laila.ResourceNotFoundError) {
		return false, nil
	} else {
		return false, werr
	}
}

func (c *containerClient) List_blobs() ([]string, error) {
	names := []string{}
	pager := c.c.NewListBlobsFlatPager(nil)
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, wrap(err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				names = append(names, *item.Name)
			}
		}
	}
	return names, nil
}

func init() { laila.Register_driver("azure.storage.blob", Driver{}) }
