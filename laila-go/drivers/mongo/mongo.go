// Package mongo is the ``mongo`` extra (``pip install laila-core[mongo]``):
// it binds laila.MongoPool to go.mongodb.org/mongo-driver. Import it for
// its side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/mongo"
package mongo

import (
	"context"
	"strconv"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Driver implements laila.Pymongo_driver.
type Driver struct{}

// MongoClient_uri is ``MongoClient(uri, serverSelectionTimeoutMS=...)``.
func (Driver) MongoClient_uri(uri string, server_selection_timeout_ms int) (laila.Pymongo_client, error) {
	opts := options.Client().ApplyURI(uri).
		SetServerSelectionTimeout(time.Duration(server_selection_timeout_ms) * time.Millisecond)
	c, err := mongo.Connect(opts)
	if err != nil {
		return nil, wrap(err)
	}
	return &client{c: c, timeout: time.Duration(server_selection_timeout_ms) * time.Millisecond}, nil
}

// MongoClient_host is ``MongoClient(host=, port=, serverSelectionTimeoutMS=...)``.
func (d Driver) MongoClient_host(host string, port int, server_selection_timeout_ms int) (laila.Pymongo_client, error) {
	return d.MongoClient_uri("mongodb://"+host+":"+strconv.Itoa(port)+"/", server_selection_timeout_ms)
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	if mongo.IsTimeout(err) {
		return laila.ServerSelectionTimeoutError.New(err.Error()).From(err)
	}
	return py.ConnectionError.New(err.Error()).From(err)
}

type client struct {
	c       *mongo.Client
	timeout time.Duration
}

func (c *client) ctx() (context.Context, context.CancelFunc) {
	// Server selection is bounded by the client option; operations get a
	// generous bound so a dead server cannot hang the pool forever.
	return context.WithTimeout(context.Background(), 30*time.Second+c.timeout)
}

func (c *client) Collection(dbname string, name string) laila.Pymongo_collection {
	return &collection{c: c, coll: c.c.Database(dbname).Collection(name)}
}

func (c *client) Admin_command_ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout+time.Second)
	defer cancel()
	return wrap(c.c.Ping(ctx, readpref.Primary()))
}

func (c *client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return wrap(c.c.Disconnect(ctx))
}

type collection struct {
	c    *client
	coll *mongo.Collection
}

// toBson converts a laila dict (``{"key": k}``, ``{"$set": {...}}``) into
// an ordered bson document.
func toBson(d *py.Dict) bson.D {
	out := bson.D{}
	if d == nil {
		return out
	}
	for _, it := range d.Items() {
		var v any = it.Value
		if sub, ok := it.Value.(*py.Dict); ok {
			v = toBson(sub)
		}
		out = append(out, bson.E{Key: py.Str(it.Key), Value: v})
	}
	return out
}

func fromBson(m bson.M) *py.Dict {
	d := py.NewDict()
	for k, v := range m {
		d.Set(k, v)
	}
	return d
}

func (c *collection) Create_index(key string, unique bool) error {
	ctx, cancel := c.c.ctx()
	defer cancel()
	_, err := c.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: key, Value: 1}},
		Options: options.Index().SetUnique(unique),
	})
	return wrap(err)
}

func (c *collection) Find_one(filter *py.Dict, projection *py.Dict) (*py.Dict, error) {
	ctx, cancel := c.c.ctx()
	defer cancel()
	var out bson.M
	err := c.coll.FindOne(ctx, toBson(filter), options.FindOne().SetProjection(toBson(projection))).Decode(&out)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, wrap(err)
	}
	return fromBson(out), nil
}

func (c *collection) Update_one(filter *py.Dict, update *py.Dict, upsert bool) error {
	ctx, cancel := c.c.ctx()
	defer cancel()
	_, err := c.coll.UpdateOne(ctx, toBson(filter), toBson(update), options.UpdateOne().SetUpsert(upsert))
	return wrap(err)
}

func (c *collection) Delete_one(filter *py.Dict) error {
	ctx, cancel := c.c.ctx()
	defer cancel()
	_, err := c.coll.DeleteOne(ctx, toBson(filter))
	return wrap(err)
}

func (c *collection) Delete_many(filter *py.Dict) error {
	ctx, cancel := c.c.ctx()
	defer cancel()
	_, err := c.coll.DeleteMany(ctx, toBson(filter))
	return wrap(err)
}

func (c *collection) Count_documents(filter *py.Dict, limit int) (int, error) {
	ctx, cancel := c.c.ctx()
	defer cancel()
	opts := options.Count()
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	n, err := c.coll.CountDocuments(ctx, toBson(filter), opts)
	return int(n), wrap(err)
}

func (c *collection) Find_sorted(filter *py.Dict, projection *py.Dict, sort_key string, direction int) ([]*py.Dict, error) {
	ctx, cancel := c.c.ctx()
	defer cancel()
	cur, err := c.coll.Find(ctx, toBson(filter),
		options.Find().SetProjection(toBson(projection)).SetSort(bson.D{{Key: sort_key, Value: direction}}))
	if err != nil {
		return nil, wrap(err)
	}
	defer cur.Close(ctx)
	out := []*py.Dict{}
	for cur.Next(ctx) {
		var m bson.M
		if err := cur.Decode(&m); err != nil {
			return nil, wrap(err)
		}
		out = append(out, fromBson(m))
	}
	return out, wrap(cur.Err())
}

func init() { laila.Register_driver("pymongo", Driver{}) }
