package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// data/schema/data_container.py -- virtual base for every laila data
// container.
//
// A *data container* is any identifiable object that holds records and
// hands entries back to whoever reads from it. The base class fixes the
// shape of that contract without choosing a key type -- that is left to
// the subclasses, which is exactly where the two concrete families differ:
//
//   - LAILA_IDENTIFIABLE_POOL_ (``data/schema/base.py``) is a **map**: keys
//     are entry ``global_id`` strings and the container is the persistence
//     tier that ``memorize`` / ``remember`` / ``forget`` route to.
//   - MultiBuffer (``data/multibuffer/multibuffer.py``) is a **list**: keys
//     are integer slot indices and the container is a fixed-capacity ring
//     with independent read/write heads -- the proxy a microcontroller uses
//     to stand in front of a camera's frame buffer.
//
// Both agree on the *value* contract: what goes in is a Record (an entry
// plus provenance), and what comes out is the bare Entry.
//
// # Virtual in the laila sense
//
// The class is instantiable as a type (pydantic needs that to build the
// schema, and tests use it to check identity plumbing), but the three
// item-access methods return NotImplementedError until a subclass supplies
// them -- the same convention ``BotoPool._get_client`` and the pool storage
// hooks follow.

// LAILA_IDENTIFIABLE_DATA_CONTAINER_ is the virtual base class for
// record-holding containers.
//
// Carries the identity contract (``global_id`` / uuid / nickname /
// scopes), per-instance atomic locking, and the CLI parameter resolution
// tiers. Declares -- but does not implement -- the item protocol every
// container exposes:
//
//   - ``container[key]`` (Getitem) returns the entry stored at *key* (or
//     nil when the slot is empty).
//   - ``container[key] = value`` (Setitem) stores *value* at *key*;
//     subclasses decide how a bare entry becomes a record on the way in.
//   - ``container.empty()`` discards every stored record.
//
// The meaning of *key* is entirely up to the subclass.
type LAILA_IDENTIFIABLE_DATA_CONTAINER_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_
}

// LAILA_IDENTIFIABLE_DATA_CONTAINER_class is the class descriptor;
// ``_scopes`` defaults to ``[_DATA_CONTAINER_SCOPE]``
// (``arbitrary_types_allowed``).
var LAILA_IDENTIFIABLE_DATA_CONTAINER_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_DATA_CONTAINER", (*LAILA_IDENTIFIABLE_DATA_CONTAINER_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{DATA_CONTAINER_SCOPE_} })

// New_LAILA_IDENTIFIABLE_DATA_CONTAINER_ is
// ``_LAILA_IDENTIFIABLE_DATA_CONTAINER(**data)``.
func New_LAILA_IDENTIFIABLE_DATA_CONTAINER_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_DATA_CONTAINER_, error) {
	c := LAILA_IDENTIFIABLE_DATA_CONTAINER_class.New().(*LAILA_IDENTIFIABLE_DATA_CONTAINER_)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// Data_container_base_ exposes the embedded base to code holding a
// subclass.
func (c *LAILA_IDENTIFIABLE_DATA_CONTAINER_) Data_container_base_() *LAILA_IDENTIFIABLE_DATA_CONTAINER_ {
	return c
}

// Model_post_init chains the identity and CLI-capable hooks.
func (c *LAILA_IDENTIFIABLE_DATA_CONTAINER_) Model_post_init(ctx any) error {
	if err := c.LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	return CLI_CAPABLE_model_post_init(c.Self())
}

// Getitem is ``container[key]``: the entry stored at *key*. Subclasses
// define key semantics.
func (c *LAILA_IDENTIFIABLE_DATA_CONTAINER_) Getitem(key any) (any, error) {
	return nil, py.NotImplementedError.Newf(
		"%s does not implement __getitem__; _LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.",
		py.Pytypename(c.Self()))
}

// Setitem is ``container[key] = value``: store *value* at *key*.
// Subclasses define key and record semantics.
func (c *LAILA_IDENTIFIABLE_DATA_CONTAINER_) Setitem(key any, value any) error {
	return py.NotImplementedError.Newf(
		"%s does not implement __setitem__; _LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.",
		py.Pytypename(c.Self()))
}

// Empty discards every record held by this container.
func (c *LAILA_IDENTIFIABLE_DATA_CONTAINER_) Empty() error {
	return py.NotImplementedError.Newf(
		"%s does not implement empty; _LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base.",
		py.Pytypename(c.Self()))
}
