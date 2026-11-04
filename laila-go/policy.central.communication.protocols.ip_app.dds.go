package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/dds.py -- DDS / RTPS
// communication transport (broker-mediated / pub-sub).
//
// Carries JSON-RPC over DDS topics via ``cyclonedds``. Each policy reads
// its inbox topic and writes to peer inbox topics. Built on the broker
// carrier; the driver is imported on connect and a missing library raises
// a clear capability error.
//
//   - ``protocol_name`` ``"dds"`` (aliases ``rtps``)
//   - URI scheme ``dds://<peer_policy_id>``

// Dds_driver_ is the ``cyclonedds`` contract
// (``cyclonedds.domain.DomainParticipant()``).
type Dds_driver_ interface {
	DomainParticipant() any
}

// LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_ is the DDS/RTPS pub-sub transport.
type LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_ struct {
	BrokerRPCProtocol_

	Participant_ any `py:"_participant"`
	Cyclonedds_  any `py:"_cyclonedds"`
}

// LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "dds"``).
var LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_)(nil),
	BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "dds").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("dds", "rtps")).
	Set_classvar("matches_token", Token_aliases_classmethod_("dds", "rtps")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("dds://"))

// New_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"dds"`` / ``"rtps"``.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``dds://`` URIs.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Broker_connect_ requires the driver and creates the domain participant.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Broker_connect_() error {
	mods, err := p.Require_drivers_([]string{"cyclonedds"}, "dds")
	if err != nil {
		return err
	}
	drv, _ := mods["cyclonedds"].(Dds_driver_)
	if drv != nil {
		p.Participant_ = drv.DomainParticipant()
	}
	p.Cyclonedds_ = mods["cyclonedds"]
	return nil
}

// Broker_subscribe_ is a no-op: a real implementation creates a
// DataReader on *topic* whose listener forwards samples to Feed_message_.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Broker_subscribe_(topic string) error {
	return nil
}

// Broker_publish_ raises: a real implementation writes *data* on a
// DataWriter for *topic*.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Broker_publish_(topic string, data []byte) error {
	return py.RuntimeError.New("DDS publish requires a configured DataWriter for the peer topic.")
}

// Broker_close_ drops the participant.
func (p *LAILA_IDENTIFIABLE_DDS_COMM_PROTOCOL_) Broker_close_() error {
	p.Participant_ = nil
	return nil
}
