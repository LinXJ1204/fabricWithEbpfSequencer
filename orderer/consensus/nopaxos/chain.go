package nopaxos

import (
	"fmt"
	"net"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	nopaxosConfig "github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/protocol"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var logger = flogging.MustGetLogger("orderer.consensus.nopaxos")

type consenter struct {
	config *localconfig.TopLevel
}

type chain struct {
	support       consensus.ConsenterSupport
	sendChan      chan *message
	deliverChan   chan []*message
	exitChan      chan struct{}
	consenters    []*etcdraft.Consenter
	NopaxosServer *Server
	Count         uint64
	batch         []*cb.Envelope
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New(config *localconfig.TopLevel) consensus.Consenter {
	return &consenter{
		config: config,
	}
}

func (nps *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	m := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	return newChain(support, m.Consenters, nps.config), nil
}

func (nps *consenter) IsChannelMember(joinBlock *cb.Block) (bool, error) {
	return true, nil
}

func newChain(support consensus.ConsenterSupport, consenters []*etcdraft.Consenter, config *localconfig.TopLevel) *chain {

	nopaxosServerConfig := &nopaxosConfig.ProtocolConfig{}

	host, _, err := net.SplitHostPort(config.Operations.ListenAddress)
	if err != nil {
		fmt.Println("Error:", err)
	}

	members := make(map[string]protocol.Member)
	for _, consenter := range consenters {
		members[consenter.GetHost()] = protocol.Member{
			ID:           consenter.GetHost(),
			Host:         consenter.GetHost(),
			APIPort:      int(consenter.GetPort()) + 35,
			ProtocolPort: int(consenter.GetPort()) + 36,
		}
	}

	cluster := protocol.NodeCluster{
		MemberID: host,
		Members:  members,
	}

	deliverChan := make(chan []struct{})

	return &chain{
		support:     support,
		sendChan:    make(chan *message),
		exitChan:    make(chan struct{}),
		deliverChan: make(chan []*message),
		consenters:  consenters,
		NopaxosServer: NewNodeServer(
			cluster,
			nopaxosServerConfig,
			deliverChan,
		),
		Count: 1,
		batch: make([]*cb.Envelope, 0),
	}
}

func (ch *chain) Start() {
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
	fmt.Println("=======TESTTEST=======")
	fmt.Println(sequencerNumber)

	msg, err := proto.Marshal(env)
	if err != nil {
		return err
	}

	ch.NopaxosServer.nopaxos.Command(
		&protocol.NewCommandRequest{
			CommandRequest: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: protocol.MessageID(sequencerNumber),
				Timestamp:  time.Now(),
				Value:      msg,
			},
			ConfigSeq: configSeq,
		},
		nil,
	)

	ch.Count++

	if !ch.NopaxosServer.nopaxos.IsLeader() {
		return nil
	}

	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		normalMsg: env,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	go ch.NopaxosServer.Start()
	defer ch.NopaxosServer.Stop()

	for {
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			if msg.configMsg == nil {
				// NormalMsg
				if msg.configSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					if err != nil {
						logger.Warningf("Discarding bad normal message: %s", err)
						continue
					}
				}

				ch.batch = append(ch.batch, msg.normalMsg)

				if len(ch.batch) > 512 {
					block := ch.support.CreateNextBlock(ch.batch)
					ch.support.WriteBlock(block, nil)
					ch.batch = []*cb.Envelope{}
					if timer != nil {
						timer = nil
					}
				}

				pending := len(ch.batch) > 0

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					timer = time.After(1 * time.Second)
					logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
				}
			} else {
				// ConfigMsg
				if msg.configSeq < seq {
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			if len(ch.batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(ch.batch)
			ch.support.WriteBlock(block, nil)
			ch.batch = []*cb.Envelope{}

		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
