package agent

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"runtime"

	"github.com/google/uuid"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
)

const serviceName = "refinery"

type Agent struct {
	agentType          string
	agentVersion       string
	instanceId         uuid.UUID
	effectiveConfig    config.Config
	agentDescription   *protobufs.AgentDescription
	opampClient        client.OpAMPClient
	remoteConfigStatus *protobufs.RemoteConfigStatus
	opampClientCert    *tls.Certificate
	caCertPath         string
	remoteConfig       *protobufs.AgentRemoteConfig

	certRequested       bool
	clientPrivateKeyPEM []byte

	logger  Logger
	metrics metrics.Metrics
}

func NewAgent(refineryLogger Logger, agentVersion string, currentConfig config.Config) *Agent {
	agent := &Agent{
		logger:          refineryLogger,
		agentType:       serviceName,
		agentVersion:    agentVersion,
		effectiveConfig: currentConfig,
	}
	agent.createAgentIdentity()
	agent.logger.Debugf(context.Background(), "starting opamp client, id=%v", agent.instanceId)
	if err := agent.connect(); err != nil {
		agent.logger.Errorf(context.Background(), "Failed to connect to OpAMP Server: %v", err)
		return nil
	}
	return agent
}

func (agent *Agent) createAgentIdentity() {
	uid, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	agent.instanceId = uid
	hostname, _ := os.Hostname()
	agent.agentDescription = &protobufs.AgentDescription{
		IdentifyingAttributes: []*protobufs.KeyValue{
			{
				Key: "service.name",
				Value: &protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{StringValue: agent.agentType},
				},
			},
			{
				Key: "service.version",
				Value: &protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{StringValue: agent.agentVersion},
				},
			},
		},
		NonIdentifyingAttributes: []*protobufs.KeyValue{
			{
				Key: "os.type",
				Value: &protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{StringValue: runtime.GOOS},
				},
			},
			{
				Key: "host.name",
				Value: &protobufs.AnyValue{
					Value: &protobufs.AnyValue_StringValue{StringValue: hostname},
				},
			},
		},
	}
}

func (agent *Agent) connect() error {
	agent.opampClient = client.NewWebSocket(&agent.logger)

	settings := types.StartSettings{
		Header:         http.Header{"agent": []string{"refinery"}},
		OpAMPServerURL: agent.effectiveConfig.GetOpAMPConfig().Endpoint,
		InstanceUid:    types.InstanceUid(agent.instanceId),
		Callbacks: types.Callbacks{
			OnConnect: func(ctx context.Context) {
				agent.logger.Debugf(ctx, "connected to OpAMP server")
			},
			OnConnectFailed: func(ctx context.Context, err error) {
				agent.logger.Errorf(ctx, "Failed to connect to server: %v", err)
			},
			OnError: func(ctx context.Context, err *protobufs.ServerErrorResponse) {
				agent.logger.Errorf(ctx, "Received error from server: %v", err)
			},
			SaveRemoteConfigStatus: func(ctx context.Context, status *protobufs.RemoteConfigStatus) {
				agent.remoteConfigStatus = status
			},
			GetEffectiveConfig: func(ctx context.Context) (*protobufs.EffectiveConfig, error) {
				return agent.composeEffectiveConfig(), nil
			},
			OnMessage:                 agent.onMessage,
			OnOpampConnectionSettings: agent.onOpampConnectionSettings,
		},
		RemoteConfigStatus: agent.remoteConfigStatus,
		Capabilities: protobufs.AgentCapabilities_AgentCapabilities_AcceptsRemoteConfig |
			protobufs.AgentCapabilities_AgentCapabilities_ReportsRemoteConfig |
			protobufs.AgentCapabilities_AgentCapabilities_ReportsEffectiveConfig |
			protobufs.AgentCapabilities_AgentCapabilities_ReportsOwnMetrics |
			protobufs.AgentCapabilities_AgentCapabilities_AcceptsOpAMPConnectionSettings |
			protobufs.AgentCapabilities_AgentCapabilities_ReportsHealth,
	}

	err := agent.opampClient.SetAgentDescription(agent.agentDescription)
	if err != nil {
		return err
	}
	err = agent.opampClient.SetHealth(agent.getHealth())
	if err != nil {
		return err
	}

	agent.logger.Debugf(context.Background(), "starting opamp client")

	err = agent.opampClient.Start(context.Background(), settings)
	if err != nil {
		return err
	}
	agent.logger.Debugf(context.Background(), "started opamp client")
	return nil
}

// TODO: call disconnect
func (agent *Agent) disconnect(ctx context.Context) {
	agent.logger.Debugf(ctx, "disconnecting from OpAMP server")
	agent.opampClient.Stop(ctx)
}

func (agent *Agent) composeEffectiveConfig() *protobufs.EffectiveConfig {
	configYAML, err := config.SerializeToYAML(agent.effectiveConfig)
	if err != nil {
		agent.logger.Errorf(context.Background(), "Failed to marshal effective config: %v", err)
		return nil
	}
	return &protobufs.EffectiveConfig{
		ConfigMap: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {Body: []byte(configYAML)},
			},
		},
	}
}

func (agent *Agent) reportConfigStatus(status protobufs.RemoteConfigStatuses, errorMessage string) {
	err := agent.opampClient.SetRemoteConfigStatus(&protobufs.RemoteConfigStatus{
		LastRemoteConfigHash: agent.remoteConfig.GetConfigHash(),
		Status:               status,
		ErrorMessage:         errorMessage,
	})
	if err != nil {
		agent.logger.Errorf(context.Background(), "Could not report OpAMP remote config status: %s", err)
	}
}

func (agent *Agent) onMessage(ctx context.Context, msg *types.MessageData) {
	if msg.RemoteConfig != nil {
		// deserialize the config and call ReloadConfig
		agent.logger.Debugf(ctx, "onMessage got remote config: %v", msg)
		if msg.RemoteConfig.GetConfig().GetConfigMap() != nil {
			confMap := msg.RemoteConfig.GetConfig().GetConfigMap()
			var opts []config.ReloadedConfigDataOption
			if c, ok := confMap["refinery_rules"]; ok {
				opts = append(opts, config.WithRulesData(config.NewConfigData(c.GetBody(), config.FormatYAML, "opamp://rules")))
			}
			if c, ok := confMap["refinery_config"]; ok {
				opts = append(opts, config.WithConfigData(config.NewConfigData(c.GetBody(), config.FormatYAML, "opamp://config")))
			}
			agent.remoteConfig = msg.RemoteConfig
			agent.logger.Debugf(ctx, "onMessage config opts: %v", opts)
			if len(opts) > 0 {
				agent.reportConfigStatus(protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLYING, "")
				err := agent.effectiveConfig.Reload(opts...)
				if err != nil {
					agent.reportConfigStatus(protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED, err.Error())
				} else {
					agent.reportConfigStatus(protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED, "")
				}
			}
		}
	}
	if msg.OwnMetricsConnSettings != nil {
		agent.logger.Debugf(ctx, "got own metrics connection settings")
	}
	if msg.AgentIdentification != nil {
		agent.logger.Debugf(ctx, "got agent identification")
	}

}

func (agent *Agent) getHealth() *protobufs.ComponentHealth {
	return &protobufs.ComponentHealth{
		Healthy: true,
	}
}

func (agent *Agent) onOpampConnectionSettings(ctx context.Context, settings *protobufs.OpAMPConnectionSettings) error {
	agent.logger.Debugf(ctx, "got connection settings")
	return nil
}
