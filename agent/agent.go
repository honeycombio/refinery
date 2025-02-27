package agent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
)

const (
	serviceName                  = "refinery"
	sendAgentTelemetryCapability = "io.honeycomb.capabilities.sendAgentTelemetry"
)

type Agent struct {
	clock              clockwork.Clock
	agentType          string
	agentVersion       string
	instanceId         uuid.UUID
	hostname           string
	effectiveConfig    config.Config
	agentDescription   *protobufs.AgentDescription
	opampClient        client.OpAMPClient
	remoteConfigStatus *protobufs.RemoteConfigStatus
	remoteConfig       *protobufs.AgentRemoteConfig

	//	opampClientCert     *tls.Certificate
	//	caCertPath          string
	//	certRequested       bool
	//	clientPrivateKeyPEM []byte
	lastHealth *protobufs.ComponentHealth

	logger       Logger
	ctx          context.Context
	cancel       context.CancelFunc
	metrics      metrics.Metrics
	usageTracker *usageTracker
	health       health.Reporter
}

func NewAgent(refineryLogger Logger, clock clockwork.Clock, agentVersion string, currentConfig config.Config, metrics metrics.Metrics, health health.Reporter) *Agent {
	ctx, cancel := context.WithCancel(context.Background())
	agent := &Agent{
		ctx:             ctx,
		cancel:          cancel,
		clock:           clock,
		logger:          refineryLogger,
		agentType:       serviceName,
		agentVersion:    agentVersion,
		effectiveConfig: currentConfig,
		metrics:         metrics,
		health:          health,
		usageTracker:    newUsageTracker(),
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
	agent.hostname = hostname
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
					Value: &protobufs.AnyValue_StringValue{StringValue: agent.hostname},
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
			// TODO: when will this get called??
			SaveRemoteConfigStatus: func(ctx context.Context, status *protobufs.RemoteConfigStatus) {
				agent.logger.Debugf(ctx, "got remote config status: %v", status)
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
	err = agent.opampClient.SetHealth(healthMessage(false))
	if err != nil {
		return err
	}

	agent.opampClient.SetCustomCapabilities(&protobufs.CustomCapabilities{
		Capabilities: []string{sendAgentTelemetryCapability},
	})

	agent.logger.Debugf(context.Background(), "starting opamp client")

	err = agent.opampClient.Start(context.Background(), settings)
	if err != nil {
		return err
	}
	agent.logger.Debugf(context.Background(), "started opamp client")

	go agent.healthCheck()
	go agent.usageReport()
	return nil
}

func (agent *Agent) Stop(ctx context.Context) {
	agent.logger.Debugf(ctx, "disconnecting from OpAMP server")
	err := agent.opampClient.SetHealth(
		&protobufs.ComponentHealth{
			Healthy: false, LastError: "Refinery is shutdown",
		},
	)
	if err != nil {
		agent.logger.Errorf(ctx, "Could not report health to OpAMP server: %v", err)
	}
	err = agent.opampClient.Stop(ctx)
	if err != nil {
		agent.logger.Errorf(ctx, "Failed to stop OpAMP client: %v", err)
	}

	agent.cancel()
}

func (agent *Agent) healthCheck() {
	//TODO: make this ticker configurable
	timer := agent.clock.NewTicker(5 * time.Second)
	for {
		select {
		case <-agent.ctx.Done():
		case <-timer.Chan():
			report := agent.calculateHealth()
			if report != nil {
				agent.lastHealth = report
				if err := agent.opampClient.SetHealth(report); err != nil {
					agent.logger.Errorf(context.Background(), "Could not report health to OpAMP server: %v", err)
				}
			}

			traceUsage, ok := agent.metrics.Get("bytes_received_trace")
			if !ok {
				agent.logger.Errorf(context.Background(), "unexpected missing trace usage metric")
			}
			logUsage, ok := agent.metrics.Get("bytes_received_log")
			if !ok {
				agent.logger.Errorf(context.Background(), "unexpected missing log usage metric")
			}

			now := agent.clock.Now()
			agent.usageTracker.Add(newTraceCumulativeUsage(traceUsage, now))
			agent.usageTracker.Add(newLogCumulativeUsage(logUsage, now))
		}
	}
}

func (agent *Agent) usageReport() {
	timer := agent.clock.NewTicker(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-agent.ctx.Done():
			// TODO: drain the existing reports
			return
		case <-timer.Chan():
			usageReport, err := agent.usageTracker.NewReport(agent.agentType, agent.agentVersion, agent.hostname)
			if err != nil {
				if errors.Is(err, errNoData) {
					agent.logger.Debugf(context.Background(), "No data to report")
					continue
				}
				agent.logger.Errorf(context.Background(), "Could not generate usage report: %v", err)
				continue
			}

			if err := agent.sendUsageReport(usageReport); err != nil {
				agent.logger.Errorf(context.Background(), "MONEY STEALING. Could not send usage report: %v", err)
			}

			agent.usageTracker.completeSend()
		}
	}
}

func (agent *Agent) sendUsageReport(usageReport []byte) error {
	isSent, err := agent.opampClient.SendCustomMessage(&protobufs.CustomMessage{
		Capability: sendAgentTelemetryCapability,
		Data:       usageReport,
	})

	if err != nil {
		if errors.Is(err, types.ErrCustomMessagePending) {
			agent.logger.Debugf(context.Background(), "Usage report is pending")
			select {
			case <-agent.ctx.Done():
				// TODO: we probably need to drain the existing reports
				return agent.ctx.Err()
			case <-isSent:
				// Retry sending the message once
				isSent, err = agent.opampClient.SendCustomMessage(&protobufs.CustomMessage{
					Capability: sendAgentTelemetryCapability,
					Data:       usageReport,
				})
				if err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}

	select {
	case <-agent.ctx.Done():
		return agent.ctx.Err()
	case <-isSent:
		return nil
	}
}

func (agent *Agent) calculateHealth() *protobufs.ComponentHealth {
	lastHealth := agent.lastHealth
	report := healthMessage(agent.health.IsAlive())
	if report.GetHealthy() {
		report.Healthy = agent.health.IsReady()
	}

	// report health only if it has changed
	if lastHealth == nil || lastHealth.GetHealthy() != report.GetHealthy() {
		return report
	}

	return nil
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
	err := agent.opampClient.SetAgentDescription(agent.agentDescription)
	if err != nil {
		agent.logger.Errorf(context.Background(), "Could not report OpAMP remote config status: %s", err)
	}
	remoteConfigstatus := &protobufs.RemoteConfigStatus{
		LastRemoteConfigHash: agent.remoteConfig.GetConfigHash(),
		Status:               status,
		ErrorMessage:         errorMessage,
	}
	err = agent.opampClient.SetRemoteConfigStatus(remoteConfigstatus)
	if err != nil {
		agent.logger.Errorf(context.Background(), "Could not report OpAMP remote config status: %s", err)
		return
	}
	agent.remoteConfigStatus = remoteConfigstatus
}

func (agent *Agent) onMessage(ctx context.Context, msg *types.MessageData) {
	if msg.OwnMetricsConnSettings != nil {
		agent.logger.Debugf(ctx, "got own metrics connection settings")
	}
	if msg.AgentIdentification != nil {
		agent.logger.Debugf(ctx, "got agent identification")
		uid, err := uuid.FromBytes(msg.AgentIdentification.NewInstanceUid)
		if err != nil {
			agent.logger.Errorf(ctx, "Failed to parse new instance uid: %v", err)
			return
		}
		agent.updateAgentIdentity(ctx, uid)
	}

	agent.updateRemoteConfig(ctx, msg)

}

func (agent *Agent) onOpampConnectionSettings(ctx context.Context, settings *protobufs.OpAMPConnectionSettings) error {
	agent.logger.Debugf(ctx, "got connection settings")
	return nil
}

func (agent *Agent) updateRemoteConfig(ctx context.Context, msg *types.MessageData) {
	if msg.RemoteConfig == nil {
		agent.logger.Debugf(context.Background(), "updateRemoteConfig: no remote config in message")
		return
	}

	if msg.RemoteConfig.GetConfig().GetConfigMap() != nil {
		// deserialize the config and call ReloadConfig
		agent.logger.Debugf(ctx, "onMessage got remote config: %v", msg)

		confMap := msg.RemoteConfig.GetConfig().GetConfigMap()

		if !agent.isConfigChanged(msg.RemoteConfig.GetConfigHash()) {
			agent.logger.Debugf(ctx, "onMessage remote config is the same as the last one, skipping")
			return
		}

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
				agent.logger.Errorf(ctx, "Failed to reload config: %v", err)
				agent.reportConfigStatus(protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED, err.Error())
			} else {
				agent.logger.Logger.Info().Logf("Successfully reloaded config")
				agent.reportConfigStatus(protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED, "")
			}
		}
	}
}

func (agent *Agent) isConfigChanged(newConfigHash []byte) bool {
	if agent.remoteConfig == nil {
		return true
	}
	if !bytes.Equal(agent.remoteConfigStatus.GetLastRemoteConfigHash(), newConfigHash) {
		return true
	}
	if agent.remoteConfigStatus.GetStatus() == protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED {
		return false
	}

	return true

}

func (agent *Agent) updateAgentIdentity(ctx context.Context, instanceId uuid.UUID) {
	agent.logger.Debugf(ctx, "Agent identify is being changed from id=%v to id=%v",
		agent.instanceId,
		instanceId)
	agent.instanceId = instanceId

	// TODO: update metrics setting when identity changes
}

func healthMessage(healthy bool) *protobufs.ComponentHealth {
	return &protobufs.ComponentHealth{
		Healthy: healthy,
	}
}
