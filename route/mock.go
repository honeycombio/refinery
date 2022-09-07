package route

import "google.golang.org/grpc/health/grpc_health_v1"

type MockGRPCHealthWatchServer struct {
	grpc_health_v1.Health_WatchServer
	sentMessages []*grpc_health_v1.HealthCheckResponse
}

func (m *MockGRPCHealthWatchServer) Send(msg *grpc_health_v1.HealthCheckResponse) error {
	m.sentMessages = append(m.sentMessages, msg)
	return nil
}

func (m *MockGRPCHealthWatchServer) GetSentMessages() []*grpc_health_v1.HealthCheckResponse {
	return m.sentMessages
}
