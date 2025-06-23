package types

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestProtoPayloadMap_Parsing(t *testing.T) {
	request := &collectorTrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{
									StringValue: "test-service",
								},
							},
							Key: "service.name",
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
								SpanId:            []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
								Name:              "test-span",
								Kind:              tracev1.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: 1234567890000000000,
								EndTimeUnixNano:   1234567890000001000,
								Attributes: []*commonv1.KeyValue{
									{
										Key: "http.method",
										Value: &commonv1.AnyValue{
											Value: &commonv1.AnyValue_StringValue{
												StringValue: "GET",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(request)
	require.NoError(t, err, "failed to marshal ExportTraceServiceRequest")

	p := NewProtoPayloadMap(data)

	iter, err := p.Iterate()
	require.NoError(t, err, "failed to iterate over ProtoPayloadMap")

	for {
		key, typ, err := iter.NextField()
		if err != nil {
			require.Equal(t, io.EOF, err, "unexpected error while iterating fields")
			break
		}

		result, err := iter.ValueBytes()

		fmt.Println("Field Key:", key, "Type:", typ, "Value:", string(result))

	}
}
