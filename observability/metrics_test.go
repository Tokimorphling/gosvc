package observability

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/jsonrpc"
)

func TestJSONRPCUnknownMethodsShareOneLabel(t *testing.T) {
	m := New("test")
	m.ObserveJSONRPC("client.chosen.1", jsonrpc.CodeMethodNotFound, time.Millisecond)
	m.ObserveJSONRPC("client.chosen.2", jsonrpc.CodeMethodNotFound, time.Millisecond)
	m.ObserveJSONRPC("client.chosen.invalid", jsonrpc.CodeInvalidRequest, time.Millisecond)
	m.ObserveJSONRPC("echo", 0, time.Millisecond)

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "gosvc_jsonrpc_requests_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			var method, code string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "method":
					method = label.GetValue()
				case "code":
					code = label.GetValue()
				}
			}
			got[method+":"+code] = metric.GetCounter().GetValue()
		}
	}
	want := map[string]float64{
		jsonrpc.UnknownMethodLabel + ":" + strconv.Itoa(jsonrpc.CodeMethodNotFound): 2,
		jsonrpc.UnknownMethodLabel + ":" + strconv.Itoa(jsonrpc.CodeInvalidRequest): 1,
		"echo:0": 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request metric series = %v, want %v", got, want)
	}
}
