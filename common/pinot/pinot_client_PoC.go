package pinot

import (
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
)

var _ GenericClient = (*pinotClient)(nil)

type (
	// pinot client implements Client
	pinotClient struct {
		client     *pinot.Connection
		logger     log.Logger
		serializer p.PayloadSerializer
	}
)

func (p pinotClient) Search(ctx interface{}, request *SearchRequest) (*SearchResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) SearchByQuery(ctx interface{}, request *SearchByQueryRequest) (*SearchResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) SearchRaw(ctx interface{}, index, query string) (*RawResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) ScanByQuery(ctx interface{}, request *ScanByQueryRequest) (*SearchResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) SearchForOneClosedExecution(ctx interface{}, index string, request *SearchForOneClosedExecutionRequest) (*SearchForOneClosedExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) CountByQuery(ctx interface{}, index, query string) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) RunBulkProcessor(ctx interface{}, p *BulkProcessorParameters) (GenericBulkProcessor, error) {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) PutMapping(ctx interface{}, index, root, key, valueType string) error {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) CreateIndex(ctx interface{}, index string) error {
	//TODO implement me
	panic("implement me")
}

func (p pinotClient) IsNotFoundError(err error) bool {
	//TODO implement me
	panic("implement me")
}
