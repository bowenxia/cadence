// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package pinotVisibility

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/startreedata/pinot-client-go/pinot"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/mapper/thrift"
	"time"
)

const (
	pinotPersistenceName = "pinot"
	tableName            = "cadence-visibility-pinot"
)

type (
	pinotVisibilityStore struct {
		pinotClient *pinot.Connection
		index       string
		producer    messaging.Producer
		logger      log.Logger
		config      *service.Config
	}

	visibilityMessage struct {
		DomainID              string                     `json:"domainID,omitempty"`
		Wid                   string                     `json:"wid,omitempty"`
		Rid                   string                     `json:"rid,omitempty"`
		WorkflowTypeName      string                     `json:"workflowTypeName,omitempty"`
		TaskList              string                     `json:"taskList,omitempty"`
		StartTimeUnixNano     int64                      `json:"startTimeUnixNano,omitempty"`
		ExecutionTimeUnixNano int64                      `json:"executionTimeUnixNano,omitempty"`
		TaskID                int64                      `json:"taskID,omitempty"`
		Memo                  []byte                     `json:"memo,omitempty"`
		Encoding              common.EncodingType        `json:"encoding,omitempty"`
		IsCron                bool                       `json:"isCron,omitempty"`
		NumClusters           int16                      `json:"numClusters,omitempty"`
		SearchAttributes      map[string][]byte          `json:"searchAttributes,omitempty"`
		VisibilityOperation   common.VisibilityOperation `json:"visibilityOperation,omitempty"`
		// specific to certain status
		EndTimeUnixNano    int64                                 `json:"endTimeUnixNano,omitempty"`    // close execution
		CloseStatus        workflow.WorkflowExecutionCloseStatus `json:"closeStatus,omitempty"`        // close execution
		HistoryLength      int64                                 `json:"historyLength,omitempty"`      // close execution
		UpdateTimeUnixNano int64                                 `json:"updateTimeUnixNano,omitempty"` // update execution,
		ShardID            int64                                 `json:"shardID,omitempty"`
	}
)

var _ p.VisibilityStore = (*pinotVisibilityStore)(nil)

func NewPinotVisibilityStore(
	pinotClient *pinot.Connection,
	index string,
	producer messaging.Producer,
	logger log.Logger,
	config *service.Config,
) p.VisibilityStore {
	return &pinotVisibilityStore{
		pinotClient: pinotClient,
		index:       index,
		producer:    producer,
		logger:      logger.WithTags(tag.ComponentPinotVisibilityManager),
		config:      config,
	}
}

func (v *pinotVisibilityStore) Close() {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) GetName() string {
	return pinotPersistenceName
}

func (v *pinotVisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *p.InternalRecordWorkflowExecutionStartedRequest,
) error {
	v.checkProducer()
	msg := createVisibilityMessage(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.TaskList,
		request.StartTimestamp.UnixNano(),
		request.ExecutionTimestamp.UnixNano(),
		request.TaskID,
		request.Memo.Data,
		request.Memo.GetEncoding(),
		request.IsCron,
		request.NumClusters,
		request.SearchAttributes,
		common.RecordStarted,
		0,                                  // will not be used
		0,                                  // will not be used
		0,                                  // will not be used
		request.UpdateTimestamp.UnixNano(), // will be updated when workflow execution updates
		int64(request.ShardID),
	)
	return v.producer.Publish(ctx, msg)
}

func (v *pinotVisibilityStore) RecordWorkflowExecutionClosed(ctx context.Context, request *p.InternalRecordWorkflowExecutionClosedRequest) error {
	v.checkProducer()
	msg := createVisibilityMessage(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.TaskList,
		request.StartTimestamp.UnixNano(),
		request.ExecutionTimestamp.UnixNano(),
		request.TaskID,
		request.Memo.Data,
		request.Memo.GetEncoding(),
		request.IsCron,
		request.NumClusters,
		request.SearchAttributes,
		common.RecordClosed,
		request.CloseTimestamp.UnixNano(),
		*thrift.FromWorkflowExecutionCloseStatus(&request.Status),
		request.HistoryLength,
		request.UpdateTimestamp.UnixNano(),
		int64(request.ShardID),
	)
	return v.producer.Publish(ctx, msg)
}

func (v *pinotVisibilityStore) RecordWorkflowExecutionUninitialized(ctx context.Context, request *p.InternalRecordWorkflowExecutionUninitializedRequest) error {
	v.checkProducer()
	msg := createVisibilityMessage(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		"",
		0,
		0,
		0,
		nil,
		"",
		false,
		0,
		nil,
		"",
		0,
		0,
		0,
		request.UpdateTimestamp.UnixNano(),
		request.ShardID,
	)
	return v.producer.Publish(ctx, msg)
}

func (v *pinotVisibilityStore) UpsertWorkflowExecution(ctx context.Context, request *p.InternalUpsertWorkflowExecutionRequest) error {
	v.checkProducer()
	msg := createVisibilityMessage(
		request.DomainUUID,
		request.WorkflowID,
		request.RunID,
		request.WorkflowTypeName,
		request.TaskList,
		request.StartTimestamp.UnixNano(),
		request.ExecutionTimestamp.UnixNano(),
		request.TaskID,
		request.Memo.Data,
		request.Memo.GetEncoding(),
		request.IsCron,
		request.NumClusters,
		request.SearchAttributes,
		common.UpsertSearchAttributes,
		0, // will not be used
		0, // will not be used
		0, // will not be used
		request.UpdateTimestamp.UnixNano(),
		request.ShardID,
	)
	return v.producer.Publish(ctx, msg)
}

func (v *pinotVisibilityStore) ListClosedWorkflowExecutions(ctx context.Context, request *p.InternalListWorkflowExecutionsRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//isRecordValid := func(rec *p.InternalVisibilityWorkflowExecutionInfo) bool {
	//	return !request.EarliestTime.After(rec.CloseTime) && !rec.CloseTime.After(request.LatestTime)
	//} // not sure where to use

	ListClosedWorkflowExecutionsQuery := getListClosedWorkflowExecutionsQuery(request)
	resp, err := v.pinotClient.ExecuteSQL(tableName, ListClosedWorkflowExecutionsQuery)
	if err != nil {
		return nil, &types.InternalServiceError{
			Message: fmt.Sprintf("ListClosedWorkflowExecutions failed, %v", err),
		}
	}
	return getInternalListWorkflowExecutionsResponse(resp), nil
}

func getInternalListWorkflowExecutionsResponse(resp *pinot.BrokerResponse) *p.InternalListWorkflowExecutionsResponse {
	// TODO implement me
	return nil
}

/*
	SELECT *
	FROM {tableName}
	WHERE DomainId = {DomainID}

	AND CloseTime BETWEEN {earlistTime} AND {latestTime}

	AND CloseStatus = 1

	Order BY CloseTime DESC
	Order BY RunId DESC
*/

func getListClosedWorkflowExecutionsQuery(request *p.InternalListWorkflowExecutionsRequest) string {
	query := NewPinotQuery()

	query.filters.addEqual("DomainId", request.DomainUUID)
	query.filters.addTimeRange("CloseTime", request.EarliestTime, request.LatestTime)
	query.filters.addEqual("CloseStatus", "1")

	query.addPinotSorter("CloseTime", "DESC")
	query.addPinotSorter("RunId", "DESC")
	return query.String()
}

type PinotQuery struct {
	query   string
	filters PinotQueryFilter
	sorters string
}

type PinotQueryFilter struct {
	string
}

func (f *PinotQueryFilter) checkFirstFilter() {
	if f.string == "" {
		f.string = "WHERE "
	} else {
		f.string += "AND "
	}
}

func (f *PinotQueryFilter) addEqual(obj string, val string) {
	f.checkFirstFilter()
	f.string += fmt.Sprintf("%s = %s\n", obj, val)
}

func (f *PinotQueryFilter) addTimeRange(obj string, earlist time.Time, latest time.Time) {
	f.checkFirstFilter()
	f.string += fmt.Sprintf("%s BETWEEN %v AND %v\n", obj, earlist, latest)
}

func NewPinotQuery() PinotQuery {
	return PinotQuery{
		query:   fmt.Sprintf("SELECT *\nFROM %s\n", tableName),
		filters: PinotQueryFilter{},
		sorters: "",
	}
}

func (q *PinotQuery) String() string {
	return fmt.Sprintf("%s%s%s", q.query, q.filters.string, q.sorters)
}

func (q *PinotQuery) addPinotSorter(orderBy string, order string) {
	q.sorters += fmt.Sprintf("Order BY %s %s\n", orderBy, order)
}

func (v *pinotVisibilityStore) ListOpenWorkflowExecutionsByType(ctx context.Context, request *p.InternalListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListClosedWorkflowExecutionsByType(ctx context.Context, request *p.InternalListWorkflowExecutionsByTypeRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListOpenWorkflowExecutionsByWorkflowID(ctx context.Context, request *p.InternalListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListClosedWorkflowExecutionsByWorkflowID(ctx context.Context, request *p.InternalListWorkflowExecutionsByWorkflowIDRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListClosedWorkflowExecutionsByStatus(ctx context.Context, request *p.InternalListClosedWorkflowExecutionsByStatusRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) GetClosedWorkflowExecution(ctx context.Context, request *p.InternalGetClosedWorkflowExecutionRequest) (*p.InternalGetClosedWorkflowExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) DeleteWorkflowExecution(ctx context.Context, request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListWorkflowExecutions(ctx context.Context, request *p.ListWorkflowExecutionsByQueryRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ScanWorkflowExecutions(ctx context.Context, request *p.ListWorkflowExecutionsByQueryRequest) (*p.InternalListWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) CountWorkflowExecutions(ctx context.Context, request *p.CountWorkflowExecutionsRequest) (*p.CountWorkflowExecutionsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) DeleteUninitializedWorkflowExecution(ctx context.Context, request *p.VisibilityDeleteWorkflowExecutionRequest) error {
	//TODO implement me
	panic("implement me")
}

func (v *pinotVisibilityStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *p.InternalListWorkflowExecutionsRequest,
) (*p.InternalListWorkflowExecutionsResponse, error) {
	//not implemented
	return nil, nil
}

func (v *pinotVisibilityStore) checkProducer() {
	if v.producer == nil {
		// must be bug, check history setup
		panic("message producer is nil")
	}
}

func createVisibilityMessage(
	// common parameters
	domainID string,
	wid,
	rid string,
	workflowTypeName string,
	taskList string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	taskID int64,
	memo []byte,
	encoding common.EncodingType,
	isCron bool,
	NumClusters int16,
	searchAttributes map[string][]byte,
	visibilityOperation common.VisibilityOperation,
	// specific to certain status
	endTimeUnixNano int64, // close execution
	closeStatus workflow.WorkflowExecutionCloseStatus, // close execution
	historyLength int64, // close execution
	updateTimeUnixNano int64, // update execution,
	shardID int64,
) []byte {
	msg := visibilityMessage{
		DomainID:              domainID,
		Wid:                   wid,
		Rid:                   rid,
		WorkflowTypeName:      workflowTypeName,
		TaskList:              taskList,
		StartTimeUnixNano:     startTimeUnixNano,
		ExecutionTimeUnixNano: executionTimeUnixNano,
		TaskID:                taskID,
		Memo:                  memo,
		Encoding:              encoding,
		IsCron:                isCron,
		NumClusters:           NumClusters,
		SearchAttributes:      searchAttributes,
		VisibilityOperation:   visibilityOperation,
		EndTimeUnixNano:       endTimeUnixNano,
		CloseStatus:           closeStatus,
		HistoryLength:         historyLength,
		UpdateTimeUnixNano:    updateTimeUnixNano,
		ShardID:               shardID,
	}

	serializedMsg, err := json.Marshal(msg)
	if err != nil {
		panic("serialize msg error!")
	}

	return serializedMsg
}
