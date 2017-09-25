package solr

import (
	"fmt"
)

var ErrNotFound = NewNotFoundError("Not found")

type SolrError struct {
	errorMessage string
}

func (err SolrError) Error() string {
	return err.errorMessage
}

func NewSolrError(status int, message string) error {
	return SolrError{errorMessage: fmt.Sprintf("received error response from solr status: %d message: %s", status, message)}
}

func NewSolrRFError(rf, minRF int) error {
	return SolrMinRFError{SolrError{errorMessage: fmt.Sprintf("received error response from solr: rf (%d) is < min_rf (%d)", rf, minRF)}, rf}
}

type SolrMinRFError struct {
	SolrError
	MinRF int
}

type SolrInternalError struct {
	SolrError
}

func NewSolrInternalError(status int, message string) error {
	return SolrInternalError{SolrError{errorMessage: fmt.Sprintf("received error response from solr status: %d message: %s", status, message)}}
}

type SolrLeaderError struct {
	SolrError
}

func NewSolrLeaderError(docID string) error {
	return SolrLeaderError{SolrError{errorMessage: fmt.Sprintf("Cannot find leader for doc %s", docID)}}
}

type SolrBatchError struct {
	error
}

func NewSolrBatchError(err error) error {
	return SolrBatchError{error: err}
}

type SolrParseError struct {
	SolrError
}

func NewSolrParseError(status int, message string) error {
	return SolrInternalError{SolrError{errorMessage: fmt.Sprintf("received error response from solr status: %d message: %s", status, message)}}
}

type SolrMapParseError struct {
	bucket string
	m      map[string]interface{}
	userId int
}

func (err SolrMapParseError) Error() string {
	return fmt.Sprintf("SolrMapParseErr: map does not contain email_register, bucket: %s, userId: %d map: %v", err.bucket, err.userId, err.m)

}
func NewSolrMapParseError(bucket string, userId int, m map[string]interface{}) error {
	return SolrMapParseError{bucket, m, userId}
}

type NotFoundError struct {
	errorMessage string
}

func (err NotFoundError) Error() string {
	return err.errorMessage
}

func NewNotFoundError(error string) error {
	return NotFoundError{errorMessage: error}
}
