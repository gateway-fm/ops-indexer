package indexer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
)

var errNoReceipts = fmt.Errorf("%w: eth_getBlockReceipts returned null", errReceiptsUnavailable)

func TestReceiptSkipRecordsTheBlockAfterMaxAttempts(t *testing.T) {
	db := new(MockDatabase)
	db.On("RequeueMissingBlock", mock.Anything, uint64(7)).Return(nil).Once()
	var s receiptSkip

	for attempt := 1; attempt < maxReceiptAttempts; attempt++ {
		if s.skip(context.Background(), db, 7, errNoReceipts) {
			t.Fatalf("attempt %d: skipped before %d attempts", attempt, maxReceiptAttempts)
		}
	}
	if !s.skip(context.Background(), db, 7, errNoReceipts) {
		t.Fatalf("attempt %d: block not skipped", maxReceiptAttempts)
	}
	db.AssertExpectations(t)
}

func TestReceiptSkipNeverSkipsOtherErrors(t *testing.T) {
	db := new(MockDatabase)
	var s receiptSkip
	for range 2 * maxReceiptAttempts {
		if s.skip(context.Background(), db, 7, errors.New("connection refused")) {
			t.Fatal("skipped a block on an error that is not about its receipts")
		}
	}
	db.AssertNotCalled(t, "RequeueMissingBlock", mock.Anything, mock.Anything)
}

func TestReceiptSkipKeepsTheBlockWhenItCannotBeRecorded(t *testing.T) {
	db := new(MockDatabase)
	db.On("RequeueMissingBlock", mock.Anything, uint64(7)).Return(errors.New("database down"))
	var s receiptSkip
	for range 2 * maxReceiptAttempts {
		if s.skip(context.Background(), db, 7, errNoReceipts) {
			t.Fatal("skipped a block that was not recorded in missing_block_ranges")
		}
	}
}

func TestReceiptSkipCountsAttemptsPerBlock(t *testing.T) {
	db := new(MockDatabase)
	var s receiptSkip
	for range maxReceiptAttempts - 1 {
		s.skip(context.Background(), db, 7, errNoReceipts)
	}
	if s.skip(context.Background(), db, 8, errNoReceipts) {
		t.Fatal("block 8 skipped on its first attempt: attempts carried over from block 7")
	}
	db.AssertNotCalled(t, "RequeueMissingBlock", mock.Anything, mock.Anything)
}
