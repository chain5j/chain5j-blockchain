// Package blockchain
//
// @author: xwc1125
package blockchain

import "errors"

var (
	errAborted              = errors.New("aborted")
	errNoGenesis            = errors.New("genesis not found in chain")
	errUnknownAncestor      = errors.New("unknown ancestor")
	errKnownBlock           = errors.New("block already known")
	errTimestampInvalid     = errors.New("timestamp of the block should be larger than that of parent block")
	errTxRootNotEqual       = errors.New("txs root is not equal")
	errReceiptRootNotEqual  = errors.New("receipts root is not equal")
	errStateRootNotEqual    = errors.New("state root is not equal")
	errGasUsedOverflow      = errors.New("gas used is larger than gas limit")
	errParentHeightNotMatch = errors.New("parent height mismatch")
	errExtraDataOverflow    = errors.New("extra data is too long")
	errBlockExist           = errors.New("block already existed in db")
	errParentNotExist       = errors.New("parent not exist")
	errBlacklistedHash      = errors.New("blacklisted hash")
	ErrFutureBlock          = errors.New("block in the future")
)
