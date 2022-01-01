// Package blockchain
//
// @author: xwc1125
package blockchain

import (
	"context"
	"github.com/chain5j/chain5j-pkg/network/rpc"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/hexutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/models/eventtype"
	"github.com/chain5j/chain5j-protocol/protocol"
)

type API struct {
	chain protocol.BlockReader
}

func NewAPI(bc protocol.BlockReader) *API {
	return &API{chain: bc}
}

func (api *API) BlockHeight(ctx context.Context) hexutil.Uint64 {
	return hexutil.Uint64(api.chain.CurrentBlock().Height())
}

func (api *API) GetBlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (map[string]interface{}, error) {
	block := api.chain.GetBlockByNumber(uint64(blockNr))

	if block == nil {
		return nil, nil
	}

	response, err := RPCMarshalBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (api *API) GetBlockByHash(ctx context.Context, blockHash types.Hash) (map[string]interface{}, error) {
	block := api.chain.GetBlockByHash(blockHash)

	if block == nil {
		return nil, nil
	}

	response, err := RPCMarshalBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (api *API) GetBlockTransactionCountByHash(ctx context.Context, blockHash types.Hash) *hexutil.Uint {
	block := api.chain.GetBlockByHash(blockHash)

	if block == nil {
		return nil
	}
	n := hexutil.Uint(block.Transactions().Len())
	return &n
}

func RPCMarshalBlock(ctx context.Context, b *models.Block) (map[string]interface{}, error) {
	head := b.Header()
	fields := map[string]interface{}{
		"height":           head.Height,
		"hash":             b.Hash(),
		"parentHash":       head.ParentHash,
		"stateRoots":       head.StateRoots,
		"consensus":        head.Consensus,
		"extra":            hexutil.Bytes(head.Extra),
		"size":             hexutil.Uint64(b.Size()),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        head.Timestamp,
		"transactionsRoot": head.TxsRoot,
	}

	txs := b.Transactions()
	transactions := make([]interface{}, txs.Len())
	for i, tx := range txs.Data() {
		transactions[i] = tx
	}
	fields["transactions"] = transactions

	return fields, nil
}

// NewHeads send a notification each time a new (header) block is appended to the chain.
func (api *API) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		headers := make(chan eventtype.ChainHeadEvent)
		headersSub := api.chain.SubscribeChainHeadEvent(headers)

		for {
			select {
			case h := <-headers:
				notifier.Notify(rpcSub.ID, h.Block.Header())
			case <-rpcSub.Err():
				headersSub.Unsubscribe()
				return
			case <-notifier.Closed():
				headersSub.Unsubscribe()
				return
			}
		}
	}()

	return rpcSub, nil
}
