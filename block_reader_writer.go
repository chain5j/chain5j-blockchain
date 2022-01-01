// Package blockchain
//
// @author: xwc1125
package blockchain

import "github.com/chain5j/chain5j-protocol/protocol"

var (
	_ protocol.BlockReadWriter = new(blockRW)
)

// blockRW block读写对象
type blockRW struct {
	protocol.BlockReader // 读对象
	protocol.BlockWriter // 写对象
}

// NewBlockRW 创建block读写对象
func NewBlockRW(reader protocol.BlockReader, writer protocol.BlockWriter) (protocol.BlockReadWriter, error) {
	return &blockRW{
		BlockReader: reader,
		BlockWriter: writer,
	}, nil
}
