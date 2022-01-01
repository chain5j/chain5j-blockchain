// Package blockchain
//
// @author: xwc1125
package blockchain

import "github.com/chain5j/chain5j-pkg/types"

// WriteStatus 写的状态
type WriteStatus byte

const (
	NonStatTy       WriteStatus = iota
	CanonicalStatTy             // 标准状态
	SideStatTy                  // 侧链状态
)

// BadHashes 硬分叉的hash
var BadHashes = map[types.Hash]bool{}
