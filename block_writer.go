// Package blockchain
//
// @author: xwc1125
package blockchain

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/chain5j/chain5j-pkg/util/dateutil"
	"github.com/chain5j/chain5j-pkg/util/hexutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	"github.com/hashicorp/golang-lru"
	"sort"
	"time"
)

var (
	_ protocol.BlockWriter = &blockWriter{}
)

const (
	maxTimeFutureBlocks = 30 * 1000       // 缓存即将入库的block(毫秒),30秒的区块时间容错
	futureTimerInter    = 3 * time.Second // 每3秒处理一次未来区块
)

// blockWriter block写操作
type blockWriter struct {
	log logger.Logger

	engine protocol.Consensus
	apps   protocol.Apps

	blockReader  *blockReader
	futureBlocks *lru.Cache // 缓存即将入库的区块
}

// NewBlockWriter 创建blockWriter
func NewBlockWriter(reader protocol.BlockReader,
	engine protocol.Consensus,
	apps protocol.Apps) *blockWriter {
	futureBlocks, _ := lru.New(maxFutureBlocks)
	w := &blockWriter{
		log:          logger.New("blockchain"),
		blockReader:  reader.(*blockReader),
		engine:       engine,
		apps:         apps,
		futureBlocks: futureBlocks,
	}

	go w.listen()

	return w
}

// ProcessBlock 同步或接收到广播的区块信息
func (bw *blockWriter) ProcessBlock(block *models.Block, propagate bool) error {
	// 判断区块是否已经存在，如果存在返回错误
	if bw.blockReader.HasBlock(block.Hash(), block.Height()) {
		return errKnownBlock
	}

	// 使用共识验证区块头
	err := bw.engine.VerifyHeader(bw.blockReader, block.Header())
	if err != nil {
		bw.log.Error("ProcessBlock VerifyHeader err", "err", err)
		return err
	}

	switch {
	case err == ErrFutureBlock:
		// 30秒的区块时间容错
		max := uint64(dateutil.CurrentTime() + maxTimeFutureBlocks)
		if block.Timestamp() > max {
			return fmt.Errorf("future block: %v > %v", block.Timestamp(), max)
		}
		bw.futureBlocks.Add(block.Hash(), block)
		return nil

	case err == errUnknownAncestor && bw.futureBlocks.Contains(block.ParentHash()):
		bw.futureBlocks.Add(block.Hash(), block)
		return nil
	}

	// 验证区块的body
	if err := bw.blockReader.ValidateBody(block); err != nil {
		bw.log.Error("validate body err", "err", err)
		return err
	}

	t1 := time.Now()

	// block可以正常入库
	if block.Transactions().Len() > 0 {
		// 修改state
		parentBlock := bw.blockReader.GetBlock(block.ParentHash(), block.Height()-1)

		ctx, err := bw.apps.NewAppContexts("process_block", parentBlock.Header().StateRoots)

		stateRoots, resultTxStatus := bw.apps.Prepare(ctx, parentBlock.Header().StateRoots, block.Header(), block.Transactions(), block.Header().GasLimit)
		if resultTxStatus == nil {
			return errors.New("result status is nil")
		}
		if bw.blockReader.config.BlockchainConfig().IsMetrics(2) {
			bw.log.Trace("stateRoots", "root", stateRoots, "headerRoot", block.Header().StateRoots)
		}
		if bytes.Compare(stateRoots, block.Header().StateRoots) != 0 {
			bw.log.Error("stateRoots is diff", "stateRoot", hexutil.Encode(stateRoots), "headerRoot", hexutil.Encode(block.Header().StateRoots))
			return errors.New("stateRoots is diff")
		}
		err = bw.apps.Commit(ctx, block.Header())
		if err != nil {
			return err
		}
	}
	bw.log.Info("block writer process transaction", "elapsed", dateutil.PrettyDuration(time.Since(t1)))

	return bw.InsertBlock(block, propagate)
}

// InsertBlock 插入区块
func (bw *blockWriter) InsertBlock(block *models.Block, propagate bool) (err error) {
	return bw.blockReader.InsertBlock(block, propagate)
}

// update 定时器
func (bw *blockWriter) listen() {
	// 每3秒处理一次未来区块
	futureTimer := time.NewTicker(futureTimerInter)
	defer futureTimer.Stop()
	for {
		select {
		case <-futureTimer.C:
			bw.procFutureBlocks()
		}
		if !bw.blockReader.IsRunning() {
			// 如果blockReader已经停止了，那么退出循环
			return
		}
	}
}

// procFutureBlocks 处理即将入库的区块
func (bw *blockWriter) procFutureBlocks() {
	blocks := make([]*models.Block, 0, bw.futureBlocks.Len())

	for _, hash := range bw.futureBlocks.Keys() {
		if block, exist := bw.futureBlocks.Peek(hash); exist {
			blocks = append(blocks, block.(*models.Block))
		}
	}

	if len(blocks) > 0 {
		blocksSort := models.NewBlocks(blocks)
		//BlockBy(SortByHeight).Sort(blocks)
		sort.Sort(blocksSort)

		// 逐个插入
		for i := range blocksSort.Data() {
			bw.ProcessBlock(blocksSort.Get(i), false)
			if !bw.blockReader.IsRunning() {
				return
			}
		}
	}
}
