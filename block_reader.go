// Package blockchain
//
// @author: xwc1125
package blockchain

import (
	"fmt"
	"github.com/chain5j/chain5j-pkg/event"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/dateutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/models/eventtype"
	"github.com/chain5j/chain5j-protocol/pkg/database/basedb"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	"github.com/hashicorp/golang-lru"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ protocol.BlockReader = new(blockReader)
)

const (
	blockCacheLimit = 1024 // block缓存大小
	bodyCacheLimit  = 256  // body缓存大小
	maxFutureBlocks = 256  // 未入库区块缓存大小
)

// blockReader 管理链读取等操作
type blockReader struct {
	log      logger.Logger
	config   protocol.Config
	database protocol.Database // 数据库
	apis     protocol.APIs
	stateDB  basedb.Database

	hc           *headerChain  // 区块头
	genesisBlock *models.Block // 创世区块
	currentBlock atomic.Value  // 当前区块头,*models.Block

	chainBlockFeed event.Feed              // 区块订阅事件
	chainHeadFeed  event.Feed              // 区块头订阅事件
	chainSideFeed  event.Feed              // 侧链订阅事件
	scope          event.SubscriptionScope // 所有订阅

	gLock     sync.RWMutex // blockchain 操作全局锁
	chainLock sync.RWMutex // blockchain 插入区块锁

	blockCache *lru.Cache // 缓存block
	bodyCache  *lru.Cache // 缓存body

	running       int32          // running 必须原子操作
	procInterrupt int32          // 中断信息
	wg            sync.WaitGroup // 退出当前模块时，等待goroutine
}

// NewBlockReader 创建区块链服务
func NewBlockReader(config protocol.Config, database protocol.Database, apis protocol.APIs) (protocol.BlockReader, error) {
	// 创建缓存对象
	bodyCache, _ := lru.New(bodyCacheLimit)
	blockCache, _ := lru.New(blockCacheLimit)

	bc := &blockReader{
		log:      logger.New("blockchain"),
		config:   config,
		database: database,
		apis:     apis,

		blockCache: blockCache,
		bodyCache:  bodyCache,
	}

	var (
		err error
	)

	// new header
	bc.hc, err = newHeaderChain(bc, bc.database, bc.getProcInterrupt)
	if err != nil {
		bc.log.Error("new header err", "err", err)
		return nil, err
	}

	// 注册api
	bc.apis.RegisterAPI([]protocol.API{
		{
			Namespace: "blockchain",
			Version:   "1.0",
			Service:   NewAPI(bc),
			Public:    true,
		},
	})

	return bc, nil
}

func (bc *blockReader) Start() error {
	// 获取链配置信息
	chainConfig, err := bc.database.ChainConfig()
	if err != nil {
		bc.log.Error("get chain config err", "err", err)
		return err
	}
	// 获取创世块信息
	bc.genesisBlock = bc.GetBlockByNumber(chainConfig.GenesisHeight)
	if bc.genesisBlock == nil {
		bc.log.Error("get genesis block err", "err", errNoGenesis)
		return errNoGenesis
	}
	// 加载数据库中的最新状态
	if err := bc.loadLastState(); err != nil {
		return err
	}

	return nil
}

// Stop 停止区块链服务。
// 如果当前正在进行任何导入，它将使用procInterrupt中止导入。
func (bc *blockReader) Stop() error {
	if !atomic.CompareAndSwapInt32(&bc.running, 0, 1) {
		return nil
	}
	// 取消所有订阅
	bc.scope.Close()

	atomic.StoreInt32(&bc.procInterrupt, 1)
	// 如果正有区块在进行入库，等待其完成再打印
	bc.wg.Wait()

	bc.log.Info("blockchain service stopped")
	return nil
}

// IsRunning 判断blockchain是否正在运行
func (bc *blockReader) IsRunning() bool {
	return atomic.LoadInt32(&bc.running) == 1
}

// getProcInterrupt 判断是否被停止
func (bc *blockReader) getProcInterrupt() bool {
	return atomic.LoadInt32(&bc.procInterrupt) == 1
}

// Reset 恢复到创世区块状态
func (bc *blockReader) Reset() error {
	return bc.ResetWithGenesisBlock(bc.genesisBlock)
}

// ResetWithGenesisBlock 清除整个区块链，将其恢复到指定的创世纪状态
func (bc *blockReader) ResetWithGenesisBlock(genesis *models.Block) error {
	bc.gLock.Lock()
	defer bc.gLock.Unlock()

	return bc.resetWithGenesisBlock(genesis)
}

// resetWithGenesisBlock 清除整个区块链，将其恢复到指定的创世纪状态【不带锁】
func (bc *blockReader) resetWithGenesisBlock(genesis *models.Block) error {
	// 转储整个块链并清除缓存
	if err := bc.setHead(genesis.Height()); err != nil {
		return err
	}

	// 使用genesis重新初始化链
	bc.database.WriteBlock(genesis)

	bc.genesisBlock = genesis
	bc.insert(bc.genesisBlock)
	bc.currentBlock.Store(bc.genesisBlock)
	bc.hc.SetCurrentHeader(bc.genesisBlock.Header())
	bc.hc.SetGenesis(bc.genesisBlock.Header())

	return nil
}

// SetHead 将本地链回滚到指定位置
func (bc *blockReader) SetHead(head uint64) error {
	bc.log.Warn("Rewinding blockchain", "head", head)

	bc.gLock.Lock()
	defer bc.gLock.Unlock()

	return bc.setHead(head)
}

// setHead 将本地链回滚到指定位置【不带锁】
func (bc *blockReader) setHead(head uint64) error {
	// 回滚链，并将链所有的body都删除
	bc.hc.SetHead(head)
	currentHeader := bc.hc.CurrentHeader()

	// 清理所有的缓存
	bc.bodyCache.Purge()
	bc.blockCache.Purge()

	// 回滚链，确保不会出现无状态的头区块
	if currentBlock := bc.CurrentBlock(); currentBlock != nil && currentHeader.Height < currentBlock.Height() {
		bc.currentBlock.Store(bc.GetBlock(currentHeader.Hash(), currentHeader.Height))
	}
	// 如果当前区块到达nil，那么用创世块填充
	if currentBlock := bc.CurrentBlock(); currentBlock == nil {
		bc.currentBlock.Store(bc.genesisBlock)
	}

	currentBlock := bc.CurrentBlock()
	bc.database.WriteLatestBlockHash(currentBlock.Hash())
	return bc.loadLastState()
}

// loadLastState 从数据库加载已知状态
func (bc *blockReader) loadLastState() error {
	// 加载最新的区块hash
	headHash, err := bc.database.LatestBlockHash()
	if err != nil {
		bc.log.Warn("get latest blockHash err", "err", err)
	}
	if headHash == (types.Hash{}) {
		// 损坏或空数据库，从头开始初始化
		bc.log.Warn("Empty database, resetting chain")
		return bc.resetWithGenesisBlock(bc.genesisBlock)
	}
	// 根据头hash获取区块信息
	currentBlock := bc.GetBlockByHash(headHash)
	if currentBlock == nil {
		// 损坏或空数据库，从头开始初始化
		bc.log.Warn("Head block missing, resetting chain", "hash", headHash)
		return bc.resetWithGenesisBlock(bc.genesisBlock)
	}

	// 存储为当前区块
	bc.currentBlock.Store(currentBlock)
	// 设置当前的header
	currentHeader := currentBlock.Header()
	// 从库中获取最新的header
	header, err := bc.database.LatestHeader()
	if err != nil {
		bc.log.Warn("get latest header err", "err", err)
	}
	if header != nil {
		currentHeader = header
	}
	bc.hc.SetCurrentHeader(currentHeader)
	bc.log.Info("loaded most recent local header", "height", currentHeader.Height, "hash", currentHeader.Hash(), "age", dateutil.PrettyAge(dateutil.MillisecondToTime(int64(currentHeader.Timestamp))))

	return nil
}

// GasLimit 返回当前header的gasLimit
func (bc *blockReader) GasLimit() uint64 {
	return bc.CurrentBlock().GasLimit()
}

// InsertBlock 写入区块
// propagate 是否广播区块内容
func (bc *blockReader) InsertBlock(block *models.Block, propagate bool) (err error) {
	t1 := time.Now()
	var status WriteStatus
	bc.wg.Add(1)
	defer bc.wg.Done()

	// 获取父header
	h := bc.GetHeader(block.ParentHash(), block.Height()-1)
	if h == nil {
		return errUnknownAncestor
	}
	if bc.config.BlockchainConfig().IsMetrics(1) {
		bc.log.Debug("get header", "parentHeight", block.Height()-1, "parentHash", block.ParentHash(), "elapsed", dateutil.PrettyDuration(time.Since(t1)))
	}

	bc.gLock.Lock()
	defer bc.gLock.Unlock()

	t2 := time.Now()
	// 获取当前区块
	currentBlock := bc.CurrentBlock()
	// 写入当前区块
	bc.database.WriteBlock(block)
	if bc.config.BlockchainConfig().IsMetrics(1) {
		bc.log.Debug("write block", "height", block.Height(), "hash", block.Hash(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
	}
	t2 = time.Now()
	// 如果父hash不等于当前区块的hash，那么需要重新组链
	if block.ParentHash() != currentBlock.Hash() {
		if block.Height() > currentBlock.Height() {
			bc.currentBlock.Store(block)
			bc.hc.SetCurrentHeader(block.Header())
			status = CanonicalStatTy
		} else {
			status = SideStatTy
		}
	} else {
		status = CanonicalStatTy
	}
	if bc.config.BlockchainConfig().IsMetrics(1) {
		bc.log.Debug("set status", "status", status, "elapsed", dateutil.PrettyDuration(time.Since(t2)))
	}
	// 设置新的区块
	if status == CanonicalStatTy {
		t2 = time.Now()
		bc.insert(block)
		bc.database.WriteTxsLookup(block)
		if bc.config.BlockchainConfig().IsMetrics(1) {
			bc.log.Debug("write canonical stat", "height", block.Height(), "hash", block.Hash(), "elapsed", dateutil.PrettyDuration(time.Since(t2)))
		}
	}

	var events []interface{}
	switch status {
	case CanonicalStatTy:
		events = append(events, eventtype.ChainEvent{Block: block, Hash: block.Hash()})
		events = append(events, eventtype.ChainHeadEvent{Block: block})
	case SideStatTy:
		events = append(events, eventtype.ChainSideEvent{Block: block})
	}
	// 广播区块内容
	if propagate {
		bc.postChainEvents(events)
	}
	bc.log.Info("InsertBlock", "height", block.Height(), "hash", block.Hash(), "txsLen", block.Body().Txs.Len(), "elapsed", dateutil.PrettyDuration(time.Since(t1)))
	return nil
}

// insert 将一个新的head块注入当前块链。该方法假定该块确实是一个真实的头【不带锁】
func (bc *blockReader) insert(block *models.Block) {
	// 如果当前块位于侧链或未知链上，则将其他头部也压在其上
	var updateHeads = true
	if canonicalHash, err := bc.database.GetCanonicalHash(block.Height()); err != nil {
		bc.log.Warn("get canonical hash err", "height", block.Height(), "err", err)
	} else if canonicalHash != block.Hash() {
		updateHeads = canonicalHash != block.Hash()
	}

	// 将区块写入标准链中，并标记为head
	if err := bc.database.WriteCanonicalHash(block.Hash(), block.Height()); err != nil {
		bc.log.Warn("write canonical hash err", "hash", block.Hash().Hex(), "height", block.Height(), "err", err)
	}
	if err := bc.database.WriteLatestBlockHash(block.Hash()); err != nil {
		bc.log.Warn("write latest block hash err", "hash", block.Hash().Hex(), "err", err)
	}

	bc.currentBlock.Store(block)
	// 如果块比我们的头好，或者在不同的链上，强制更新头
	if updateHeads {
		bc.hc.SetCurrentHeader(block.Header())
	}
}

// CurrentHeader 获取当前区块头
func (bc *blockReader) CurrentHeader() *models.Header {
	return bc.hc.CurrentHeader()
}

// GetHeader 根据hash及number查询区块头
func (bc *blockReader) GetHeader(hash types.Hash, height uint64) *models.Header {
	return bc.hc.GetHeader(hash, height)
}

// GetHeaderByHash 根据hash查询区块头
func (bc *blockReader) GetHeaderByHash(hash types.Hash) *models.Header {
	return bc.hc.GetHeaderByHash(hash)
}

// GetHeaderByNumber 根据number查询区块头
func (bc *blockReader) GetHeaderByNumber(height uint64) *models.Header {
	return bc.hc.GetHeaderByNumber(height)
}

// HasHeader 根据hash及number判断区块头是否存在
func (bc *blockReader) HasHeader(hash types.Hash, height uint64) bool {
	return bc.hc.HasHeader(hash, height)
}

// CurrentBlock 获取当前区块
func (bc *blockReader) CurrentBlock() *models.Block {
	return bc.currentBlock.Load().(*models.Block)
}

// GetBlock 根据hash及number查询区块
func (bc *blockReader) GetBlock(hash types.Hash, height uint64) *models.Block {
	// 根据hash从缓存读取
	if block, ok := bc.blockCache.Get(hash); ok {
		return block.(*models.Block)
	}
	block, err := bc.database.GetBlock(hash, height)
	if err != nil {
		bc.log.Warn("get block by hash and height err", "hash", hash, "height", height, "err", err)
	}
	if block == nil {
		return nil
	}
	// 将从数据库中读取的区块存入缓存
	bc.blockCache.Add(block.Hash(), block)
	return block
}

// GetBlockByHash 根据hash查询区块
func (bc *blockReader) GetBlockByHash(hash types.Hash) *models.Block {
	// 根据hash从缓存读取
	if block, ok := bc.blockCache.Get(hash); ok {
		return block.(*models.Block)
	}
	block, err := bc.database.GetBlockByHash(hash)
	if err != nil {
		bc.log.Warn("get block by hash err", "hash", hash.Hex(), "err", err)
	}
	return block
}

// GetBlockByNumber 根据number查询区块
func (bc *blockReader) GetBlockByNumber(height uint64) *models.Block {
	block, err := bc.database.GetBlockByHeight(height)
	if err != nil {
		bc.log.Warn("get block by height", "height", height, "err", err)
	}
	return block
}

// HasBlock 根据hash及number判断区块是否存在
func (bc *blockReader) HasBlock(hash types.Hash, height uint64) bool {
	if bc.blockCache.Contains(hash) {
		return true
	}
	b, err := bc.database.HasBlock(hash, height)
	if err != nil {
		bc.log.Warn("has block err", "err", err)
	}
	return b
}

// GetBlockHashesFromHash 从指定hash开始获取一系列区块hash, 降序
func (bc *blockReader) GetBlockHashesFromHash(bHash types.Hash, max uint64) []types.Hash {
	// 根据hash从缓存读取
	bHashes := make([]types.Hash, 0, max)
	for {
		if block, ok := bc.blockCache.Get(bHash); ok {
			blockTemp := block.(*models.Block)
			bHashes = append(bHashes, blockTemp.Hash())
			bHash = blockTemp.ParentHash()
			max = max - 1
		} else {
			break
		}
	}
	bHashes = append(bHashes, bc.hc.GetBlockHashesFromHash(bHash, max)...)
	return bHashes
}

// GetBody 根据hash查询区块的交易内容及叔块信息
func (bc *blockReader) GetBody(hash types.Hash) *models.Body {
	// 从缓存中查询
	if cached, ok := bc.bodyCache.Get(hash); ok {
		body := cached.(*models.Body)
		return body
	}
	// 根据hash从缓存读取
	if block, ok := bc.blockCache.Get(hash); ok {
		return block.(*models.Block).Body()
	}
	height := bc.hc.GetBlockNumber(hash)
	if height == nil {
		return nil
	}
	body, _ := bc.database.GetBody(hash, *height)
	if body == nil {
		return nil
	}
	// 将查询到的信息存入缓存
	body.Height = *height
	bc.bodyCache.Add(hash, body)
	return body
}

// ValidateBody 验证区块的body
func (bc *blockReader) ValidateBody(block *models.Block) error {
	header := block.Header()

	// 验证区块的交易hash
	if !reflect.DeepEqual(block.Transactions().TxsRoot(), header.TxsRoot) {
		return fmt.Errorf("transaction root hash mismatch: have %x, want %x", block.Transactions().TxsRoot(), header.TxsRoot)
	}

	return nil
}

//func (bc *blockReader) HasBlockAndState(hash types.Hash, number uint64) bool {
//	block := bc.GetBlock(hash, number)
//	if block == nil {
//		return false
//	}
//	return bc.HasState(block.StateRoots())
//}
//
//// HasState 判断root是否存在于状态数据库
//func (bc *blockReader) HasState(root types.Hash) bool {
//	_, err := bc.stateDB.OpenTree(root)
//	return err == nil
//}

// postChainEvents 循环将事件发送给订阅方
func (bc *blockReader) postChainEvents(events []interface{}) {
	for _, event := range events {
		switch ev := event.(type) {
		case eventtype.ChainEvent:
			bc.chainBlockFeed.Send(ev)
		case eventtype.ChainHeadEvent:
			bc.chainHeadFeed.Send(ev)
		case eventtype.ChainSideEvent:
			bc.chainSideFeed.Send(ev)
		}
	}
}

// GetAncestor 根据hash及number获取其对应的第n代祖先的hash及区块高度
func (bc *blockReader) GetAncestor(hash types.Hash, height, ancestor uint64, maxNonCanonical *uint64) (types.Hash, uint64) {
	return bc.hc.GetAncestor(hash, height, ancestor, maxNonCanonical)
}

// SubscribeChainEvent 订阅链事件
func (bc *blockReader) SubscribeChainEvent(ch chan<- eventtype.ChainEvent) event.Subscription {
	return bc.scope.Track(bc.chainBlockFeed.Subscribe(ch))
}

// SubscribeChainHeadEvent 订阅区块事件
func (bc *blockReader) SubscribeChainHeadEvent(ch chan<- eventtype.ChainHeadEvent) event.Subscription {
	return bc.scope.Track(bc.chainHeadFeed.Subscribe(ch))
}

// SubscribeChainSideEvent 订阅侧链事件
func (bc *blockReader) SubscribeChainSideEvent(ch chan<- eventtype.ChainSideEvent) event.Subscription {
	return bc.scope.Track(bc.chainSideFeed.Subscribe(ch))
}
