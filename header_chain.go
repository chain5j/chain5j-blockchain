// Package blockchain
//
// @author: xwc1125
package blockchain

import (
	"crypto/rand"
	"fmt"
	"github.com/chain5j/chain5j-pkg/math"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-pkg/util/dateutil"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
	"github.com/hashicorp/golang-lru"
	"math/big"
	mrand "math/rand"
	"sync/atomic"
	"time"
)

const (
	headerCacheLimit = 512  // 当前区块头缓存数
	numberCacheLimit = 2048 // 当前区块高度缓存数
)

// headerChain 实现基本的区块头逻辑，非线程安全，操作是需要包含互斥锁
type headerChain struct {
	log         logger.Logger
	database    protocol.Database
	blockReader protocol.BlockReader
	//engine      protocol.Consensus

	genesisHeader *models.Header // 创世区块

	currentHeader     atomic.Value // 当前区块头
	currentHeaderHash types.Hash   // 当前区块头hash

	headerCache *lru.Cache // 最近的区块头，header
	numberCache *lru.Cache // 最近的区块高度，uint64

	procInterrupt func() bool // 进程中断函数

	rand *mrand.Rand
}

// newHeaderChain 初始化HeaderChain
func newHeaderChain(blockReader protocol.BlockReader, database protocol.Database, procInterrupt func() bool) (*headerChain, error) {
	headerCache, _ := lru.New(headerCacheLimit)
	numberCache, _ := lru.New(numberCacheLimit)
	seed, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))

	hc := &headerChain{
		log:         logger.New("blockchain"),
		database:    database,
		blockReader: blockReader,
		//engine:      engine,

		headerCache:   headerCache,
		numberCache:   numberCache,
		procInterrupt: procInterrupt,
		rand:          mrand.New(mrand.NewSource(seed.Int64())),
	}

	return hc, nil
}

func (hc *headerChain) Start() (err error) {
	// 获取创世块内容
	// 获取最新的链配置信息
	{
		// 读取创世块
		chainConfig, err := hc.database.ChainConfig()
		if err != nil {
			hc.log.Error("get chainConfig err", "err", err)
			return err
		}
		// 根据链配置获取创世块文件
		hc.genesisHeader, err = hc.database.GetHeaderByHeight(chainConfig.GenesisHeight)
		if err != nil {
			hc.log.Error("get genesis header err", "err", err)
			return err
		}
		if hc.genesisHeader == nil {
			hc.log.Error("get header by number err", "err", errNoGenesis)
			return errNoGenesis
		}
		hc.currentHeader.Store(hc.genesisHeader)
	}

	// 获取最新的区块头hash及最新的区块头
	if latestHeader, err := hc.database.LatestHeader(); err != nil {
		hc.log.Warn("get current header err", "err", err)
	} else {
		if latestHeader != nil {
			hc.currentHeader.Store(latestHeader)
		}
	}

	// 当前header的hash
	hc.currentHeaderHash = hc.CurrentHeader().Hash()
	return nil
}

// CurrentHeader 从缓存中获取当前区块头
func (hc *headerChain) CurrentHeader() *models.Header {
	return hc.currentHeader.Load().(*models.Header)
}

// SetCurrentHeader 设置当前区块头
func (hc *headerChain) SetCurrentHeader(head *models.Header) {
	// 写入最新的区块头Hash
	if err := hc.database.WriteLatestHeaderHash(head.Hash()); err != nil {
		hc.log.Warn("write latest header hash err", "hash", head.Hash().Hex(), "err", err)
	}

	// 内存存储header
	hc.currentHeader.Store(head)
	hc.currentHeaderHash = head.Hash()
}

// GetHeader 根据区块号和hash检索区块头, 并做缓存
func (hc *headerChain) GetHeader(hash types.Hash, number uint64) *models.Header {
	if header, ok := hc.headerCache.Get(hash); ok {
		return header.(*models.Header)
	}
	// 根据hash及number获取header
	header, err := hc.database.GetHeader(hash, number)
	if err != nil {
		hc.log.Warn("get header by hash and number err", "hash", hash.Hex(), "number", number, "err", err)
	}
	if header == nil {
		return nil
	}
	// 进行缓存处理
	hc.headerCache.Add(hash, header)
	return header
}

// GetHeaderByHash 根据区块hash检索区块头， 并做缓存
func (hc *headerChain) GetHeaderByHash(hash types.Hash) *models.Header {
	header, err := hc.database.GetHeaderByHash(hash)
	if err != nil {
		hc.log.Warn("get header by hash", "hash", hash.Hex(), "err", err)
	}
	return header
}

// GetHeaderByNumber 根据区块号检索区块头，并做缓存
func (hc *headerChain) GetHeaderByNumber(number uint64) *models.Header {
	// 根据区块高度读取规范区块头hash
	header, err := hc.database.GetHeaderByHeight(number)
	if err != nil {
		hc.log.Warn("get header by height", "number", number, "err", err)
	}
	return header
}

// HasHeader 检查区块头是否在DB中存在
func (hc *headerChain) HasHeader(hash types.Hash, number uint64) bool {
	if hc.numberCache.Contains(hash) || hc.headerCache.Contains(hash) {
		return true
	}
	b, err := hc.database.HasHeader(hash, number)
	if err != nil {
		hc.log.Warn("has header err", "err", err)
	}
	return b
}

// GetBlockNumber 通过区块hash从缓存或者DB中获取区块高度
func (hc *headerChain) GetBlockNumber(hash types.Hash) *uint64 {
	if cached, ok := hc.numberCache.Get(hash); ok {
		number := cached.(uint64)
		return &number
	}
	// 根据hash读取区块高度
	number, _ := hc.database.GetHeaderHeight(hash)
	if number != nil {
		hc.numberCache.Add(hash, *number)
	}
	return number
}

// WriteHeader 将区块头写入当前链，如果写入区块高度大于当前区块高度，就作为当前规范链。
// 非线程安全。
func (hc *headerChain) WriteHeader(header *models.Header) (status WriteStatus, err error) {
	var (
		hash   = header.Hash()
		height = header.Height
	)

	// 将header写入db
	if err := hc.database.WriteHeader(header); err != nil {
		hc.log.Warn("writer header err", "err", err)
		return NonStatTy, err
	}

	if header.Height > hc.CurrentHeader().Height {
		// 如果新的header高度大于内存中的区块高度
		// 那么需要删除所有比header高度区块头
		var currentHeight, desHeight uint64
		for i := height + 1; ; i++ {
			// 根据高度查询标准区块头hash
			if hash, err := hc.database.GetCanonicalHash(i); err != nil {
				break
			} else if hash == (types.EmptyHash) {
				break
			}
			// 根据高度删除标准区块头
			if desHeight == 0 {
				desHeight = i
			}
			if currentHeight < i {
				currentHeight = i
			}
		}
		hc.database.DeleteBlock(nil, currentHeight, desHeight)

		var (
			headHash   = header.ParentHash
			headNumber = header.Height - 1
			// 根据父hash及高度减1查询父区块头
			headHeader = hc.GetHeader(headHash, headNumber)
		)
		// 如果数据库中标准的父区块头hash不等于当前区块头的父hash
		// 那么说明，数据库中的标准父区块头已经不匹配了，也可以体检为已经发生了分叉，
		// 或数据不一致，此时就需要不但回滚重写数据库中的区块头
		if canonicalHash, err := hc.database.GetCanonicalHash(headNumber); err != nil {
			hc.log.Error("get canonical hash", "height", headNumber, "err", err)
		} else if canonicalHash != headHash {
			// 覆盖重写标准区块头及hash
			if err := hc.database.WriteCanonicalHash(headHash, headNumber); err != nil {
				hc.log.Warn("write canonical hash err", "hash", headHash.Hex(), "height", headNumber, "err", err)
			}

			headHash = headHeader.ParentHash
			headNumber = headHeader.Height - 1
			headHeader = hc.GetHeader(headHash, headNumber)
		}
		// 用新的高度及hash扩展链
		if err := hc.database.WriteCanonicalHash(hash, height); err != nil {
			hc.log.Warn("write canonical hash err", "hash", hash.Hex(), "height", height, "err", err)
			return NonStatTy, err
		}
		if err := hc.database.WriteLatestBlockHash(hash); err != nil {
			hc.log.Warn("write latest block hash err", "hash", hash.Hex(), "err", err)
			return NonStatTy, err
		}

		hc.currentHeaderHash = hash
		hc.currentHeader.Store(models.CopyHeader(header))

		status = CanonicalStatTy
	} else {
		status = SideStatTy
	}

	hc.headerCache.Add(hash, header)
	hc.numberCache.Add(hash, height)

	return
}

// ValidateHeaderChain 区块头校验是否合法
// 当同步下载后，进行区块头验证使用
func (hc *headerChain) ValidateHeaderChain(chain []*models.Header, checkFreq int) (int, error) {
	// 进行健全性检查，确保所提供的链实际上是有序和连续的
	for i := 1; i < len(chain); i++ {
		if chain[i].Height != chain[i-1].Height+1 || chain[i].ParentHash != chain[i-1].Hash() {
			// 如果后一个的高度不等于前一个高度加1，
			// 或后一个的父hash不等于前一个的hash，
			// 那么说明不连续，则需要返回错误
			hc.log.Error("Non contiguous header insert", "number", chain[i].Height, "hash", chain[i].Hash(),
				"parent", chain[i].ParentHash, "prevNumber", chain[i-1].Height, "prevHash", chain[i-1].Hash())

			return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, chain[i-1].Height,
				chain[i-1].Hash().Bytes()[:4], i, chain[i].Height, chain[i].Hash().Bytes()[:4], chain[i].ParentHash[:4])
		}
	}

	// TODO 在共识引擎中校验区块头是否合法，
	//// 生成需要验签的列表
	//seals := make([]bool, len(chain))
	//for i := 0; i < len(seals)/checkFreq; i++ {
	//	index := i*checkFreq + hc.rand.Intn(checkFreq)
	//	if index >= len(seals) {
	//		index = len(seals) - 1
	//	}
	//	seals[index] = true
	//}
	//seals[len(seals)-1] = true // 最后一个一定设置为true，避免空
	//
	//abort, results := hc.engine.VerifyHeaders(hc.blockReader, chain, seals)
	//defer close(abort)
	//
	//// 循环判断，保证所有的header被签
	//for i, header := range chain {
	//	if hc.procInterrupt() {
	//		hc.log.Debug("Premature abort during headers verification")
	//		return 0, errAborted
	//	}
	//	// 如果hash被禁，那么直接返回错误
	//	if BadHashes[header.Hash()] {
	//		return i, errBlacklistedHash
	//	}
	//	// 等待header验证成功
	//	if err := <-results; err != nil {
	//		return i, err
	//	}
	//}

	return 0, nil
}

// WhCallback 插入单个区块是的回调函数
type WhCallback func(*models.Header) error

// InsertHeaderChain 尝试将给定的区块头写入当前链，可能会造成重组。
// 如果产生错误，返回错误原因，并返回产生错误的header索引。
func (hc *headerChain) InsertHeaderChain(chain []*models.Header, writeHeader WhCallback, start time.Time) (int, error) {
	// 收集统计数据
	stats := struct {
		processed int // 被执行的个数
		ignored   int // 被忽略的个数
	}{}
	// 所有检验通过的header需要写入数据库
	for i, header := range chain {
		// 如果外部出现了中断，那么停止数据的继续操作
		if hc.procInterrupt() {
			hc.log.Debug("Premature abort during headers import")
			return i, errAborted
		}
		// 如果header已经在数据库中存在，那么忽略
		if hc.HasHeader(header.Hash(), header.Height) {
			stats.ignored++
			continue
		}
		// 使用回调进行单个写库
		if err := writeHeader(header); err != nil {
			return i, err
		}
		stats.processed++
	}
	// 打印日志
	last := chain[len(chain)-1]

	context := []interface{}{
		"count", stats.processed,
		"elapsed", dateutil.PrettyDuration(time.Since(start)),
		"number", last.Height,
		"hash", last.Hash(),
	}
	// 计算平均值
	if timestamp := time.Unix(int64(last.Timestamp), 0); time.Since(timestamp) > time.Minute {
		context = append(context, []interface{}{"age", dateutil.PrettyAge(timestamp)}...)
	}
	// 统计忽略个数
	if stats.ignored > 0 {
		context = append(context, []interface{}{"ignored", stats.ignored}...)
	}
	hc.log.Info("Imported new block headers", context...)

	return 0, nil
}

// GetBlockHashesFromHash 从指定hash开始获取一系列区块hash, 降序
func (hc *headerChain) GetBlockHashesFromHash(bHash types.Hash, max uint64) []types.Hash {
	// 根据hash获取区块头
	header := hc.GetHeaderByHash(bHash)
	if header == nil {
		return nil
	}
	// 迭代获取header，知道获得满足条目的header或者到达genesis
	chain := make([]types.Hash, 0, max)
	for i := uint64(0); i < max; i++ {
		next := header.ParentHash
		if header = hc.GetHeader(next, header.Height-1); header == nil {
			// header已经为nil，那么停止
			break
		}
		chain = append(chain, next)
		// 如果高度为0，那么也会停止
		if header.Height == 0 {
			break
		}
	}
	return chain
}

// GetAncestor 检索给定块的第n个祖先。它假定给定的块或其近祖先是规范的。
// MaxNoncononical指向一个向下的计数器，该计数器限制在我们到达规范链之前要单独检查的块的数量。
// 注意：祖先==0返回同一块，1返回其父块，依此类推。
// GetAncestor 检索给定块的第n个祖先
// @params hash 给定的块hash。
// @params number 给定的块。它假定给定的块或其近祖先是规范的。
// @params ancestor 第n个祖先
// @params maxNonCanonical 向下递减的计数器。该计数器限制在到达规范链之前要单独检查的块的数量
// @return types.Hash 祖先对应的hash
// @return uint64 祖先对应的区块高度
func (hc *headerChain) GetAncestor(hash types.Hash, number, ancestor uint64, maxNonCanonical *uint64) (types.Hash, uint64) {
	// 祖先高度大于起始高度，那么就直接返回0
	if ancestor > number {
		return types.EmptyHash, 0
	}
	if ancestor == 1 {
		// 如果祖先高度=1，那么startNumber=1，那么说明只产生了一个区块，
		// 那么直接可以读取区块1的header
		if header := hc.GetHeader(hash, number); header != nil {
			return header.ParentHash, number - 1
		} else {
			return types.EmptyHash, 0
		}
	}
	for ancestor != 0 {
		// 如果number对应的标准库的hash就等于查询时的hash，
		// 那么返回祖先的区块高度=number-ancestor
		// 祖先的hash=(number-ancestor)对应的hash
		if canonicalHash, err := hc.database.GetCanonicalHash(number); err != nil {
			hc.log.Warn("get canonical hash err", "err", err)
		} else if canonicalHash == hash {
			number -= ancestor
			canonicalHash1, _ := hc.database.GetCanonicalHash(number)
			return canonicalHash1, number
		}
		// 如果最大的非标准数据=0，那么返回0
		if *maxNonCanonical == 0 {
			return types.EmptyHash, 0
		}
		// 迭代次数减1
		*maxNonCanonical--
		// 祖先层级减1
		ancestor--
		// 通过hash及number获取到header
		header := hc.GetHeader(hash, number)
		if header == nil {
			return types.EmptyHash, 0
		}
		// 并将hash及number进行减1调整
		hash = header.ParentHash
		number--
	}
	return hash, number
}

// SetGenesis 设置创世header
func (hc *headerChain) SetGenesis(head *models.Header) {
	hc.genesisHeader = head
}

// SetHead 将本地链进行回滚到指定高度的位置
func (hc *headerChain) SetHead(desHeaderNumber uint64) {
	height := uint64(0)

	// 获取当前header
	if hdr := hc.CurrentHeader(); hdr != nil {
		height = hdr.Height
	}

	blockAbs := make([]models.BlockAbstract, 0)
	for hdr := hc.CurrentHeader(); hdr != nil && hdr.Height > desHeaderNumber; hdr = hc.CurrentHeader() {
		// 当前header不为空，并且其高度大于需要回滚的目标高度
		blockAbs = append(blockAbs, models.BlockAbstract{
			Hash:   hdr.Hash(),
			Height: hdr.Height,
		})
		// 设置内存的当前header
		hc.currentHeader.Store(hc.GetHeader(hdr.ParentHash, hdr.Height-1))
	}
	if err := hc.database.DeleteBlock(blockAbs, height, desHeaderNumber); err != nil {
		hc.log.Error("delete block err", "err", err)
	}

	// 进行回滚后，将lru进行清空
	hc.headerCache.Purge()
	hc.numberCache.Purge()

	// 如果最终currentHeader为空，那么就缓存为创世header
	if hc.CurrentHeader() == nil {
		hc.currentHeader.Store(hc.genesisHeader)
	}
	hc.currentHeaderHash = hc.CurrentHeader().Hash()
	// 数据库写入头部header的hash
	hc.database.WriteLatestHeaderHash(hc.currentHeaderHash)
}
