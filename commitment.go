// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sort"
	"sync"
)

// Commitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with match(), and
// this notifies on commitCh when the commit index has advanced.
type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64
}

// newCommitment returns a commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
// 写磁盘后调用，
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	// 如果传入的 server 已投票, 另外 index 大于上一个记录的 index, 则更新 matchIndex.
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		// 重新计算
		c.recalculate()
	}
}

// Internal helper to calculate new commitIndex from matchIndexes.
// Must be called with lock held.
func (c *commitment) recalculate() {
	// 需要重计算, 但还未初始化 matchIndex 数据时, 直接退出.
	if len(c.matchIndexes) == 0 {
		return
	}

	// 初始化 一个容器存放各个 server 的 index.
	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	// 索引排序 （升序）
	sort.Sort(uint64Slice(matched))
	// 法定判断点位：大多数 [1,2,3, | 4,5]
	// [1,1,1,2,2]
	quorumMatchIndex := matched[(len(matched)-1)/2]
	// 如果法定判断位大于当前的提交点, 并且法定点大于 first index, 则更新 commitIndex 和通知 commitCh.
	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex // 更新提交点位
		asyncNotifyCh(c.commitCh)        // 通知提交信号，Leader 处理
	}
}
