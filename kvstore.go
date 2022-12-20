// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- []byte // channel for proposing updates
	db          *gorm.DB
	snapshotter *snap.Snapshotter
}

type Op string

const (
	Create Op = "create"
	Update Op = "update"
	Delete Op = "delete"
)

type Operation struct {
	Op     Op       `json:"op"`
	Target *Product `json:"target"`
}

type Model struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deletedAt"`
}

type Product struct {
	Model `json:",inline"`
	Code  string `json:"code"`
	Price uint   `json:"price"`
}

type Epoch struct {
	Model `json:",inline"`
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
}

func newKVStore(id int, snapshotter *snap.Snapshotter, proposeC chan<- []byte, commitC <-chan *commit, errorC <-chan error) *kvstore {
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("%d.db", id)), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// 迁移 schema
	if err = db.AutoMigrate(&Product{}); err != nil {
		panic("failed to create product table")
	}
	if err = db.AutoMigrate(&Epoch{}); err != nil {
		panic("failed to create epoch table")
	}

	s := &kvstore{proposeC: proposeC, db: db, snapshotter: snapshotter}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Metadata.Term, snapshot.Metadata.Index, snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(id uint) (*Product, bool) {
	var product Product
	err := s.db.First(&product, id).Error
	if err != nil {
		return nil, false
	}
	return &product, true
}

func (s *kvstore) Propose(op *Operation) {
	data, err := json.Marshal(op)
	if err != nil {
		return
	}
	s.proposeC <- data
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		v, _ := json.Marshal(commit.data)
		log.Println("committed, ", string(v))
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Metadata.Term, snapshot.Metadata.Index, snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		var epoch Epoch
		term, index := commit.data.term, commit.data.index

		s.db.First(&epoch)
		if term >= epoch.Term && index > epoch.Index {
			for _, operation := range commit.data.ops {
				switch operation.Op {
				case Create:
					s.db.Create(operation.Target)
				case Update:
					s.db.Updates(operation.Target)
				case Delete:
					if operation.Target.ID != 0 {
						s.db.Delete(&Product{}, operation.Target.ID)
					}
				}
			}
			epoch.Term, epoch.Index = term, index
			if epoch.ID == 0 {
				s.db.Create(&epoch)
			} else {
				s.db.Updates(&epoch)
			}
		}

		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	products := make([]*Product, 0)
	err := s.db.Find(&products).Error
	if err != nil {
		return nil, err
	}

	return json.Marshal(products)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(term, index uint64, snapshot []byte) error {
	products := make([]*Product, 0)
	if err := json.Unmarshal(snapshot, &products); err != nil {
		return err
	}

	s.db.Delete(&Product{})
	for _, p := range products {
		s.db.Create(p)
	}

	var epoch Epoch
	s.db.First(&epoch)
	epoch.Term, epoch.Index = term, index
	if epoch.ID == 0 {
		s.db.Create(&epoch)
	} else {
		s.db.Updates(&epoch)
	}

	return nil
}
