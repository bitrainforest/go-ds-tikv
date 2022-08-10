package tikv

import (
	"context"
	"fmt"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	tikvErr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"time"
)

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)
var _ datastore.Batch = (*batch)(nil)
var _ datastore.TTL = (*Datastore)(nil)

func NewDatastore(tx *txnkv.Client, raw *rawkv.Client) (*Datastore, error) {
	return &Datastore{
		client: tx,
		raw:    raw,
	}, nil
}

type Datastore struct {
	client *txnkv.Client
	raw    *rawkv.Client
}

func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	tx, err := d.client.Begin()
	if err != nil {
		return nil, err
	}
	val, err := tx.Get(ctx, key.Bytes())
	if tikvErr.IsErrNotFound(err) {
		return nil, datastore.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	tx, err := d.client.Begin()
	if err != nil {
		return false, err
	}
	val, err := tx.Get(ctx, key.Bytes())
	if tikvErr.IsErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	tx, err := d.client.Begin()
	if err != nil {
		return 0, err
	}
	val, err := tx.Get(ctx, key.Bytes())
	if tikvErr.IsErrNotFound(err) {
		return 0, datastore.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return len(val), nil
}

func (d *Datastore) Query(_ context.Context, _ query.Query) (query.Results, error) {
	return nil, fmt.Errorf("not implemented")
}

func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	tx, err := d.client.Begin()
	if err != nil {
		return err
	}
	if err := tx.Set(key.Bytes(), value); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	tx, err := d.client.Begin()
	if err != nil {
		return err
	}
	if err := tx.Delete(key.Bytes()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *Datastore) Sync(_ context.Context, _ datastore.Key) error {
	return nil
}

func (d *Datastore) Close() error {
	return d.client.Close()
}

func (d *Datastore) Batch(_ context.Context) (datastore.Batch, error) {
	tx, err := d.client.Begin()
	if err != nil {
		return nil, err
	}
	return &batch{tx: tx}, nil
}

func (d *Datastore) PutWithTTL(ctx context.Context, key datastore.Key, value []byte, ttl time.Duration) error {
	return d.raw.PutWithTTL(ctx, key.Bytes(), value, uint64(ttl))
}

func (d *Datastore) SetTTL(ctx context.Context, key datastore.Key, ttl time.Duration) error {
	value, err := d.Get(ctx, key)
	if err != nil {
		return err
	}
	return d.PutWithTTL(ctx, key, value, ttl)
}

func (d *Datastore) GetExpiration(ctx context.Context, key datastore.Key) (time.Time, error) {
	t, err := d.raw.GetKeyTTL(ctx, key.Bytes())
	switch err {
	case datastore.ErrNotFound:
		return time.Time{}, nil
	case nil:
		return time.Now().Add(time.Duration(int64(*t))), nil
	default:
		return time.Time{}, err
	}
}

type batch struct {
	tx *transaction.KVTxn
}

func (b *batch) Put(_ context.Context, key datastore.Key, value []byte) error {
	return b.tx.Set(key.Bytes(), value)
}

func (b *batch) Delete(_ context.Context, key datastore.Key) error {
	return b.tx.Delete(key.Bytes())
}

func (b *batch) Commit(ctx context.Context) error {
	return b.tx.Commit(ctx)
}

func (b *batch) Cancel() error {
	return b.tx.Rollback()
}
