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
	"golang.org/x/sync/errgroup"
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

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("s3ds: filters or orders are not supported")
	}

	var startKey []byte
	if q.Prefix != "" {
		startKey = []byte(q.Prefix)
	}

	var opts []rawkv.ScanOption
	if q.KeysOnly && !q.ReturnsSizes {
		opts = append(opts, rawkv.ScanKeyOnly())
	}

	keys, values, err := d.raw.Scan(ctx, startKey, nil, q.Limit, opts...)
	if err != nil {
		return nil, err
	}

	var index int

	nextValue := func() (query.Result, bool) {
		if index >= len(keys) {
			return query.Result{}, false
		}
		ent := query.Entry{
			Key: string(keys[index]),
		}
		if !q.KeysOnly {
			ent.Value = values[index]
		}
		if q.ReturnsSizes {
			ent.Size = len(values[index])
		}
		if q.ReturnExpirations {
			exp, err := d.GetExpiration(ctx, datastore.NewKey(ent.Key))
			if err != nil {
				return query.Result{Error: err}, true
			}
			ent.Expiration = exp
		}
		return query.Result{Entry: ent}, true
	}

	return query.ResultsFromIterator(q, query.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
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
	grp, _ := errgroup.WithContext(context.TODO())
	grp.Go(func() error {
		return d.client.Close()
	})
	grp.Go(func() error {
		return d.raw.Close()
	})
	return grp.Wait()
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
