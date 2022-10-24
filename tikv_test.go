package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
	"testing"
	"time"
)

var (
	bg      = context.Background()
	pdAddrs = []string{"127.0.0.1:2379"}
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
	//"/g":     "",
}

func newDS(t *testing.T) *Datastore {
	txn, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := rawkv.NewClient(bg, pdAddrs, config.Security{})
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDatastore(txn, raw)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		t.Logf("set kv: %s: %s", k, v)
		dsk := ds.NewKey(k)
		if err := d.Put(bg, dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(bg, dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}
}

func TestHas(t *testing.T) {
	d := newDS(t)
	defer d.Close()
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if !has {
		t.Error("Key should be found")
	}

	has, err = d.Has(bg, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}
}

func TestGetSize(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck
	addTestCases(t, d, testcases)

	size, err := d.GetSize(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if size != len(testcases["/a/b/c"]) {
		t.Error("")
	}

	_, err = d.GetSize(bg, ds.NewKey("/a/b/c/d"))
	if err != ds.ErrNotFound {
		t.Error(err)
	}
}

func TestNotExistGet(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}

	val, err := d.Get(bg, ds.NewKey("/a/b/c/d"))
	if val != nil {
		t.Error("Key should not be found")
	}

	if err != ds.ErrNotFound {
		t.Error("Error was not set to ds.ErrNotFound")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestDelete(t *testing.T) {
	d := newDS(t)
	defer d.Close()
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Error("Key should be found")
	}

	err = d.Delete(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	has, err = d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if has {
		t.Error("Key should not be found")
	}
}

func TestGetEmpty(t *testing.T) {
	d := newDS(t)
	defer d.Close()

	err := d.Put(bg, ds.NewKey("/a"), []byte{})
	if err != nil {
		t.Error(err)
	}

	v, err := d.Get(bg, ds.NewKey("/a"))
	if err != nil {
		t.Error(err)
	}

	if len(v) != 0 {
		t.Error("expected 0 len []byte form get")
	}
}

func TestBatching(t *testing.T) {
	d := newDS(t)
	defer d.Close()

	b, err := d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(bg, ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(bg, ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}

	//Test delete

	b, err = d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(bg, ds.NewKey("/a/b"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	//Test cancel

	b, err = d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	const key = "/xyz"

	err = b.Put(bg, ds.NewKey(key), []byte("/x/y/z"))
	if err != nil {
		t.Fatal(err)
	}

	// TODO: remove type assertion once datastore.Batch interface has Cancel
	err = b.(*batch).Cancel()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.Get(bg, ds.NewKey(key))
	if err == nil {
		t.Fatal("expected error trying to get uncommited data")
	}
}

func TestBasicPutGet(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck

	k := ds.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := d.Put(bg, k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := d.Get(bg, k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = d.Delete(bg, k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
}

func TestNotFounds(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck

	badk := ds.NewKey("notreal")

	val, err := d.Get(bg, badk)
	if err != ds.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := d.Has(bg, badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}
}

func TestTxnRollback(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck

	txn, err := d.client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	key := ds.NewKey("/test/thing")
	if err := txn.Set(key.Bytes(), []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	txn.Rollback() // nolint: errcheck
	has, err := d.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("key written in aborted transaction still exists")
	}
}

func TestTxnCommit(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck

	txn, err := d.client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	key := ds.NewKey("/test/thing")
	if err := txn.Set(key.Bytes(), []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	err = txn.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}
	has, err := d.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("key written in committed transaction does not exist")
	}
}

func TestTxnBatch(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck
	txn, err := d.client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	data := make(map[ds.Key][]byte)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("/test/%d", i))
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		data[key] = bytes

		err = txn.Set(key.Bytes(), bytes)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = txn.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	for key, value := range data {
		retrieved, err := d.Get(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != len(value) {
			t.Fatal("bytes stored different length from bytes generated")
		}
		for i, b := range retrieved {
			if value[i] != b {
				t.Fatal("bytes stored different content from bytes generated")
			}
		}
	}
}

func TestTTL(t *testing.T) {
	d := newDS(t)
	defer d.Close() // nolint: errcheck

	data := make(map[ds.Key][]byte)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("/test/%d", i))
		value := make([]byte, 16)
		_, err := rand.Read(value)
		if err != nil {
			t.Fatal(err)
		}
		data[key] = value
	}

	// write data
	for key, value := range data {
		err := d.PutWithTTL(bg, key, value, time.Second)
		if err != nil {
			t.Fatalf("PutWithTTL: %s", err)
		}
	}

	txn, err := d.client.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for key := range data {
		_, err := txn.Get(bg, key.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}
	txn.Rollback()

	time.Sleep(time.Second)

	for key := range data {
		has, err := d.Has(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Fatal("record with ttl did not expire")
		}
	}

}

func TestExpirations(t *testing.T) {
	var err error

	d := newDS(t)
	defer d.Close() // nolint: errcheck

	key := ds.NewKey("/abc/def")
	val := make([]byte, 32)
	if n, err := rand.Read(val); n != 32 || err != nil {
		t.Fatal("source of randomness failed")
	}

	ttl := time.Hour
	now := time.Now()
	tgt := now.Add(ttl)

	if err = d.PutWithTTL(bg, key, val, ttl); err != nil {
		t.Fatalf("adding with ttl failed: %v", err)
	}

	// GetExpiration returns expected value.
	var dsExp time.Time
	if dsExp, err = d.GetExpiration(bg, key); err != nil {
		t.Fatalf("getting expiration failed: %v", err)
	} else if tgt.Sub(dsExp) >= 5*time.Second {
		t.Fatal("expiration returned by datastore not within the expected range (tolerance: 5 seconds)")
	} else if tgt.Sub(dsExp) < 0 {
		t.Fatal("expiration returned by datastore was earlier than expected")
	}

	// Datastore->GetExpiration()
	if exp, err := d.GetExpiration(bg, key); err != nil {
		t.Fatalf("querying datastore failed: %v", err)
	} else if exp != dsExp {
		t.Fatalf("expiration returned from DB differs from that returned by txn, expected: %v, actual: %v", dsExp, exp)
	}

	if _, err := d.GetExpiration(bg, ds.NewKey("/foo/bar")); err != ds.ErrNotFound {
		t.Fatalf("wrong error type: %v", err)
	}
}

func TestSuite(t *testing.T) {
	d := newDS(t)
	defer d.Close()
	dstest.SubtestAll(t, d)
}
