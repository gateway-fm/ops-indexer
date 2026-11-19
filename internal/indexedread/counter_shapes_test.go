package indexedread

import "testing"

func TestReaderKnownCounterShapes(t *testing.T) {
	f := indexFixtureVersion(t, false)
	legacy := f.open(t)
	legacy.Close()
	denied := func() {
		t.Helper()
		unexpected, e := OpenReader(t.Context(), f.dsn, f.binding, DefaultLimits())
		if unexpected != nil {
			unexpected.Close()
		}
		if e != ErrUnavailable {
			t.Fatal("unreviewed width/type/ACL shape accepted", e)
		}
	}
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN holder_count TYPE BIGINT`)
	denied()
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN holder_count TYPE INT`)
	// The exact locked producer migration, not a hand-created approximation.
	f.exec(t, currentCounterSQL)
	current := f.open(t)
	f.exec(t, `INSERT INTO public.tokens(address,symbol,block_number,holder_count,transfer_count) VALUES('counter-fixture','TEST',0,2147483648,2147483649)`)
	var holders, transfers string
	if e := current.withSnapshot(t.Context(), newBudget(current.limits), func(s *snapshot) error {
		return s.one(`SELECT holder_count::text,transfer_count::text FROM public.tokens WHERE address='counter-fixture'`, nil, &holders, &transfers)
	}); e != nil || holders != "2147483648" || transfers != "2147483649" {
		t.Fatal("BIGINT counter did not survive lossless text projection", holders, transfers, e)
	}
	f.exec(t, `ALTER TABLE public.address_stats ALTER COLUMN tx_count TYPE INT`)
	denied()
	f.exec(t, `ALTER TABLE public.address_stats ALTER COLUMN tx_count TYPE BIGINT`)
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN holder_count TYPE NUMERIC(78,0)`)
	denied()
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN holder_count TYPE BIGINT`)
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN decimals TYPE BIGINT`)
	denied()
	f.exec(t, `ALTER TABLE public.tokens ALTER COLUMN decimals TYPE INT`)
	f.exec(t, `GRANT SELECT(abi) ON public.contracts TO index_reader`)
	denied()
	f.exec(t, `REVOKE SELECT(abi) ON public.contracts FROM index_reader`)
	repaired := f.open(t)
	repaired.Close()
}
