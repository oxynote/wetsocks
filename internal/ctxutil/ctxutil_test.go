package ctxutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_IsInterrupted(t *testing.T) {
	assert.True(t, IsInterrupted(context.Canceled))
	assert.True(t, IsInterrupted(context.DeadlineExceeded))
	assert.False(t, IsInterrupted(assert.AnError))
}

func Test_MultiContext(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	mctx, mcancel := MultiContext(ctx1, ctx2)

	assert.NotNil(t, mctx.(*multiCtx).done)
	assert.Same(t, ctx1, mctx.(*multiCtx).ctxs[0])
	assert.Same(t, ctx2, mctx.(*multiCtx).ctxs[1])

	mcancel()

	<-mctx.(*multiCtx).done

	assert.Equal(t, context.Canceled, mctx.(*multiCtx).err)
}

func Test_multiCtx_Deadline(t *testing.T) {
	var mctx multiCtx

	// empty
	d, ok := mctx.Deadline()

	assert.Zero(t, d)
	assert.False(t, ok)

	// single ctx
	rootCtx := context.Background()

	ctx1, cancel1 := context.WithTimeout(rootCtx, time.Hour)
	defer cancel1()

	mctx.ctxs = append(mctx.ctxs, ctx1)

	d, ok = mctx.Deadline()

	assert.NotZero(t, d)
	assert.WithinDuration(t, time.Now(), d, time.Hour)
	assert.True(t, ok)

	// multi ctx
	ctx2, cancel2 := context.WithTimeout(rootCtx, time.Hour*5)
	defer cancel2()

	ctx3, cancel3 := context.WithTimeout(rootCtx, time.Minute*3)
	defer cancel3()

	mctx.ctxs = append(mctx.ctxs, ctx2, ctx3, rootCtx)

	d, ok = mctx.Deadline()

	assert.NotZero(t, d)
	assert.WithinDuration(t, time.Now(), d, time.Minute*3)
	assert.True(t, ok)
}

func Test_multiCtx_Done(t *testing.T) {
	mctx := multiCtx{done: make(chan struct{})}
	assert.NotNil(t, mctx.Done())
}

func Test_multiCtx_Err(t *testing.T) {
	mctx := multiCtx{err: assert.AnError}
	assert.Equal(t, assert.AnError, mctx.Err())
}

func Test_multiCtx_Value(t *testing.T) {
	rootCtx := context.Background()
	mctx := multiCtx{
		ctxs: []context.Context{
			context.WithValue(rootCtx, "hello", "test1"), //nolint:revive,staticcheck // used for testing
			context.WithValue(rootCtx, "hello", "300"),   //nolint:revive,staticcheck // used for testing
			context.WithValue(rootCtx, "hey", "123"),     //nolint:revive,staticcheck // used for testing
		},
	}

	assert.Equal(t, "test1", mctx.Value("hello"))
	assert.Equal(t, "123", mctx.Value("hey"))
	assert.Nil(t, mctx.Value("hey123"))
}

func Test_multiCtx_cancel(t *testing.T) {
	mctx := multiCtx{done: make(chan struct{})}

	mctx.cancel(context.Canceled)
	assert.Equal(t, context.Canceled, mctx.err)

	_, ok := <-mctx.done
	assert.False(t, ok)

	mctx.cancel(assert.AnError)
	assert.Equal(t, context.Canceled, mctx.err)

	_, ok = <-mctx.done
	assert.False(t, ok)
}

func Test_multiCtx_run(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	mctx := multiCtx{
		done: make(chan struct{}),
		ctxs: []context.Context{
			context.Background(),
			ctx1,
			ctx2,
		},
	}

	mctx.run()
	cancel1()

	<-mctx.done

	assert.Equal(t, context.Canceled, mctx.err)
}
