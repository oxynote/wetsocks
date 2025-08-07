package wsutil

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"syscall"
	"testing"

	"github.com/coder/websocket"
	"github.com/lucidence/purse/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func Test_LogError(t *testing.T) {
	cc := map[string]struct {
		Critical bool
		Err      error
		Message  string
	}{
		"Normal closure": {
			Err: websocket.CloseError{
				Code: websocket.StatusNormalClosure,
			},
		},
		"Context cancellation": {
			Err: context.Canceled,
		},
		"Status too big": {
			Err: websocket.CloseError{
				Code: websocket.StatusMessageTooBig,
			},
			Message: `"msg":"websocket error"`,
		},
		"Status too big and critical": {
			Err: websocket.CloseError{
				Code: websocket.StatusMessageTooBig,
			},
			Critical: true,
			Message:  `"msg":"websocket payload too big"`,
		},
		"Other error": {
			Err:     assert.AnError,
			Message: `"msg":"websocket error"`,
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			var b bytes.Buffer
			out := bufio.NewWriter(&b)
			log := slog.New(slog.NewJSONHandler(out, nil))

			LogError(log, c.Err, c.Critical)
			require.NoError(t, out.Flush())
			assert.Regexp(t, regexp.MustCompile(c.Message), b.String())
		})
	}
}

func Test_IsStatus(t *testing.T) {
	// error is not websocket error
	err := errors.New("err")
	assert.False(t, IsStatus(err, websocket.StatusBadGateway))

	// error is not present in codes
	err = websocket.CloseError{
		Code: websocket.StatusAbnormalClosure,
	}

	assert.False(t, IsStatus(err, websocket.StatusBadGateway))

	// successful execution
	assert.True(t, IsStatus(err, websocket.StatusAbnormalClosure))
}

func Test_IsClosure(t *testing.T) {
	assert.False(t, IsClosure(assert.AnError))
	assert.True(t, IsClosure(websocket.CloseError{
		Code: websocket.StatusBadGateway,
	}))
	assert.False(t, IsClosure(websocket.CloseError{
		Code: websocket.StatusBadGateway,
	}, websocket.StatusBadGateway))
	assert.True(t, IsClosure(context.Canceled))
	assert.True(t, IsClosure(syscall.ECONNRESET))
	assert.True(t, IsClosure(io.EOF))
}

func Test_Topic_String(t *testing.T) {
	tpc := Topic{
		Operation: "update",
		Path:      []string{"abc", "123", "qwe"},
	}
	assert.Equal(t, "update@abc.123.qwe", tpc.String())

	tpc.Descriptor = "sub"
	assert.Equal(t, "sub~update@abc.123.qwe", tpc.String())
}

func Test_ParseTopic(t *testing.T) {
	cc := map[string]struct {
		Input  string
		Result Topic
		Err    error
	}{
		"No @": {
			Input: "updatehello.test",
			Err:   assert.AnError,
		},
		"Two dots": {
			Input: "update@hello..test",
			Err:   assert.AnError,
		},
		"Last symbol is a dot": {
			Input: "update@hello.test.",
			Err:   assert.AnError,
		},
		"Non-alphanumeric descriptor": {
			Input: "!@sub~update@hel#lo.test.",
			Err:   assert.AnError,
		},
		"Non-alphanumeric path": {
			Input: "update@hel#lo.test.",
			Err:   assert.AnError,
		},
		"Non-alphanumeric operation": {
			Input: "sub~up@date@hel#lo.test.",
			Err:   assert.AnError,
		},
		"Two @": {
			Input: "update@hello@test",
			Err:   assert.AnError,
		},
		"Two ~": {
			Input: "sub~update@he~llo.test",
			Err:   assert.AnError,
		},
		"Successful parsing without descriptor": {
			Input: "update@hello.test",
			Result: Topic{
				Operation: "update",
				Path:      []string{"hello", "test"},
			},
		},
		"Successful execution with descriptor": {
			Input: "sub~update@hello_world.test.{symbol}.{interval}",
			Result: Topic{
				Descriptor: "sub",
				Operation:  "update",
				Path:       []string{"hello_world", "test", "{symbol}", "{interval}"},
			},
		},
		"Successful execution with descriptor and a single path element": {
			Input: "sub~update@hellotest",
			Result: Topic{
				Descriptor: "sub",
				Operation:  "update",
				Path:       []string{"hellotest"},
			},
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			res, err := ParseTopic(c.Input)
			testutil.AssertEqualError(t, c.Err, err)
			assert.Equal(t, c.Result, res)
		})
	}
}
