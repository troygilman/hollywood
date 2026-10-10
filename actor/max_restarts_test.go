package actor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// maxRestartTrigger is the user message that panics the actor under test.
type maxRestartTrigger struct{}

// maxRestartProbe is broadcast after Subscribe so the test can wait until the
// watcher is actually registered with the event stream.
type maxRestartProbe struct{}

// TestMaxRestartsExceededStopsActor guards the path where a panicked receive
// hits MaxRestarts and tryRestart calls cleanup(nil). A nil cancel must not
// run: that panic is on the inbox goroutine and nothing recovers it, so the
// whole process dies. WithMaxRestarts(0) stops on the first panic. The
// default limit stops on the panic after that many restarts.
func TestMaxRestartsExceededStopsActor(t *testing.T) {
	t.Run("WithMaxRestarts(0)", func(t *testing.T) {
		testMaxRestartsExceeded(t, true)
	})
	t.Run("default", func(t *testing.T) {
		testMaxRestartsExceeded(t, false)
	})
}

func testMaxRestartsExceeded(t *testing.T, zero bool) {
	t.Helper()

	e, err := NewEngine(NewEngineConfig())
	require.NoError(t, err)

	timeout := time.After(5 * time.Second)

	subscribed := make(chan struct{}, 1)
	exceeded := make(chan *PID, 1)
	stopped := make(chan *PID, 1)
	restarted := make(chan struct{}, defaultMaxRestarts+1)
	childReady := make(chan *PID, 1)
	childStopped := make(chan *PID, 1)

	var target atomic.Pointer[PID]

	watcher := e.SpawnFunc(func(c *Context) {
		switch msg := c.Message().(type) {
		case maxRestartProbe:
			subscribed <- struct{}{}
		case ActorMaxRestartsExceededEvent:
			if pid := target.Load(); pid != nil && msg.PID.Equals(pid) {
				exceeded <- msg.PID
			}
		case ActorStoppedEvent:
			if pid := target.Load(); pid != nil && msg.PID.Equals(pid) {
				stopped <- msg.PID
			}
		case ActorRestartedEvent:
			if pid := target.Load(); pid != nil && msg.PID.Equals(pid) {
				restarted <- struct{}{}
			}
		}
	}, "max-restart-watcher", WithID("watcher"))

	e.Subscribe(watcher)
	e.BroadcastEvent(maxRestartProbe{})
	waitRecv(t, timeout, subscribed, "event-stream subscription")

	var starts atomic.Int32
	opts := []OptFunc{
		WithID("worker"),
		WithRestartDelay(0),
	}
	if zero {
		opts = append(opts, WithMaxRestarts(0))
	}

	pid := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Started:
			// The first start spawns a child. Later starts are restarts and
			// panic so a single trigger walks the counter up to the limit.
			if starts.Add(1) == 1 {
				c.SpawnChildFunc(func(ctx *Context) {
					switch ctx.Message().(type) {
					case Started:
						childReady <- ctx.PID()
					case Stopped:
						childStopped <- ctx.PID()
					}
				}, "child", WithID("child"))
				return
			}
			panic("restarted")
		case maxRestartTrigger:
			panic("triggered")
		}
	}, "max-restart-actor", opts...)
	target.Store(pid)

	child := waitRecv(t, timeout, childReady, "child start")
	e.Send(pid, maxRestartTrigger{})

	gotExceeded := waitRecv(t, timeout, exceeded, "ActorMaxRestartsExceededEvent")
	gotStopped := waitRecv(t, timeout, stopped, "ActorStoppedEvent")
	gotChild := waitRecv(t, timeout, childStopped, "child ActorStopped")

	require.True(t, gotExceeded.Equals(pid))
	require.True(t, gotStopped.Equals(pid))
	require.True(t, gotChild.Equals(child))
	require.Nil(t, e.Registry.get(pid))
	require.Nil(t, e.Registry.get(child))
	require.Nil(t, e.Registry.GetPID("max-restart-actor", "worker"))

	if zero {
		require.Equal(t, int32(1), starts.Load())
		select {
		case <-restarted:
			t.Fatal("WithMaxRestarts(0) restarted the actor")
		default:
		}
	} else {
		// Initial start plus one Started panic per allowed restart.
		require.Equal(t, int32(defaultMaxRestarts)+1, starts.Load())
		for i := 0; i < defaultMaxRestarts; i++ {
			waitRecv(t, timeout, restarted, "ActorRestartedEvent")
		}
		select {
		case <-restarted:
			t.Fatal("restarted more times than MaxRestarts")
		default:
		}
	}

	e.Unsubscribe(watcher)
	<-e.Poison(watcher).Done()
}

func waitRecv[T any](t *testing.T, timeout <-chan time.Time, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-timeout:
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}
