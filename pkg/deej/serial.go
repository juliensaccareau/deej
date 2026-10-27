package deej

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jacobsa/go-serial/serial"
	"go.uber.org/zap"

	"github.com/omriharel/deej/pkg/deej/util"
)

// SerialIO provides a deej-aware abstraction layer to managing serial I/O
type SerialIO struct {
	comPort  string
	baudRate uint

	deej   *Deej
	logger *zap.SugaredLogger

	// guards running, stopChannel and doneChannel
	lock sync.Mutex

	// closed by Stop to ask the connection goroutine to shut down
	stopChannel chan struct{}

	// closed by the connection goroutine once it has fully shut down
	doneChannel chan struct{}

	// true from a successful Start until Stop. while running, a lost
	// connection is automatically re-established in the background
	running bool

	connOptions serial.OpenOptions

	lastKnownNumSliders        int
	currentSliderPercentValues []float32

	sliderMoveConsumers []chan SliderMoveEvent
}

// SliderMoveEvent represents a single slider move captured by deej
type SliderMoveEvent struct {
	SliderID     int
	PercentValue float32
}

var expectedLinePattern = regexp.MustCompile(`^\d{1,4}(\|\d{1,4})*\r\n$`)

const (
	// how long to wait between two reconnection attempts
	reconnectDelay = 2 * time.Second

	// how often the "no data received" watchdog is checked
	watchdogInterval = 500 * time.Millisecond
)

// NewSerialIO creates a SerialIO instance that uses the provided deej
// instance's connection info to establish communications with the arduino chip
func NewSerialIO(deej *Deej, logger *zap.SugaredLogger) (*SerialIO, error) {
	logger = logger.Named("serial")

	sio := &SerialIO{
		deej:                deej,
		logger:              logger,
		running:             false,
		sliderMoveConsumers: []chan SliderMoveEvent{},
	}

	logger.Debug("Created serial i/o instance")

	// respond to config changes
	sio.setupOnConfigReload()

	return sio, nil
}

// Start attempts to connect to our arduino chip. Once connected, the connection
// is monitored and automatically re-established if it gets lost (read error,
// USB hiccup, sleep/resume, or no data received for a while) until Stop is called
func (sio *SerialIO) Start() error {
	sio.lock.Lock()
	defer sio.lock.Unlock()

	// don't allow multiple concurrent connections
	if sio.running {
		sio.logger.Warn("Already connected, can't start another without closing first")
		return errors.New("serial: connection already active")
	}

	// set minimum read size according to platform (0 for windows, 1 for linux)
	// this prevents a rare bug on windows where serial reads get congested,
	// resulting in significant lag
	minimumReadSize := 0
	if util.Linux() {
		minimumReadSize = 1
	}

	sio.connOptions = serial.OpenOptions{
		PortName:        sio.deej.config.ConnectionInfo.COMPort,
		BaudRate:        uint(sio.deej.config.ConnectionInfo.BaudRate),
		DataBits:        8,
		StopBits:        1,
		MinimumReadSize: uint(minimumReadSize),
	}

	sio.logger.Debugw("Attempting serial connection",
		"comPort", sio.connOptions.PortName,
		"baudRate", sio.connOptions.BaudRate,
		"minReadSize", minimumReadSize)

	conn, err := serial.Open(sio.connOptions)
	if err != nil {

		// might need a user notification here, TBD
		sio.logger.Warnw("Failed to open serial connection", "error", err)
		return fmt.Errorf("open serial connection: %w", err)
	}

	namedLogger := sio.logger.Named(strings.ToLower(sio.connOptions.PortName))
	namedLogger.Infow("Connected", "conn", conn)

	sio.running = true
	sio.stopChannel = make(chan struct{})
	sio.doneChannel = make(chan struct{})

	go sio.maintainConnection(namedLogger, conn, sio.connOptions, sio.stopChannel, sio.doneChannel)

	return nil
}

// Stop signals us to shut down our serial connection, if one is active,
// and waits until it is fully closed
func (sio *SerialIO) Stop() {
	sio.lock.Lock()

	if !sio.running {
		sio.lock.Unlock()
		sio.logger.Debug("Not currently connected, nothing to stop")
		return
	}

	sio.logger.Debug("Shutting down serial connection")

	stopChannel := sio.stopChannel
	doneChannel := sio.doneChannel
	sio.running = false
	sio.lock.Unlock()

	close(stopChannel)
	<-doneChannel
}

// SubscribeToSliderMoveEvents returns an unbuffered channel that receives
// a sliderMoveEvent struct every time a slider moves
func (sio *SerialIO) SubscribeToSliderMoveEvents() chan SliderMoveEvent {
	ch := make(chan SliderMoveEvent)
	sio.sliderMoveConsumers = append(sio.sliderMoveConsumers, ch)

	return ch
}

func (sio *SerialIO) setupOnConfigReload() {
	configReloadedChannel := sio.deej.config.SubscribeToChanges()

	const stopDelay = 50 * time.Millisecond

	go func() {
		for {
			select {
			case <-configReloadedChannel:

				// make any config reload unset our slider number to ensure process volumes are being re-set
				// (the next read line will emit SliderMoveEvent instances for all sliders)\
				// this needs to happen after a small delay, because the session map will also re-acquire sessions
				// whenever the config file is reloaded, and we don't want it to receive these move events while the map
				// is still cleared. this is kind of ugly, but shouldn't cause any issues
				go func() {
					<-time.After(stopDelay)
					sio.lastKnownNumSliders = 0
				}()

				// if connection params have changed, attempt to stop and start the connection
				if sio.deej.config.ConnectionInfo.COMPort != sio.connOptions.PortName ||
					uint(sio.deej.config.ConnectionInfo.BaudRate) != sio.connOptions.BaudRate {

					sio.logger.Info("Detected change in connection parameters, attempting to renew connection")
					sio.Stop()

					// let the connection close
					<-time.After(stopDelay)

					if err := sio.Start(); err != nil {
						sio.logger.Warnw("Failed to renew connection after parameter change", "error", err)
					} else {
						sio.logger.Debug("Renewed connection successfully")
					}
				}
			}
		}
	}()
}

// maintainConnection reads from the given connection until it is lost, then keeps
// trying to reopen the same port until it succeeds, over and over, until stopped
func (sio *SerialIO) maintainConnection(
	logger *zap.SugaredLogger,
	conn io.ReadWriteCloser,
	options serial.OpenOptions,
	stopChannel <-chan struct{},
	doneChannel chan<- struct{},
) {
	defer close(doneChannel)

	for {
		stopped := sio.readUntilFailure(logger, conn, stopChannel)
		sio.closeConn(logger, conn)

		if stopped {
			return
		}

		logger.Warnw("Serial connection lost, will keep trying to reconnect",
			"retryEvery", reconnectDelay)

		conn = sio.reconnect(logger, options, stopChannel)
		if conn == nil {
			// stopped while reconnecting
			return
		}

		logger.Infow("Reconnected", "conn", conn)

		// force move events for all sliders on the next line, so that
		// volumes get re-synced with the physical slider positions
		sio.lastKnownNumSliders = 0
	}
}

// reconnect tries to reopen the serial port until it succeeds (returning the new
// connection) or until we're asked to stop (returning nil)
func (sio *SerialIO) reconnect(
	logger *zap.SugaredLogger,
	options serial.OpenOptions,
	stopChannel <-chan struct{},
) io.ReadWriteCloser {
	attempt := 0

	for {
		select {
		case <-stopChannel:
			return nil
		case <-time.After(reconnectDelay):
		}

		attempt++

		conn, err := serial.Open(options)
		if err == nil {
			return conn
		}

		// the port is typically missing while the device is unplugged / re-enumerating,
		// no need to spam warnings for each attempt
		if attempt == 1 || attempt%30 == 0 {
			logger.Infow("Reconnection attempt failed, still retrying", "attempt", attempt, "error", err)
		} else {
			logger.Debugw("Reconnection attempt failed", "attempt", attempt, "error", err)
		}
	}
}

// readUntilFailure reads and handles lines from the connection. It returns true if it
// stopped because we were asked to stop, or false if the connection appears to be lost
// (read error, or no data received for longer than the configured timeout)
func (sio *SerialIO) readUntilFailure(
	logger *zap.SugaredLogger,
	conn io.ReadWriteCloser,
	stopChannel <-chan struct{},
) bool {

	// closed when we return, so the reader goroutine never blocks forever on a send.
	// the caller closes the connection right after, which unblocks any pending read
	readerDone := make(chan struct{})
	defer close(readerDone)

	lineChannel, errChannel := sio.readLines(logger, conn, readerDone)

	readTimeout := time.Duration(sio.deej.config.ConnectionInfo.ReadTimeoutSeconds) * time.Second
	lastDataTime := time.Now()

	watchdog := time.NewTicker(watchdogInterval)
	defer watchdog.Stop()

	for {
		select {
		case <-stopChannel:
			return true

		case err := <-errChannel:
			logger.Warnw("Failed to read from serial", "error", err)
			return false

		case line := <-lineChannel:
			lastDataTime = time.Now()
			sio.handleLine(logger, line)

		case <-watchdog.C:
			if readTimeout > 0 && time.Since(lastDataTime) > readTimeout {
				logger.Warnw("No data received from serial for too long, considering connection dead",
					"timeout", readTimeout)
				return false
			}
		}
	}
}

func (sio *SerialIO) closeConn(logger *zap.SugaredLogger, conn io.ReadWriteCloser) {
	if err := conn.Close(); err != nil {
		logger.Warnw("Failed to close serial connection", "error", err)
	} else {
		logger.Debug("Serial connection closed")
	}
}

// readLines reads lines from the connection in the background. Lines are delivered on the
// first channel; the first read error (after which reading stops) on the second one
func (sio *SerialIO) readLines(
	logger *zap.SugaredLogger,
	conn io.Reader,
	done <-chan struct{},
) (<-chan string, <-chan error) {
	lineChannel := make(chan string)
	errChannel := make(chan error, 1)

	go func() {
		reader := bufio.NewReader(conn)

		for {
			line, err := reader.ReadString('\n')
			if err != nil {

				if sio.deej.Verbose() {
					logger.Debugw("Read loop ending", "error", err, "line", line)
				}

				// buffered, never blocks
				errChannel <- err
				return
			}

			if sio.deej.Verbose() {
				logger.Debugw("Read new line", "line", line)
			}

			// deliver the line, unless nobody is listening anymore
			select {
			case lineChannel <- line:
			case <-done:
				return
			}
		}
	}()

	return lineChannel, errChannel
}

func (sio *SerialIO) handleLine(logger *zap.SugaredLogger, line string) {

	// this function receives an unsanitized line which is guaranteed to end with LF,
	// but most lines will end with CRLF. it may also have garbage instead of
	// deej-formatted values, so we must check for that! just ignore bad ones
	if !expectedLinePattern.MatchString(line) {
		return
	}

	// trim the suffix
	line = strings.TrimSuffix(line, "\r\n")

	// split on pipe (|), this gives a slice of numerical strings between "0" and "1023"
	splitLine := strings.Split(line, "|")
	numSliders := len(splitLine)

	// update our slider count, if needed - this will send slider move events for all
	if numSliders != sio.lastKnownNumSliders {
		logger.Infow("Detected sliders", "amount", numSliders)
		sio.lastKnownNumSliders = numSliders
		sio.currentSliderPercentValues = make([]float32, numSliders)

		// reset everything to be an impossible value to force the slider move event later
		for idx := range sio.currentSliderPercentValues {
			sio.currentSliderPercentValues[idx] = -1.0
		}
	}

	// for each slider:
	moveEvents := []SliderMoveEvent{}
	for sliderIdx, stringValue := range splitLine {

		// convert string values to integers ("1023" -> 1023)
		number, _ := strconv.Atoi(stringValue)

		// turns out the first line could come out dirty sometimes (i.e. "4558|925|41|643|220")
		// so let's check the first number for correctness just in case
		if sliderIdx == 0 && number > 1023 {
			sio.logger.Debugw("Got malformed line from serial, ignoring", "line", line)
			return
		}

		// map the value from raw to a "dirty" float between 0 and 1 (e.g. 0.15451...)
		dirtyFloat := float32(number) / 1023.0

		// normalize it to an actual volume scalar between 0.0 and 1.0 with 2 points of precision
		normalizedScalar := util.NormalizeScalar(dirtyFloat)

		// if sliders are inverted, take the complement of 1.0
		if sio.deej.config.InvertSliders {
			normalizedScalar = 1 - normalizedScalar
		}

		// check if it changes the desired state (could just be a jumpy raw slider value)
		if util.SignificantlyDifferent(sio.currentSliderPercentValues[sliderIdx], normalizedScalar, sio.deej.config.NoiseReductionLevel) {

			// if it does, update the saved value and create a move event
			sio.currentSliderPercentValues[sliderIdx] = normalizedScalar

			moveEvents = append(moveEvents, SliderMoveEvent{
				SliderID:     sliderIdx,
				PercentValue: normalizedScalar,
			})

			if sio.deej.Verbose() {
				logger.Debugw("Slider moved", "event", moveEvents[len(moveEvents)-1])
			}
		}
	}

	// deliver move events if there are any, towards all potential consumers
	if len(moveEvents) > 0 {
		for _, consumer := range sio.sliderMoveConsumers {
			for _, moveEvent := range moveEvents {
				consumer <- moveEvent
			}
		}
	}
}
