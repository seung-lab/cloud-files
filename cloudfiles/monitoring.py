from typing import Optional

import enum
import queue
import uuid
import time
import threading

import intervaltree
import numpy as np
import numpy.typing as npt

class IOEnum(enum.Enum):
  RX = 1
  TX = 2

class TransmissionMonitor:
  """Monitors the current transmissing rate of a file set."""
  def __init__(self, direction:IOEnum):
    self._intervaltree = intervaltree.IntervalTree()
    self._lock = threading.Lock()
    self._total_bytes_landed = 0
    self._in_flight = {}
    self._errors = set()
    
    # NOTE: _in_flight_bytes doesn't work for downloads b/c we are not
    # requesting the size of the file up front to avoid perf impact.
    # _in_flight_bytes isn't necessary unless we are modeling the contribution
    # of CloudFiles to machine network usage to implement throttling.
    self._in_flight_bytes = 0 
    self._direction = direction

  @classmethod
  def merge(klass, tms:list["TransmissionMonitor"]) -> "TransmissionMonitor":
    if len(tms) == 0:
      return TransmissionMonitor(IOEnum.TX)

    tm = TransmissionMonitor(tms[0]._direction)

    with tm._lock:
      for other in tms:
        with other._lock:
          tm._intervaltree = tm._intervaltree.union(other._intervaltree)

    return tm

  def start_io(self, num_bytes:int, start_time:Optional[float] = None) -> uuid.UUID:
    flight_id = uuid.uuid1()
    with self._lock:
      if start_time is None:
        start_time = time.monotonic()
      self._in_flight[flight_id] = start_time
      self._in_flight_bytes += num_bytes
    return flight_id

  def end_error(self, flight_id:uuid.UUID) -> None:
    with self._lock:
      self._errors.add(flight_id)

  def end_io(self, flight_id:uuid.UUID, num_bytes:int) -> None:
    """Add a new value to the interval set."""
    end_us = int(time.monotonic() * 1e6)
    
    with self._lock:
      start_us = int(self._in_flight.pop(flight_id) * 1e6)
      self._in_flight_bytes -= num_bytes
      self._intervaltree.addi(start_us, end_us, [flight_id, num_bytes])
      self._total_bytes_landed += num_bytes

  def total_bps(self) -> float:
    """Average bits per second sent during the entire session."""
    with self._lock:
      begin = self._intervaltree.begin()
      end = self._intervaltree.end()
    return self._total_bytes_landed / ((end - begin) / 1e6) * 8

  def total_bytes(self) -> int:
    """Sum of all bytes sent."""
    num_bytes = 0
    with self._lock:
      for interval in self._intervaltree:
        num_bytes += interval.data[1]
    return num_bytes

  def current_bps(self, look_back_sec:float = 2.0) -> float:
    """
    Compute the current bits per a second with a lookback
    value given in microseconds.
    """
    look_back_us = int(look_back_sec * 1e6)

    with self._lock:
      now_us = int(time.monotonic() * 1e6)
      query_us = now_us - look_back_us
      lookback_intervals = self._intervaltree[query_us:]
      begin_us = self._intervaltree.begin()

    num_bytes = 0
    for interval in lookback_intervals:
      if interval.begin > query_us:
        num_bytes += interval.data[1]
      else:
        adjustment_factor = (interval.end - query_us) / (interval.end - interval.begin)
        num_bytes += int(round(interval.data[1] * adjustment_factor))

    window_us = min(look_back_us, now_us - begin_us)

    return float(num_bytes) / (window_us / 1e6) * 8

  def total_Mbps(self, *args, **kwargs) -> float:
    """The total rate in megabits per a second over all files this run."""
    return self.total_bps(*args, **kwargs) / 1e6

  def total_Gbps(self, *args, **kwargs) -> float:
    """The total rate in gigabits per a second over all files this run."""
    return self.total_bps(*args, **kwargs) / 1e9

  def total_MBps(self, *args, **kwargs) -> float:
    """The total rate in megabytes per a second over all files this run."""
    return self.total_Mbps(*args, **kwargs) / 8.0

  def total_GBps(self, *args, **kwargs) -> float:
    """The total rate in gigabytes per a second over all files this run."""
    return self.total_Gbps(*args, **kwargs) / 8.0

  def current_Mbps(self, *args, **kwargs) -> float:
    """The current rate in megabits per a second."""
    return self.current_bps(*args, **kwargs) / 1e6

  def current_Gbps(self, *args, **kwargs) -> float:
    """The current rate in gigabits per a second."""
    return self.current_bps(*args, **kwargs) / 1e9

  def current_MBps(self, *args, **kwargs) -> float:
    """The current rate in megabytes per a second."""
    return self.current_Mbps(*args, **kwargs) / 8.0

  def current_GBps(self, *args, **kwargs) -> float:
    """The current rate in gigabytes per a second."""
    return self.current_Gbps(*args, **kwargs) / 8.0

  def start_unix_time(self):
    with self._lock:
      return self._intervaltree.begin() / 1e6

  def end_unix_time(self):
    with self._lock:
      return self._intervaltree.end() / 1e6

  def peak_bps(self) -> float:
    return np.max(self.histogram(resolution=1.0)) * 8

  def histogram(self, resolution:float = 1.0) -> npt.NDArray[np.uint32]:

    if resolution <= 0:
      raise ValueError(f"Resolution must be positive. Got: {resolution}")

    if not self._intervaltree:
      return np.array([], dtype=np.uint32)

    with self._lock:
      all_begin = int(np.floor(self._intervaltree.begin() / 1e6))
      all_end = int(np.ceil(self._intervaltree.end() / 1e6))

      num_bins = int(np.ceil((all_end - all_begin) / resolution))
      bins = np.zeros([ num_bins ], dtype=np.float64)

      for interval in self._intervaltree:
        begin = interval.begin / 1e6
        end = interval.end / 1e6
        duration = end - begin
        total_bytes = interval.data[1]
        
        first_bin = int((begin - all_begin) / resolution)
        last_bin = int((end - all_begin) / resolution)

        if first_bin == last_bin:
          bins[first_bin] += total_bytes
        else:
          bin_start = all_begin + first_bin * resolution
          bin_end = all_begin + last_bin * resolution

          first_bin_coverage = (bin_start + resolution) - begin
          bins[first_bin] += total_bytes * (first_bin_coverage / duration)

          last_bin_coverage = end - bin_end
          bins[last_bin] += total_bytes * (last_bin_coverage / duration)

          full_bins_count = last_bin - first_bin - 1
          if full_bins_count > 0:
              per_bin_bytes = total_bytes * (resolution / duration)
              bins[first_bin+1:last_bin] += per_bin_bytes

    return bins.round().astype(np.uint32)

  def plot_histogram(self, resolution:float = 1.0, filename:Optional[str] = None) -> None:
    """
    Plot a bar chart showing the number of bytes transmitted
    per a unit time. Resolution is specified in seconds.
    """
    import matplotlib.pyplot as plt

    xfer = self.histogram(resolution) * 8
    xfer = xfer.astype(np.float32)
    peak = np.max(xfer)

    if peak < 1000:
      ylabel = 'bps'
      factor = 1.0
    elif 1000 <= peak < int(1e6):
      ylabel = 'Kbps'
      factor = 1000.0
    elif int(1e6) <= peak < int(1e9):
      ylabel = 'Mbps'
      factor = 1e6
    else:
      ylabel = "Gbps"
      factor = 1e9

    xfer /= factor

    plt.figure(figsize=(10, 6))

    x_values = np.arange(len(xfer)) * resolution
    shade_alpha = 0.4

    plt.plot(
        x_values,
        xfer,
        color='dodgerblue',
        linestyle='-',
        linewidth=1.5,
        alpha=0.8,
        marker='',  # Remove markers for cleaner look
    )
    plt.fill_between(
      x_values, 0, xfer, 
      color='dodgerblue', alpha=shade_alpha
    )

    direction_text = "Download"
    if self._direction == IOEnum.TX:
      direction_text = "Upload"

    plt.title(f'Estimated Data {direction_text} Rate')
    plt.xlabel('Time (seconds)')
    plt.ylabel(ylabel)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    if filename is not None:
      plt.savefig(filename)
    else:
      plt.show()

    plt.gca().clear()
    plt.close('all')

  def plot_gantt(
    self, 
    filename:Optional[str] = None,
    title:Optional[str] = None,
    show_size_labels:Optional[bool] = None,
  ):
    import matplotlib.pyplot as plt
    import matplotlib.colors as colors
    from matplotlib.cm import ScalarMappable

    start_time = self.start_unix_time()

    file_sizes = []
    with self._lock:
      for interval in self._intervaltree:
        file_sizes.append(interval.data[1])

    if show_size_labels is None:
      show_size_labels = len(file_sizes) < 40

    if len(file_sizes):
      min_file_size = min(file_sizes)
      max_file_size = max(file_sizes)
    else:
      min_file_size = 0
      max_file_size = 0

    del file_sizes

    fig, ax = plt.subplots(figsize=(10, 5))

    if max_file_size == min_file_size:
      norm = colors.Normalize(vmin=0, vmax=max_file_size*1.1)
    else:
      norm = colors.Normalize(vmin=min_file_size, vmax=max_file_size)
    cmap = plt.cm.viridis

    def human_readable_bytes(x:int) -> str:
      factor = (1, 'B')
      if x < 1000:
        return f"{x} B"
      elif 1000 <= x < int(1e6):
        factor = (1000, 'kB')
      elif int(1e6) <= x < int(1e9):
        factor = (1e6, 'MB')
      elif int(1e9) <= x < int(1e12):
        factor = (1e9, 'GB')
      elif int(1e12) <= x < int(1e15):
        factor = (1e12, 'TB')
      else:
        factor = (1e15, 'EB')

      return f"{x/factor[0]:.2f} {factor[1]}"

    with self._lock:
      for i, interval in enumerate(self._intervaltree):
          duration = (interval.end - interval.begin) / 1e6
          left = (interval.begin / 1e6) - start_time
          flight_id = interval.data[0]

          if flight_id in self._errors:
            color = "red"
          else:
            cval = norm(interval.data[1])
            color = cmap(cval)

          ax.barh(
            str(i), 
            width=duration,
            left=left,
            height=1,
            color=color,
          )
          if show_size_labels:
            ax.text(
                x=left + (duration/2),
                y=i,            
                s=human_readable_bytes(int(interval.data[1])),
                ha='center',     
                va='center',
                color='black' if cval > 0.5 else '0.8',
                fontsize=8,
            )

    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])  # Required for ScalarMappable with empty data
    cbar = plt.colorbar(sm, ax=ax, label='File Size (bytes)')
    
    direction_text = "Download"
    if self._direction == IOEnum.TX:
      direction_text = "Upload"

    if title is None:
      title = f"File {direction_text} Recording"

    plt.xlabel("Time (seconds)")
    plt.ylabel("Files in Flight")
    ax.set_yticks([])
    plt.title(title)
    plt.tight_layout()

    if filename is not None:
      plt.savefig(filename)
    else:
      plt.show()

    plt.gca().clear()
    plt.close('all')

  def __getstate__(self):
    # Copy the object's state from self.__dict__ which contains
    # all our instance attributes. Always use the dict.copy()
    # method to avoid modifying the original state.
    state = self.__dict__.copy()
    # Remove the unpicklable entries.
    del state['_lock']
    return state

  def __setstate__(self, state):
    # Restore instance attributes (i.e., filename and lineno).
    self.__dict__.update(state)
    self._lock = threading.Lock()

class IOSampler:
  def __init__(
    self, 
    buffer_sec:float = 600.0, 
    interval:float = 0.25
  ):
    if buffer_sec <= 0 or interval <= 0:
      raise ValueError(
        f"Buffer and interval must be positive. buffer sec: {buffer_sec}, interval: {interval}"
      )

    self._terminate = threading.Event()
    self._thread = None
    self._interval = interval
    self._buffer_sec = buffer_sec

    self._sample_lock = threading.Lock()
    self._init_sample_buffers()

  def peak_bps(self, window:float = 1.0) -> tuple[float,float]:
    """Returns the peak rate over the look back window given in seconds. 

    Returns (download, upload) in bits per second (bps).
    """
    N = self.num_samples()
    if N <= 1:
      return (0.0, 0.0)

    rx, tx, ts = self.samples()
    
    def measure_peak(data):
      peak_rate = 0.0
      for i in range(len(ts) - 1):
        for j in range(i + 1, len(ts)):
          elapsed = (ts[j] - ts[i])
          if elapsed >= window:
            rate = (data[j] - data[i]) / elapsed * 8
            peak_rate = max(peak_rate, rate)
            break

        if (ts[-1] - ts[i]) > 0:
          rate = (data[-1] - data[i]) / (ts[-1] - ts[i]) * 8
          peak_rate = max(peak_rate, rate)

      return peak_rate

    return (measure_peak(rx), measure_peak(tx))

  def current_bps(self, look_back_sec:float = 2.0) -> tuple[float,float]:
    N = self.num_samples()
    if N <= 1:
      return (0.0, 0.0)

    rx, tx, ts = self.samples()
    i = ts.size - 2
    elapsed = 0
    t = ts[-1]
    while (i >= 0) and not (elapsed >= look_back_sec):
      elapsed = t - ts[i]
      i -= 1
    
    i += 1

    if elapsed < 1e-4:
      return (0.0, 0.0)

    compute_rate = lambda data: (data[-1] - data[i]) / elapsed * 8
    return (compute_rate(rx), compute_rate(tx))

  def _histogram(self, resolution, bs, ts) -> npt.NDArray[np.uint32]:
    bs = bs[1:] - bs[:-1]
    num_bins = int(np.ceil((ts[-1] - ts[0]) / resolution))
    bins = np.zeros([ num_bins ], dtype=np.uint32)

    for i in range(bs.size):
      j = int((ts[i] - ts[0]) / resolution)
      bins[j] += bs[i]

    return bins

  def histogram_rx(self, resolution:float = 1.0) -> npt.NDArray[np.uint32]:
    N = self.num_samples()
    if N <= 1:
      return np.array([], dtype=np.uint32)

    rx, tx, ts = self.samples()
    return self._histogram(resolution, rx, ts)

  def histogram_tx(self, resolution:float = 1.0) -> npt.NDArray[np.uint32]:
    N = self.num_samples()
    if N <= 1:
      return np.array([], dtype=np.uint32)

    rx, tx, ts = self.samples()
    return self._histogram(resolution, tx, ts)

  def plot_histogram(self, resolution:float = None, filename:Optional[str] = None) -> None:
    """
    Plot a bar chart showing the number of bytes transmitted
    per a unit time. Resolution is specified in seconds.
    """
    import matplotlib.pyplot as plt

    if resolution is None:
      resolution = 1.0
    elif resolution < self._interval:
      raise ValueError(
        f"Can't create histogram bins at a higher resolution "
        f"than the sample rate. Got: {resolution} Sample Rate: {self._interval}"
      )

    download_bps = self.histogram_rx(resolution) * 8
    upload_bps = self.histogram_tx(resolution) * 8

    download_bps = download_bps.astype(np.float32)
    upload_bps = upload_bps.astype(np.float32)

    peak_down = np.max(download_bps)
    peak_up = np.max(upload_bps)
    peak = max(peak_up, peak_down)

    if peak < 1000:
      ylabel = 'bps'
      factor = 1.0
    elif 1000 <= peak < int(1e6):
      ylabel = 'Kbps'
      factor = 1000.0
    elif int(1e6) <= peak < int(1e9):
      ylabel = 'Mbps'
      factor = 1e6
    else:
      ylabel = "Gbps"
      factor = 1e9

    download_bps /= factor
    upload_bps /= factor

    plt.figure(figsize=(10, 6))

    min_length = min(len(download_bps), len(upload_bps))
    x_values = np.arange(min_length) * resolution
    shade_alpha = 0.4

    plt.plot(
        x_values,
        download_bps[:min_length],
        color='dodgerblue',
        linestyle='-',
        linewidth=1.5,
        label='Download',
        alpha=0.8,
        marker='',  # Remove markers for cleaner look
    )
    plt.fill_between(
      x_values, 0, download_bps[:min_length], 
      color='dodgerblue', alpha=shade_alpha
    )
    
    plt.plot(
        x_values,
        upload_bps[:min_length],
        color='salmon',
        linestyle='-',
        linewidth=1.5,
        label='Upload',
        alpha=0.8,
        marker='',
    )
    plt.fill_between(
      x_values, 0, upload_bps[:min_length], 
      color='salmon', alpha=shade_alpha
    )

    plt.legend()
    plt.tight_layout()

    plt.title(f'Measured Data Transfer Rate')
    plt.xlabel('Time (seconds)')
    plt.ylabel(ylabel)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    if filename is not None:
      plt.savefig(filename)
    else:
      plt.show()

    plt.gca().clear()
    plt.close('all')

  def _init_sample_buffers(self):
    buffer_size = int(max(np.ceil(self._buffer_sec / self._interval) + 1, 1))
    self._samples_bytes_rx = np.full(buffer_size, -1, dtype=np.int64)
    self._samples_bytes_tx = np.full(buffer_size, -1, dtype=np.int64)
    self._samples_time = np.full(buffer_size, -1, dtype=np.float64)
    self._cursor = 0
    self._num_samples = 0

  def start_sampling(self, force=False):
    if force == False and self.is_sampling():
      return

    self._terminate.set()
    self._terminate = threading.Event()

    if self._thread is not None:
      self._thread.join()

    self._init_sample_buffers()

    self._thread = threading.Thread(
      target=self.sample_loop, 
      args=(self._terminate, self._interval)
    )
    self._thread.daemon = True
    self._thread.start()

  def num_samples(self) -> int:
    with self._sample_lock:
      return self._num_samples

  def start_time(self) -> float:
    with self._sample_lock:
      if self._num_samples == 0:
        return 0.0

      if self.num_samples < self._samples_time.size:
        return self._samples_time[0]
      else:
        pos = (self._cursor + 1) % self._samples_time.size
        return self._samples_time[pos]

  def end_time(self) -> float:
    with self._sample_lock:
      if self._num_samples == 0:
        return 0.0
      return self._samples_time[self._cursor]

  def samples(self) -> tuple[npt.NDArray[np.uint64], npt.NDArray[np.float64]]:
    with self._sample_lock:
      byte_samples_rx = np.copy(self._samples_bytes_rx)
      byte_samples_tx = np.copy(self._samples_bytes_tx)
      time_samples = np.copy(self._samples_time)
      cursor = self._cursor

      if self._num_samples < byte_samples_rx.size:
        byte_samples_rx = byte_samples_rx[:cursor]
        byte_samples_tx = byte_samples_tx[:cursor]
        time_samples = time_samples[:cursor]
      else:
        byte_samples_rx = np.concatenate([byte_samples_rx[cursor:], byte_samples_rx[:cursor]])
        byte_samples_tx = np.concatenate([byte_samples_tx[cursor:], byte_samples_tx[:cursor]])
        time_samples = np.concatenate([time_samples[cursor:], time_samples[:cursor]])

    return (byte_samples_rx, byte_samples_tx, time_samples)

  def _do_sample(self, time_correction:float) -> float:
    import psutil
    net = psutil.net_io_counters(nowrap=True)
    t = time.monotonic() - time_correction

    with self._sample_lock:
      self._samples_bytes_rx[self._cursor] = net.bytes_recv
      self._samples_bytes_tx[self._cursor] = net.bytes_sent
      self._samples_time[self._cursor] = t

      self._cursor += 1
      if self._cursor >= self._samples_time.size:
        self._cursor = 0
      self._num_samples += 1

  def sample_loop(self, terminate_evt:threading.Event, interval:float):
    import psutil

    # measure time to measure time
    def measure_correction():
      s = time.monotonic()
      time.monotonic()
      time.monotonic()
      time.monotonic()
      time.monotonic()
      time.monotonic()
      e = time.monotonic()
      return (e - s) / 5

    time_correction = measure_correction()
    psutil.net_io_counters.cache_clear()

    recorrection_start = time.monotonic()
    e = time.monotonic()

    while not terminate_evt.is_set():
      s = time.monotonic()
      self._do_sample(time_correction)

      if (recorrection_start-s) > 60:
        time_correction = measure_correction()
        recorrection_start = time.monotonic()

      e = time.monotonic()

      wait = interval - (e-s)
      if wait > 0:
        time.sleep(wait)

    if (interval * 0.5) < (time.monotonic() - e):
      self._do_sample(time_correction)

  def is_sampling(self):
    return self._thread is not None

  def stop_sampling(self):
    self._terminate.set()
    if self._thread is not None:
      self._thread.join()
    self._thread = None

  def __del__(self):
    self.stop_sampling()

  def __getstate__(self):
    # Copy the object's state from self.__dict__ which contains
    # all our instance attributes. Always use the dict.copy()
    # method to avoid modifying the original state.
    state = self.__dict__.copy()
    # Remove the unpicklable entries.
    del state['_sample_lock']
    del state['_terminate']
    del state['_thread']
    return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    self._sample_lock = threading.Lock()
    self._terminate_evt = threading.Event()
    self._thread = None

  def __enter__(self):
    self.start_sampling()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.stop_sampling()