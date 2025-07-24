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
    self._in_flight_bytes = 0
    self._direction = direction

    # self._network_sampler = NetworkSampler(direction)
    # self._network_sampler.start_sampling()

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
        start_time = time.time()
      self._in_flight[flight_id] = start_time
      self._in_flight_bytes += num_bytes
    return flight_id

  def end_io(self, flight_id:uuid.UUID, num_bytes:int) -> None:
    """Add a new value to the interval set."""
    end_us = int(time.time() * 1e6)
    
    with self._lock:
      start_us = int(self._in_flight.pop(flight_id) * 1e6)
      self._in_flight_bytes -= num_bytes    
      self._intervaltree.addi(start_us, end_us, num_bytes)
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
        num_bytes += interval.data
    return num_bytes

  def current_bps(self, look_back_sec:float = 2.0) -> float:
    """
    Compute the current bits per a second with a lookback
    value given in microseconds.
    """
    look_back_us = look_back_sec * 1e6

    with self._lock:
      now_us = int(time.time() * 1e6)
      query_us = now_us - look_back_us
      lookback_intervals = self._intervaltree[query_us:]
      begin_us = self._intervaltree.begin()

    num_bytes = 0
    for interval in lookback_intervals:
      if interval.begin > query_us:
        num_bytes += interval.data
      else:
        adjustment_factor = (interval.end - query_us) / (interval.end - interval.begin)
        num_bytes += int(round(interval.data * adjustment_factor))

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
    with self._lock:
      all_begin = int(np.floor(self._intervaltree.begin() / 1e6))
      all_end = int(np.ceil(self._intervaltree.end() / 1e6))

      num_bins = int(np.ceil((all_end - all_begin) / resolution))
      bins = np.zeros([ num_bins ], dtype=np.uint32)

      for interval in self._intervaltree:
        begin = interval.begin / 1e6
        end = interval.end / 1e6

        elapsed = (interval.end - interval.begin) / 1e6

        if elapsed < resolution:
          num_bytes_per_bin = interval.data
        else:
          num_bytes_per_bin = round(interval.data / np.ceil(elapsed / resolution))

        bin_start = int((begin - all_begin) / resolution)
        bin_end = int((end - all_begin) / resolution)
        bins[bin_start:bin_end+1] += num_bytes_per_bin

    return bins

  def plot_histogram(self, resolution:float = 1.0, filename:Optional[str] = None) -> None:
    """
    Plot a bar chart showing the number of bytes transmitted
    per a unit time. Resolution is specified in seconds.
    """
    bins = self.histogram(resolution)
    plot_histogram(
      bins, 
      direction=self._direction, 
      resolution=resolution, 
      filename=filename
    )

  def plot_gantt(
    self, 
    filename:Optional[str] = None,
    title:Optional[str] = None,
    show_size_labels:bool = True,
  ):
    import matplotlib.pyplot as plt
    import matplotlib.colors as colors
    from matplotlib.cm import ScalarMappable

    start_time = self.start_unix_time()

    file_sizes = []
    with self._lock:
      for interval in self._intervaltree:
        file_sizes.append(interval.data)

    min_file_size = min(file_sizes)
    max_file_size = max(file_sizes)
    del file_sizes

    fig, ax = plt.subplots(figsize=(10, 5))

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
      else:
        factor = (1e12, 'TB')

      return f"{x/factor[0]:.2f} {factor[1]}"

    with self._lock:
      for i, interval in enumerate(self._intervaltree):
          duration = (interval.end - interval.begin) / 1e6
          left = (interval.begin / 1e6) - start_time
          cval = norm(interval.data)
          ax.barh(
            str(i), 
            width=duration,
            left=left,
            height=1,
            color=cmap(cval)
          )
          if show_size_labels:
            ax.text(
                x=left + (duration/2),
                y=i,            
                s=human_readable_bytes(int(interval.data)),
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

class NetworkSampler:
  def __init__(
    self, 
    direction:IOEnum, 
    buffer_sec:float = 10.0, 
    interval:float = 0.1
  ):
    self._terminate = threading.Event()
    self._thread = None
    self._direction = direction
    self._interval = interval
    self._buffer_sec = buffer_sec

    self._sample_lock = threading.Lock()
    self._init_sample_buffers()

  def peak_bps(self, window:float = 1.0) -> float:
    N = self.num_samples()
    if N <= 1:
      return 0

    bs, ts = self.samples()
    
    peak_rate = 0.0

    for i in range(len(ts) - 1):
      for j in range(i + 1, len(ts)):
        elapsed = (ts[j] - ts[i])
        if elapsed >= window:
          rate = (bs[j] - bs[i]) / elapsed * 8
          peak_rate = max(peak_rate, rate)
          break

      if (ts[-1] - ts[i]) > 0:
        rate = (bs[-1] - bs[i]) / (ts[-1] - ts[i]) * 8
        peak_rate = max(peak_rate, rate)

    return peak_rate

  def current_bps(self, look_back_sec:float = 2.0) -> float:
    N = self.num_samples()
    if N <= 1:
      return 0

    bs, ts = self.samples()

    i = ts.size - 2
    elapsed = 0
    t = ts[-1]
    while (i >= 0) and not (elapsed >= look_back_sec):
      elapsed = t - ts[i]
      i -= 1
    
    i += 1

    if elapsed < 1e-4:
      return 0

    return (bs[-1] - bs[i]) / elapsed * 8

  def histogram(self, resolution:float = 1.0) -> npt.NDArray[np.uint32]:
    N = self.num_samples()
    if N <= 1:
      return 0

    bs, ts = self.samples()
    bs = bs[1:] - bs[:-1]

    num_bins = int(np.ceil((ts[-1] - ts[0]) / resolution))
    bins = np.zeros([ num_bins ], dtype=np.uint32)

    for i in range(bs.size):
      j = int((ts[i] - ts[0]) / resolution)
      bins[j] += bs[i]

    return bins

  def plot_histogram(self, resolution:float = None, filename:Optional[str] = None) -> None:
    """
    Plot a bar chart showing the number of bytes transmitted
    per a unit time. Resolution is specified in seconds.
    """
    if resolution is None:
      resolution = self._interval
    elif resolution < self._interval:
      raise ValueError(
        f"Can't create histogram bins at a higher resolution "
        f"than the sample rate. Got: {resolution} Sample Rate: {self._interval}"
      )

    bins = self.histogram(resolution)
    plot_histogram(
      bins, 
      direction=self._direction, 
      resolution=resolution, 
      filename=filename
    )

  def _init_sample_buffers(self):
    buffer_size = int(max(np.ceil(self._buffer_sec / self._interval) + 1, 1))
    self._samples_bytes = np.full(buffer_size, -1, dtype=np.int64)
    self._samples_time = np.full(buffer_size, -1, dtype=np.float64)
    self._cursor = 0
    self._num_samples = 0

  def start_sampling(self):
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

  def samples(self) -> tuple[npt.NDArray[np.uint64], npt.NDArray[np.float64]]:
    with self._sample_lock:
      byte_samples = np.copy(self._samples_bytes)
      time_samples = np.copy(self._samples_time)
      cursor = self._cursor

      if self._num_samples < byte_samples.size:
        byte_samples = byte_samples[:cursor]
        time_samples = time_samples[:cursor]
      else:
        byte_samples = np.concatenate([byte_samples[cursor:], byte_samples[:cursor]])
        time_samples = np.concatenate([time_samples[cursor:], time_samples[:cursor]])

    return (byte_samples, time_samples)

  def sample_loop(self, terminate_evt:threading.Event, interval:float):
    import psutil

    # measure time to measure time
    def measure_correction():
      s = time.time()
      time.time()
      time.time()
      time.time()
      time.time()
      time.time()
      e = time.time()
      return (e - s) / 5

    time_correction = measure_correction()
    psutil.net_io_counters.cache_clear()

    def measure() -> tuple[int,float]:
      net = psutil.net_io_counters(nowrap=True)
      t = time.time() - time_correction

      if self._direction == IOEnum.RX:
        return (net.bytes_recv, t)
      else:
        return (net.bytes_sent, t)

    recorrection_start = time.time()
    while not terminate_evt.is_set():
      s = time.time()
      
      size, t = measure()
      with self._sample_lock:
        self._samples_bytes[self._cursor] = size
        self._samples_time[self._cursor] = t

        if (recorrection_start-s) > 60:
          time_correction = measure_correction()
          recorrection_start = time.time()

        self._cursor += 1
        if self._cursor >= self._samples_time.size:
          self._cursor = 0
        self._num_samples += 1

      e = time.time()

      wait = interval - (e-s)
      if wait > 0:
        time.sleep(wait)

  def stop_sampling(self):
    self._terminate.set()
    if self._thread is not None:
      self._thread.join()

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
    # Restore instance attributes (i.e., filename and lineno).
    self.__dict__.update(state)
    self._sample_lock = threading.Lock()
    self._terminate_evt = threading.Event()
    self._thread = None


def plot_histogram(bins:npt.NDArray[np.integer], direction:IOEnum, resolution:float, filename:Optional[str] = None) -> None:
  """
  Plot a bar chart showing the number of bytes transmitted
  per a unit time. Resolution is specified in seconds.
  """
  import matplotlib.pyplot as plt
  
  plt.figure(figsize=(10, 6))
  plt.bar(range(len(bins)), bins, color='dodgerblue')

  tick_step = 1
  if len(bins) > 20:
    tick_step = len(bins) // 20

  timestamps = [ 
    f"{i*resolution:.2f}" for i in range(0, len(bins), tick_step)
  ]
  plt.xticks(
    range(0, len(bins), tick_step), 
    timestamps, 
    rotation=45, 
    ha='right'
  )

  if resolution == 1.0:
    text = "Second"
  else:
    text = f"{resolution:.2f} Seconds"

  direction_text = "Downloaded"
  if direction == IOEnum.TX:
    direction_text = "Uploaded"

  plt.title(f'Bytes {direction_text} per {text}')
  plt.xlabel('Time (seconds)')
  plt.ylabel(f'Bytes {direction_text}')
  plt.grid(axis='y', linestyle='--', alpha=0.7)
  plt.tight_layout()

  if filename is not None:
    plt.savefig(filename)
  else:
    plt.show()
