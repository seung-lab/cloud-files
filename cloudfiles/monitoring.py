import intervaltree
import threading
import time
import numpy as np

class TransmissionMonitor:
  """Monitors the current transmissing rate of a file set."""
  def __init__(self):
    self._intervaltree = intervaltree.IntervalTree()
    self._lock = threading.Lock()
    self._total_bytes_landed = 0
    self._in_flight_ct = 0
    self._in_flight_bytes = 0


  @classmethod
  def merge(klass, tms:list["TransmissionMonitor"]) -> "TransmissionMonitor":
    tm = TransmissionMonitor()

    with tm._lock:
      for other in tms:
        with other._lock:
          tm._intervaltree.union(other._intervaltree)

    return tm

  def start_io(self, num_bytes:int) -> None:
    with self._lock:
      self._in_flight_ct += 1
      self._in_flight_bytes += num_bytes

  def end_io(self, start_sec:float, end_sec:float, num_bytes:int) -> None:
    """Add a new value to the interval set."""
    start_us = int(start_sec * 1e6)
    end_us = int(end_sec * 1e6)

    with self._lock:
      self._in_flight_ct -= 1
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

  def begin(self):
    with self._lock:
      return self._intervaltree.begin()

  def end(self):
    with self._lock:
      return self._intervaltree.end()

  def histogram(self, resolution:float = 1.0) -> None:
    """
    Plot a bar chart showing the number of bytes transmitted
    per a unit time. Resolution is specified in seconds.
    """
    import matplotlib.pyplot as plt
    
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

    plt.title(f'Bytes Transmitted per {text}')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Bytes Transmitted')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
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










