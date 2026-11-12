"""
Detector settings in one place (B12 and later detectors).

Change a number here, not inside the detector code, so experiments (B25) can
compare settings without editing logic. Every value can also be overridden
on the command line, e.g. --max-distance 150.
"""

BUNCHING = {
    # Two buses on the same route and direction closer than this are "bunched".
    "max_distance_m": 200,
    # Only compare two position reports taken within this many seconds of each
    # other, so a bus is not compared with where another bus was a minute ago.
    "max_time_gap_s": 60,
    # Length of each event-time window (tumbling). Bigger = fewer, later alerts.
    "window_s": 60,
    # How long to wait for late reports before closing a window (watermark).
    # Bigger = fewer late reports missed, but alerts arrive later.
    "watermark_s": 30,
}
