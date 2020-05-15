# dynsampler-go changelog

## 0.2.1 2019-08-07

Fixes

- Corrects some sample rate calculations in the Exponential Moving Averge for very small counts.

## 0.2.0 2019-07-31

Features

- Adds Exponential Moving Average (`EMASampleRate`) implementation with Burst Detection, based on the `AvgSampleRate` implementation. See docs for description.
- Adds `SaveState` and `LoadState` to interface to enable serialization of internal state for persistence between process restarts.

## 0.1.0 2019-05-22

Versioning introduced.
