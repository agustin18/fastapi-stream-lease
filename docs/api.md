# API reference

The public API is exported from the top-level `fastapi_stream_lease` package. Internal
modules, names prefixed with `_`, and Lua script layouts are implementation details.

## Manager

::: fastapi_stream_lease.StreamLeaseManager
    options:
      show_source: false

## Lease

::: fastapi_stream_lease.StreamLease
    options:
      show_source: false

## Configuration

::: fastapi_stream_lease.LeaseConfig
    options:
      show_source: false

## Circuit breaker

::: fastapi_stream_lease.CircuitBreakerConfig
    options:
      show_source: false

::: fastapi_stream_lease.BackendFailurePolicy
    options:
      show_source: false

::: fastapi_stream_lease.FallbackMode
    options:
      show_source: false

::: fastapi_stream_lease.CircuitState
    options:
      show_source: false

## Exceptions

::: fastapi_stream_lease.StreamLeaseError
    options:
      show_source: false

::: fastapi_stream_lease.StreamLeaseRejected
    options:
      show_source: false

::: fastapi_stream_lease.StreamLeaseUnavailable
    options:
      show_source: false

::: fastapi_stream_lease.StreamLeaseLost
    options:
      show_source: false

::: fastapi_stream_lease.ConfigurationMismatchError
    options:
      show_source: false

## Helpers

::: fastapi_stream_lease.register_exception_handlers
    options:
      show_source: false

::: fastapi_stream_lease.ProtectedStreamingResponse
    options:
      show_source: false
