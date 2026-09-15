# Load Balancer

features:

- http server with hyper plus an http client with hyper also.
- lb has a list of backends, after getting a request, it forwards the request to one of the backends marked healthy and enabled, choosing a backend is done via round robin.
- on a separate tokio task, the lb has a health checker, which pokes the backends periodically to check if they are responding correctly.
- connection pooling, the lb stores connections to backends there, so that we dont keep making a new connection for each request.
- idempotent retries.
- failures to backends are recorded and act as an additional health checker.
- retry budget to protect already struggling backend servers from collapsing by limiting the retry-to-request ratio.
- manual backend adding, removing, enabling and disabling via a listener on separate tokio task, only listens to requests from lb server localhost.
